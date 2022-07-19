/**
 *描述：缓存管理
 *      连接redis集群，为其他文件提供redis数据读写方法
 *作者：江洪
 *时间：2019-5-20
 */

package cache

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v7"
	uuid "github.com/satori/go.uuid"
	"github.com/valyala/fastrand"
	"runtime"
	"strings"
	"sync"
	"library_Test/common"
	"library_Test/library/log"
	"library_Test/library/utils"
	"time"
)

//封装redis结构体，建议直接使用Client操作redis
type Redis struct {
	//lock        *sync.Mutex
	Client            redis.UniversalClient
	Config            *RedisConfig
	Slaves            *sync.Map       //所有slave节点客户端
	slaveErrors       chan SlaveError //slave节点异常信息
	stop              chan bool       //停止所有协程
	once              *sync.Once      //哨兵模式下，当没有任何从节点时，触发slave节点健康检查以发现正常的slave节点
	slaveCheckTrigger chan bool       //手动触发slave节点健康检查，同一时刻最多触发一次
	closeFunc         func() error    //关闭
}

type SlaveError struct {
	SlaveAddr string
	Err       error
}

func (s *SlaveError) Error() string {
	return s.Err.Error()
}

//redis配置
type RedisConfig struct {
	Points   []string
	UserName string
	//新增加用户名选项
	User     string
	Password string
	Type     RedisType `check:"required"`
	DB       int
	// Maximum number of socket connections.
	// Default is 10 connections per every CPU as reported by runtime.NumCPU.
	PoolSize int
	// Minimum number of idle connections which is useful when establishing
	// new connection is slow.
	//默认是0
	MinIdleConns int
	// Amount of time after which client closes idle connections.
	// Should be less than server's timeout.
	// Default is 5 minutes. -1 disables idle timeout check.
	IdleTimeout           time.Duration
	SentinelClientOptions *redis.FailoverOptions
	ClusterClientOptions  *redis.ClusterOptions
}

type RedisLock struct {
	Id            string        //锁id,锁唯一标识，将作为Name存储的value
	RedisClient   *Redis        //redis客户端
	Name          string        //锁名称
	Expire        time.Duration //锁过期时间
	isLockTimeout bool          //是否加锁超时
}

type RedisType string

const RedisTypeSentinel = "Sentinel"
const RedisTypeCluster = "Cluster"
const RedisTypeSingleton = "Singleton"

//连接redis
func Connect(c *RedisConfig) (*Redis, error) {
	log.Logger.Infof("%+v", *c)
	err := check(c)
	if err != nil {
		return nil, err
	}
	if c.Type == "" {
		c.Type = RedisTypeSentinel
	}
	if c.PoolSize == 0 {
		c.PoolSize = runtime.NumCPU() * 10
	}
	var client redis.UniversalClient
	var closeFunc func() error
	//需要启动的协程
	var goFunc func(rc *Redis) error
	/*switch c.Type {
	case RedisTypeCluster:
		if c.ClusterClientOptions == nil {
			c.ClusterClientOptions = &redis.ClusterOptions{
				Addrs:        c.Points,
				Password:     c.Password,
				MaxRetries:   common.RedisMaxRetries,
				DialTimeout:  common.RedisDialTimeout,
				PoolSize:     c.PoolSize,
				ReadTimeout:  common.RedisReadTimeout,
				MinIdleConns: c.MinIdleConns,
				IdleTimeout:  c.IdleTimeout,
			}
		}
		client = redis.NewClusterClient(c.ClusterClientOptions)
	case RedisTypeSentinel:
		if c.SentinelClientOptions == nil {
			c.SentinelClientOptions = &redis.FailoverOptions{
				MasterName:    c.UserName,
				Password:      c.Password,
				SentinelAddrs: c.Points,
				DB:            c.DB,
				MaxRetries:    common.RedisMaxRetries,
				DialTimeout:   common.RedisDialTimeout,
				PoolSize:      c.PoolSize,
				ReadTimeout:   common.RedisReadTimeout,
				MinIdleConns:  c.MinIdleConns,
				IdleTimeout:   c.IdleTimeout,
			}
		}
		client = redis.NewFailoverClient(c.SentinelClientOptions) */
	//采用redis/v7版本内容
	switch c.Type {
	case RedisTypeCluster:
		if c.ClusterClientOptions == nil {
			c.ClusterClientOptions = &redis.ClusterOptions{
				Addrs: c.Points,
				// +++ start +++
				Username: c.User,
				// +++ end +++
				Password:     c.Password,
				MaxRetries:   common.RedisMaxRetries,
				DialTimeout:  common.RedisDialTimeout,
				PoolSize:     c.PoolSize,
				ReadTimeout:  common.RedisReadTimeout,
				MinIdleConns: c.MinIdleConns,
				IdleTimeout:  c.IdleTimeout,
			}
		}
		client = redis.NewClusterClient(c.ClusterClientOptions)
	case RedisTypeSentinel:
		if c.SentinelClientOptions == nil {
			c.SentinelClientOptions = &redis.FailoverOptions{
				MasterName: c.UserName,
				// +++ start +++
				Username: c.User,
				// +++ end +++
				Password:      c.Password,
				SentinelAddrs: c.Points,
				DB:            c.DB,
				MaxRetries:    common.RedisMaxRetries,
				DialTimeout:   common.RedisDialTimeout,
				PoolSize:      c.PoolSize,
				ReadTimeout:   common.RedisReadTimeout,
				MinIdleConns:  c.MinIdleConns,
				IdleTimeout:   c.IdleTimeout,
			}
		}
		client = redis.NewFailoverClient(c.SentinelClientOptions)
		goFunc = func(rc *Redis) error {
			//初始化slave客户端
			err = rc.initSlaves()
			if err != nil {
				log.Logger.Error("failed to init slaves, err: ", err)
				return err
			}
			//监听主从切换
			go rc.watchSentinel()
			//处理slave节点访问异常,默认尝试ping3次异常的节点
			go rc.handleSlaveError(3)
			//从节点健康检查,默认1分钟检查一次,可通过channel slaveCheckTrigger立即触发健康检查
			go rc.slaveHealthCheck(time.Minute)
			return nil
		}
	default:
		client = redis.NewClient(commonOptions(c))
	}
	cmd := client.Ping()
	if cmd.Err() != nil {
		return nil, cmd.Err()
	}
	rc := &Redis{Client: client, Config: c, stop: make(chan bool), Slaves: &sync.Map{},
		slaveErrors: make(chan SlaveError, 1000), once: &sync.Once{},
		slaveCheckTrigger: make(chan bool, 1)}
	if c.Type == RedisTypeSentinel {
		closeFunc = func() error {
			_ = client.Close()
			rc.Slaves.Range(func(key, value interface{}) bool {
				sc := value.(redis.UniversalClient)
				_ = sc.Close()
				return true
			})
			return nil
		}
	} else {
		closeFunc = func() error {
			return client.Close()
		}
	}
	rc.closeFunc = closeFunc
	if goFunc == nil {
		return rc, nil
	}
	err = goFunc(rc)
	if err != nil {
		return nil, err
	}
	return rc, nil
}

//公共的客户端配置
func commonOptions(c *RedisConfig) *redis.Options {
	return &redis.Options{
		// +++ start +++
		Username: c.User,
		// +++ end +++
		Password:     c.Password,
		Addr:         c.Points[0],
		DB:           c.DB,
		MaxRetries:   common.RedisMaxRetries,
		DialTimeout:  common.RedisDialTimeout,
		PoolSize:     c.PoolSize,
		ReadTimeout:  common.RedisReadTimeout,
		MinIdleConns: c.MinIdleConns,
		IdleTimeout:  c.IdleTimeout,
	}
}

//监控master切换与从节点添加
func (c *Redis) watchSentinel() {
	if c.Config.Type != RedisTypeSentinel {
		return
	}
	var sc *redis.Client
	for _, addr := range c.Config.Points {
		rc := redis.NewClient(&redis.Options{Addr: addr})
		cmd := rc.Ping()
		if cmd.Err() != nil {
			log.Logger.Error(cmd.Err())
			continue
		}
		sc = rc
		break
	}
	if sc == nil {
		log.Logger.Error("all sentinel are unreachable")
		return
	}
	c.subscribeOnSentinel(sc.Subscribe("+slave", "+switch-master", "+sdown", "-sdown", "+odown", "-odown"))
	sc.Close()
}

//哨兵模式下订阅相关Channel
func (c *Redis) subscribeOnSentinel(mc *redis.PubSub) {
	for {
		select {
		case <-c.stop:
			log.Logger.Info("unsubscribe")
			return
		default:
			m, err := mc.ReceiveMessage()
			if err != nil && err.Error() == "EOF" {
				log.Logger.Error(err)
				//连接中断，重新监听其他sentinel
				go c.watchSentinel()
				return
			}
			l := log.Logger.WithField("Channel", m.Channel)
			l.Info(m)
			var addr string
			infos := strings.Split(m.Payload, " ")
			switch m.Channel {
			case "+slave", "-sdown":
				t := infos[0]
				if t != "slave" {
					continue
				}
				addr = infos[1]
				l = l.WithField("NewSlave", addr)
				addr = maybeIpv6(addr)
				rcnf := *c.Config
				rcnf.Points = []string{addr}
				//新增从节点客户端

				err := c.AddSlave(false, commonOptions(&rcnf))
				if err != nil {
					l.Error("failed to add slave, err: ", err)
					continue
				}
				l.Info("add slave successfully")
			case "+switch-master":
				if infos[0] != c.Config.UserName {
					l.Infof("ignore addr for master=%q", infos[0])
					continue
				}
				addr = fmt.Sprintf("%s:%s", infos[3], infos[4])
				l = l.WithField("NewMaster", addr)
				if v, ok := c.Slaves.Load(addr); ok {
					_ = v.(redis.UniversalClient).Close()
					c.Slaves.Delete(addr)
				}
				l.Info("remove it from slaves")
			case "+sdown":
				t := infos[0]
				if t != "slave" {
					continue
				}
				addr = infos[1]
				l = l.WithField("SlaveDown", addr)
				c.RemoveSlave(addr)
			default:
				fmt.Println(m.String())
			}
		}
	}
}

//新增一个从节点,如果不存在则创建客户端，已存在，则忽略创建，最后ping一下确定是否正常
func (c *Redis) AddSlave(closeOld bool, opts *redis.Options) error {
	var client redis.UniversalClient
	if v, ok := c.Slaves.Load(opts.Addr); ok {
		fmt.Println("opts.Addr:", opts.Addr)

		client = v.(redis.UniversalClient)
		if closeOld {
			log.Logger.Info("close old redis client")
			newClient := redis.NewClient(opts)
			c.Slaves.Store(opts.Addr, newClient)
			_ = client.Close()
			client = newClient
		}
	} else {
		/*fmt.Println("new client opts:",opts)
		fmt.Println("opts.U:",opts.Username)
		fmt.Println("opts.P:",opts.Password)
		fmt.Println("opts.Addr:",opts.Addr) */
		client = redis.NewClient(opts)
		//fmt.Println("new client:",client)

	}
	//redis6 目前ping操作有问题
	/*
		cmd := client.Ping()
		if cmd.Err() != nil {
			log.Logger.Info("Ping Error")
			return cmd.Err()
		}

	*/
	fmt.Println("after ping client:", client)

	c.Slaves.LoadOrStore(opts.Addr, client)
	//if v, ok := c.Slaves.LoadOrStore(opts.Addr, client); ok {
	//	//已有的客户端，关闭
	//	_ = v.(redis.UniversalClient).Close()
	//	//再保存
	//	c.Slaves.Store(opts.Addr, client)
	//}
	return nil
}

//获取一个从客户端
//输出：
//	从节点地址
//	从节点客户端
func (c *Redis) GetSlave() (string, redis.UniversalClient) {
	fmt.Println("try to slaves now")
	addrs, clients := c.slaveClients()
	fmt.Println("has clients:", clients)

	if len(clients) == 0 {
		log.Logger.Info("no slaves, use master")
		//立即触发slave健康检查, 间隔5秒后才能再次触发
		if c.Config.Type == RedisTypeSentinel {
			c.once.Do(func() {
				go func() {
					c.slaveCheckTrigger <- true
					time.Sleep(time.Second * 5)
					c.resetOnce()
				}()
			})
		}
		return "", c.Client
	} else if len(clients) == 1 {
		log.Logger.Debug("use slave: ", addrs[0])
		return addrs[0], clients[0]
	}
	i := fastrand.Uint32n(uint32(len(clients)))
	log.Logger.Debug("use slave: ", addrs[i])
	return addrs[i], clients[i]
}

//从节点健康检查
//	哨兵模式下订阅+slave +sdown -sdown等通道，可以知道节点挂掉或恢复，
//	但节点有可能处于初始化状态，无法提供服务，需要此健康检查发现正常的节点
func (c *Redis) slaveHealthCheck(interval time.Duration) {
	if c.Config.Type != RedisTypeSentinel {
		return
	}
	if interval == 0 {
		interval = time.Minute
	}
	log.Logger.Info("start slave health check")
	c.tryDiscoverySlaves(interval)
}

//尝试发现正常的从节点
func (c *Redis) tryDiscoverySlaves(interval time.Duration) {
	if interval == 0 {
		interval = time.Second * 5
	}
	log.Logger.Info("try to discovery slaves")
	timer := time.NewTimer(interval).C
	for {
		select {
		case <-c.stop:
			return
		case <-timer:
			c.discoverySlaves()
			timer = time.NewTimer(interval).C
		case <-c.slaveCheckTrigger:
			c.discoverySlaves()
		}
	}
}

//发现slave节点,发现存在正常的slave节点，返回true，其他返回false
// 如果发现异常的slave节点，则删除该节点的客户端
func (c *Redis) discoverySlaves() bool {

	addresses, err := c.discoverySlaveAddresses()
	if err != nil {
		log.Logger.Info("failed to discovery slaves, err: ", err)
		return false
	}
	if len(addresses) == 0 {
		log.Logger.Info("no slave was found")
		return false
	}
	var ok bool
	for _, addr := range addresses {
		log.Logger.Debug("slave addr: ", addr)
		//addr = maybeIpv6(addr)
		rcnf := *c.Config
		rcnf.Points = []string{addr}
		log.Logger.Debug("request slave config: ", rcnf)
		err := c.AddSlave(false, commonOptions(&rcnf))
		if err != nil {
			log.Logger.WithField("Slave", addr).Error("failed to ping slave, err: ", err)
			c.Slaves.Delete(addr)
			continue
		}
		ok = true
	}
	return ok
}

func (c *Redis) resetOnce() {
	c.once = &sync.Once{}
}

//从客户端
func (c *Redis) slaveClients() ([]string, []redis.UniversalClient) {
	var addrs []string
	var clients []redis.UniversalClient
	c.Slaves.Range(func(key, value interface{}) bool {
		addrs = append(addrs, key.(string))
		clients = append(clients, value.(redis.UniversalClient))
		return true
	})
	return addrs, clients
}

//初始化从节点客户端
//	暂时只支持sentinel模式的slave节点
func (c *Redis) initSlaves() error {
	//获取所有从节点地址
	var addrs []string
	switch c.Config.Type {
	case RedisTypeSentinel:
		addresses, err := c.discoverySlaveAddresses()
		if err != nil {
			return err
		}
		addrs = addresses
	case RedisTypeCluster:
		//pass
	default:
		//pass
	}
	if len(addrs) == 0 {
		log.Logger.Info("no slaves")
		return nil
	}
	//初始化从节点客户端
	for _, addr := range addrs {
		log.Logger.Info("slave addr: ", addr)
		addr = maybeIpv6(addr)
		rcnf := *c.Config
		rcnf.Points = []string{addr}
		//log.Logger.Info("init slave config: ", rcnf)

		err := c.AddSlave(true, commonOptions(&rcnf))
		if err != nil {
			log.Logger.WithField("Slave", addr).Error("failed to add slave, err: ", err)
		}
	}
	return nil
}

//向哨兵服务发现所有slave的地址
func (c *Redis) discoverySlaveAddresses() ([]string, error) {
	var addrs []string
	var discoveryError error
	//defPoint := c.Config.Points[0]
	for _, sentinel := range c.Config.Points {
		pc := redis.NewClient(&redis.Options{Addr: sentinel, PoolSize: 1, IdleTimeout: -1})
		cmd := redis.NewSliceCmd("SENTINEL", "slaves", c.Config.UserName)
		err := pc.Process(cmd)
		pc.Close()
		if err != nil {
			discoveryError = err
			continue
		}
		slaveInfoBlobs, err := cmd.Result()
		if err != nil {
			discoveryError = err
			continue
		}
		for _, slaveInfoBlob := range slaveInfoBlobs {
			infos := slaveInfoBlob.([]interface{})
			var ip, port string
			for i, info := range infos {
				s := info.(string)
				if s == "ip" {
					ip = infos[i+1].(string)
				} else if s == "port" {
					port = infos[i+1].(string)
				}
			}
			addrs = append(addrs, ip+":"+port)
		}
		if len(addrs) == 0 {
			continue
		} else {
			//发现一个或多个slave地址
			discoveryError = nil
			break
		}
	}
	//fmt.Println("addrs:",addrs)
	return addrs, discoveryError
}

//处理从节点异常
func (c *Redis) handleSlaveError(tryTimes int) {
	if c.Config.Type != RedisTypeSentinel {
		return
	}
	if tryTimes == 0 {
		tryTimes = 1
	}
	slaveChecking := sync.Map{}
	for err := range c.slaveErrors {
		if err.SlaveAddr == "" {
			continue
		}
		if _, ok := slaveChecking.Load(err.SlaveAddr); ok {
			continue
		}
		slaveChecking.Store(err.SlaveAddr, true)
		f := func() {
			defer slaveChecking.Delete(err.SlaveAddr)
			l := log.Logger.WithField("Slave", err.SlaveAddr)
			l.Info("slave err: ", err)
			//ping一下从节点
			statusOk, e := c.Ping(err.SlaveAddr, tryTimes)
			if !statusOk {
				l.Error(e)
				l.Info("remove slave")
				c.RemoveSlave(err.SlaveAddr)
				return
			}
			l.Info("ignore error")
		}
		f()
	}
}

//ping一下指定redis地址，只要成功一次，返回true,nil
func (c *Redis) Ping(addr string, times int) (bool, error) {
	if times == 0 {
		times = 1
	}
	pc := redis.NewClient(&redis.Options{Addr: addr, PoolSize: 1, IdleTimeout: -1})
	defer pc.Close()
	var err error
	var statusOk bool
	for i := 0; i < times; i++ {
		cmd := pc.Ping()
		if cmd.Err() != nil {
			log.Logger.WithField("Addr", addr).WithField("times", i+1).Info("ping failed, err: ", cmd.Err())
			err = cmd.Err()
			continue
		}
		statusOk = true
		err = nil
		break
	}
	return statusOk, err
}

//删除从节点
func (c *Redis) RemoveSlave(addr string) {
	if v, ok := c.Slaves.Load(addr); ok {
		c.Slaves.Delete(addr)
		_ = v.(redis.UniversalClient).Close()
	}
}

//报告从节点请求异常
func (c *Redis) reportSlaveError(slaveErr string, err error) {
	c.slaveErrors <- SlaveError{SlaveAddr: slaveErr, Err: err}
}

func check(conf *RedisConfig) error {
	err := utils.CheckRequiredStringField("RedisConfig", conf)
	if err != nil {
		return err
	}
	if len(conf.Points) == 0 {
		return errors.New("RedisConfig.Points must be not empty")
	}
	return nil
}

func (c *Redis) Close() error {
	close(c.stop)
	if c.closeFunc != nil {
		return c.closeFunc()
	}
	return nil
}

//判断key是否存在
func (c *Redis) IsExist(key string) (bool, error) {
	addr, client := c.GetSlave()
	cmd := client.Exists(key)
	val, err := cmd.Result()
	if err != nil {
		c.reportSlaveError(addr, err)
		return false, err
	}
	if val != 1 {
		return false, err
	}
	return true, err
}

//redis set, 永不过期
func (c *Redis) Set(key string, value interface{}) error {
	return c.SetWithExpireTime(key, value, 0)
}

//redis set, 指定过期时间
func (c *Redis) SetWithExpireTime(key string, value interface{}, ex time.Duration) error {
	_, err := c.Client.Set(key, value, ex).Result()
	return err
}

//redis get
//找不到，返回""字符串
func (c *Redis) Get(key string) (string, error) {
	addr, client := c.GetSlave()
	s, err := client.Get(key).Result()
	if err != nil {
		if err == redis.Nil {
			return "", nil
		}
		c.reportSlaveError(addr, err)
		return "", err
	}
	return s, nil
}

func (c *Redis) GetBytes(key string) ([]byte, error) {
	addr, client := c.GetSlave()
	bs, err := client.Get(key).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		c.reportSlaveError(addr, err)
		return nil, err
	}
	return bs, err
}

//会有redis.Nil错误
func (c *Redis) TryGet(key string) (string, error) {
	addr, client := c.GetSlave()
	s, err := client.Get(key).Result()
	if err != nil {
		if err == redis.Nil {
			return "", err
		}
		c.reportSlaveError(addr, err)
		return "", err
	}
	return s, nil
}

//redis del
//输入：
// key：要删除的key
//输出：
// 删除结果，1为删除成功,为0时若无错误则表示key不存在
// 错误
func (c *Redis) Del(key string) (int64, error) {
	return c.Client.Del(key).Result()
}

//将给定 key 的值设为 value ，并返回 key 的旧值(old value)。
func (c *Redis) GetSet(key string, val interface{}) (string, error) {
	return c.Client.GetSet(key, val).Result()
}

//redis HSET
func (c *Redis) HSet(key, field string, val interface{}) error {
	_, err := c.Client.HSet(key, field, val).Result()
	return err
}

//redis HMSET
func (c *Redis) HMSet(key string, val map[string]interface{}) error {
	_, err := c.Client.HMSet(key, val).Result()
	return err
}

//redis HMSET 并设置超时时间
func (c *Redis) HMSetEX(key string, val map[string]interface{}, ex time.Duration) error {
	err := c.HMSet(key, val)
	if err != nil {
		return err
	}
	_, err = c.Client.Expire(key, ex).Result()
	return err
}

//获取指定key所有指定field的值，
// 返回:
// 		如果key或所有field不存在，返回nil,nil
// 		如果存在field不存在，则不存在的field对应的值在[]interface{}中nil
func (c *Redis) HMGet(key string, fields ...string) ([]interface{}, error) {
	result, err := c.TryHMGet(key, fields...)
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}
	allFieldNotExist := true
	for i := range result {
		if result[i] != nil {
			allFieldNotExist = false
		}
	}
	if allFieldNotExist {
		return nil, nil
	}
	return result, err
}

//获取指定key所有指定field的值，
// 返回:
// 		如果key或所有field不存在，返回nil,nil
// 		如果key存在部分field不存在，不存在的field不会记录到map中，最终返回map,nil
func (c *Redis) HMGetForMap(key string, fields ...string) (map[string]interface{}, error) {
	result, err := c.TryHMGet(key, fields...)
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}
	resMap := make(map[string]interface{})
	for i := range fields {
		if result[i] == nil {
			continue
		}
		resMap[fields[i]] = result[i]
	}
	if len(resMap) == 0 {
		return nil, nil
	}
	return resMap, err
}

//获取指定key所有指定field的值，
// 返回:
// 		如果key不存在，error = redis.Nil
// 		如果存在field不存在，则不存在的field对应的值在[]interface{}中为nil
func (c *Redis) TryHMGet(key string, fields ...string) ([]interface{}, error) {
	addr, client := c.GetSlave()
	vs, err := client.HMGet(key, fields...).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, err
		}
		c.reportSlaveError(addr, err)
		return nil, err
	}
	return vs, nil
}

//获取指定key指定field的值，
// 返回:
// 		如果key或field不存在，返回nil,nil
func (c *Redis) HGet(key string, field string) (interface{}, error) {
	result, err := c.TryHGet(key, field)
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}
	return result, err
}

//获取指定key指定field的值，
// 返回:
// 		如果key或field不存在，返回nil,redis.Nil
func (c *Redis) TryHGet(key string, field string) (interface{}, error) {
	addr, client := c.GetSlave()
	v, err := client.HGet(key, field).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		c.reportSlaveError(addr, err)
		return nil, err
	}
	return v, nil
}

//redis HGETALL 获取所有字段
// 返回:
// 		如果key或field不存在，返回nil,nil
func (c *Redis) HGetAll(key string) (map[string]string, error) {
	addr, client := c.GetSlave()
	result, err := client.HGetAll(key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		c.reportSlaveError(addr, err)
		return nil, err
	}
	return result, err
}

//获取锁
//输入：
//	lockName: 锁名称
//	expire: 锁超时时间
//输出：
//	锁实例指针，如果lockName为空或expire为0，返回nil
func (c *Redis) GetLock(lockName string, expire time.Duration) *RedisLock {
	return NewRedisLock(c, lockName, expire)
}

//创建一个redis分布式锁
//输入：
//	client: redis客户端
//	lockName: 锁名称
//	expire: 锁超时时间，默认为3s
//输出：
//	锁实例指针，如果client为nil或lockName为空，返回nil
func NewRedisLock(client *Redis, lockName string, expire time.Duration) *RedisLock {
	if client == nil {
		log.Logger.Warn("client is nil")
		return nil
	}
	if lockName == "" {
		log.Logger.Warn("lockName must be not empty")
		return nil
	}
	if expire.Seconds() == 0 {
		expire = time.Second * 3
	}
	return &RedisLock{
		Id:          uuid.NewV4().String(),
		RedisClient: client,
		Name:        lockName,
		Expire:      expire,
	}
}

//加锁
//注意：
// 1、确保在锁有效期内完成业务流程
// 2、很有可能出现锁过期而业务未完成的情况，
// 		建议调用者在锁生效内业务流程的步骤尽量少，可以采用一个流程步骤一个锁的方式。
//TODO 解决redis主从不同步下主从切换导致锁失效的问题
func (l *RedisLock) Lock() {
	lg := log.Logger.WithField("LockKey", l.Name)
	lg.Info("Waiting for lock")
	t := time.NewTicker(l.Expire)
	for {
		select {
		case <-t.C:
			lg.Info("Lock timeout")
			l.isLockTimeout = true
			break
		default:
		}
		res := l.RedisClient.Client.SetNX(l.Name, l.Id, l.Expire)
		if res == nil {
			continue
		}
		if ok, err := res.Result(); !ok || err != nil {
			continue
		}
		lg.Info("Lock acquired successfully")
		break
	}
}

//释放锁
func (l *RedisLock) UnLock() {
	if l.isLockTimeout {
		return
	}
	script := "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end"
	l.RedisClient.Client.Eval(script, []string{l.Name}, l.Id)
	log.Logger.Info("Release lock")
}

//向集合添加一个或多个成员
//输入：
//	key: redis set的key
//	members：一个或多个成员，建议类型为string，如果为struct或map或slice，将通过json转为string，
//				如果成员为nil，则忽略，如果所有成员都为nil，则报错
//输出：
// 添加失败的错误
func (c *Redis) SAddMembers(key string, members ...interface{}) error {
	if valid, err := validateKey(key); !valid || err != nil {
		return err
	}
	members = transferInterface2str(members, false)
	if len(members) == 0 {
		return errors.New("all members could be nil")
	}
	_, err := c.Client.SAdd(key, members...).Result()
	if err != nil {
		return err
	}
	return nil
}

//获取集合的成员数
//输入：
//	key: redis set的key
//输出：
// 成员数，添加失败的错误
func (c *Redis) SGetCount(key string) (int64, error) {
	if valid, err := validateKey(key); !valid || err != nil {
		return 0, err
	}
	addr, client := c.GetSlave()
	count, err := client.SCard(key).Result()
	if err != nil {
		c.reportSlaveError(addr, err)
	}
	return count, err
}

//获取集合的所有成员
//输入：
// key: redis set的key
//输出：
// []string: 成员slice
// error：获取失败的错误
func (c *Redis) SGetMembers(key string) ([]string, error) {
	if valid, err := validateKey(key); !valid || err != nil {
		return nil, err
	}
	addr, client := c.GetSlave()
	ms, err := client.SMembers(key).Result()
	if err != nil {
		c.reportSlaveError(addr, err)
	}
	return ms, err
}

//判断指定value是否是集合 key 的成员
//输入：
// key: 集合的key
// value：判断该值是否存在于集合中
//输出：
// bool： true为存在于集合中
// error： 错误
func (c *Redis) SIsExist(key string, value string) (bool, error) {
	return c.SIsExistForInterface(key, value)
}

//判断指定value是否是集合 key 的成员
//输入：
// key: 集合的key
// value：判断该值是否存在于集合中
//输出：
// bool： true为存在于集合中
// error： 错误
func (c *Redis) SIsExistForInterface(key string, value interface{}) (bool, error) {
	if valid, err := validateKey(key); !valid || err != nil {
		return false, err
	}
	addr, client := c.GetSlave()
	exist, err := client.SIsMember(key, value).Result()
	if err != nil {
		c.reportSlaveError(addr, err)
	}
	return exist, err
}

//移除并返回集合中的一个随机成员
//输入：
// key: redis set的key
//输出：
// string: 随机成员
// error：错误，key不存在则返回redis.Nil错误
func (c *Redis) SPopOne(key string) (string, error) {
	if valid, err := validateKey(key); !valid || err != nil {
		return "", err
	}
	return c.Client.SPop(key).Result()
}

//返回集合中的一个或多个随机成员，不移除返回的成员
//输入：
// key: redis set的key，key不存在，返回 空[]string,nil
// count:返回随机成员的数量，如果成员数量小于count，则返回所有成员
//输出：
// []string: count个随机成员
// error：错误
func (c *Redis) SRandomGetNMembers(key string, count int64) ([]string, error) {
	if valid, err := validateKey(key); !valid || err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, errors.New("count has to be greater than 0")
	}
	return c.Client.SRandMemberN(key, count).Result()
}

//移除一个或多个成员，不返回被移除的集合成员
//输入：
//	key: redis set的key，key不存在时，不会返回错误
//	members：要删除的一个或多个成员
//输出：
// 错误
func (c *Redis) SRemove(key string, members ...interface{}) error {
	if valid, err := validateKey(key); !valid || err != nil {
		return err
	}
	if len(members) == 0 {
		return errors.New("members has at least one member")
	}
	var vs []interface{}
	for _, m := range members {
		vs = append(vs, m)
	}
	_, err := c.Client.SRem(key, vs...).Result()
	return err
}

//获取多个集合的交集
//输入：
// keys: 一个或多个集合的key，只有一个key时返回该集合的所有成员
//输出：
// []string: 多个集合的交集,没有交集则返回空[]string
// error：错误
func (c *Redis) SInter(keys ...string) ([]string, error) {
	if len(keys) == 0 {
		return nil, errors.New("keys has at least one key")
	}
	addr, client := c.GetSlave()
	inter, err := client.SInter(keys...).Result()
	if err != nil {
		c.reportSlaveError(addr, err)
	}
	return inter, err
}

//获取多个集合的差集
//输入：
// keys: 一个或多个集合的key，
// 		1、一个时返回该集合的所有成员,
// 		2、多个key按顺序比较，如：abc三个集合，a先与b比较得到的差集再与c比较
// 		3、只要有个key不存在，差集都是[]
//输出：
// []string: 多个集合的差集
// error：错误
func (c *Redis) SDiff(keys ...string) ([]string, error) {
	if len(keys) == 0 {
		return nil, errors.New("keys has at least one key")
	}
	addr, client := c.GetSlave()
	diff, err := client.SDiff(keys...).Result()
	if err != nil {
		c.reportSlaveError(addr, err)
	}
	return diff, err
}

//获取多个集合的并集
//输入：
// keys: 一个或多个集合的key，一个时返回该集合的所有成员
//输出：
// []string: 多个集合的并集
// error：错误
func (c *Redis) SUnion(keys ...string) ([]string, error) {
	if len(keys) == 0 {
		return nil, errors.New("keys has at least one key")
	}
	addr, client := c.GetSlave()
	union, err := client.SUnion(keys...).Result()
	if err != nil {
		c.reportSlaveError(addr, err)
	}
	return union, err
}

//验证key是否有效
func validateKey(key string) (bool, error) {
	if strings.TrimSpace(key) == "" {
		return false, errors.New("the key must be not empty")
	}
	return true, nil
}

//将[]interface转为[]string
func transferInterface2str(values []interface{}, allowNil bool) []interface{} {
	var strValues []interface{}
	for _, v := range values {
		if v == nil && !allowNil {
			continue
		}
		if _, ok := v.(string); ok {
			strValues = append(strValues, v)
			continue
		}
		var s string
		bytes, err := json.Marshal(v)
		if err != nil {
			s = fmt.Sprintf("%v", v)
		} else {
			s = string(bytes)
		}
		strValues = append(strValues, s)
	}
	return strValues
}

type ListPosition string

const ListLeft ListPosition = "Left"
const ListRight ListPosition = "Right"

//获取list长度
//输入：
// key: list的key,key不存在时返回0,nil
//输出：
// list的长度，错误
func (c *Redis) LLength(key string) (int64, error) {
	if valid, err := validateKey(key); !valid || err != nil {
		return 0, err
	}
	addr, client := c.GetSlave()
	length, err := client.LLen(key).Result()
	if err != nil {
		c.reportSlaveError(addr, err)
	}
	return length, err
}

func (c *Redis) LInsert(position ListPosition, key string, values ...interface{}) error {
	return c.lInsert(position, true, key, values...)
}

func (c *Redis) LInsertLeft(key string, values ...interface{}) error {
	return c.lInsert(ListLeft, true, key, values...)
}

func (c *Redis) LInsertRight(key string, values ...interface{}) error {
	return c.lInsert(ListRight, true, key, values...)
}

func (c *Redis) LInsertNotAllowNil(position ListPosition, key string, values ...interface{}) error {
	return c.lInsert(position, false, key, values...)
}

func (c *Redis) LInsertLeftNotAllowNil(key string, values ...interface{}) error {
	return c.lInsert(ListLeft, false, key, values...)
}

func (c *Redis) LInsertRightNotAllowNil(key string, values ...interface{}) error {
	return c.lInsert(ListRight, false, key, values...)
}

//在list添加一个或多个元素,如果list不存在，会自动创建list
//输入：
//	position: 插入list的位置
//	key: redis list的key
//	values：一个或多个元素，建议类型为string，如果为struct或map或slice，将通过json转为string
//输出：
// 添加失败的错误
func (c *Redis) lInsert(position ListPosition, allowNil bool, key string, values ...interface{}) error {
	if valid, err := validateKey(key); !valid || err != nil {
		return err
	}
	values = transferInterface2str(values, allowNil)
	if len(values) == 0 {
		return errors.New("all values could be nil")
	}
	var err error
	if position == ListRight {
		_, err = c.Client.RPush(key, values...).Result()
	} else {
		_, err = c.Client.LPush(key, values...).Result()
	}
	return err
}

//获取指定索引的list元素，不会移除该元素
//输入：
// key：list的key
// index：元素在list的索引，-1表示list最后一个元素，如果索引越界，返回redis.Nil错误
//输出：
// 字符串，错误
func (c *Redis) LGetByIndex(key string, index int64) (string, error) {
	if valid, err := validateKey(key); !valid || err != nil {
		return "", err
	}
	addr, client := c.GetSlave()
	s, err := client.LIndex(key, index).Result()
	if err != nil {
		if err == redis.Nil {
			return "", err
		}
		c.reportSlaveError(addr, err)
	}
	return s, err
}

//从list的左/右移除并返回一个元素
//输入：
// position：list的左或右
// key：list的key,如果key不存在则返回redis.Nil错误
//输出：
// 返回的元素，错误
func (c *Redis) LGetAndDel(position ListPosition, key string) (string, error) {
	if valid, err := validateKey(key); !valid || err != nil {
		return "", err
	}
	if position == ListRight {
		return c.Client.RPop(key).Result()
	}
	return c.Client.LPop(key).Result()
}

func (c *Redis) LGetAndDelFromLeft(key string) (string, error) {
	return c.LGetAndDel(ListLeft, key)
}

func (c *Redis) LGetAndDelFromRight(key string) (string, error) {
	return c.LGetAndDel(ListRight, key)
}

//移出并获取列表的一个元素,如果list没有元素会阻塞list直到timeout超时或发现有元素为止。
//输入：
// position：list的左或右
// timeout: 等待超时时间，当所有list的元素都被移除，等待超时时，返回redis.Nil错误
// keys：一个或多个list的key
//输出：
// 返回的元素，格式：[list的key,被移除的元素]
// 错误
func (c *Redis) LGetAndDelUntilTimeout(position ListPosition, timeout time.Duration, keys ...string) ([]string, error) {
	if timeout.Seconds() == 0 {
		return nil, errors.New("timeout must be not zero")
	}
	if len(keys) == 0 {
		return nil, errors.New("keys has at least one key")
	}
	if position == ListRight {
		return c.Client.BRPop(timeout, keys...).Result()
	}
	return c.Client.BLPop(timeout, keys...).Result()
}

func (c *Redis) LGetAndDelFromLeftUntilTimeout(timeout time.Duration, keys ...string) ([]string, error) {
	return c.LGetAndDelUntilTimeout(ListLeft, timeout, keys...)
}

func (c *Redis) LGetAndDelFromRightUntilTimeout(timeout time.Duration, keys ...string) ([]string, error) {
	return c.LGetAndDelUntilTimeout(ListRight, timeout, keys...)
}

//更新指定list索引对应的元素
//输入：
// key：list的key，key不存在，返回redis.Nil错误
// index：索引，索引越界返回"ERR index out of range"错误
// value：新的值
//输出：
// 错误
func (c *Redis) LUpdateByIndex(key string, index int64, value string) error {
	if valid, err := validateKey(key); !valid || err != nil {
		return err
	}
	_, err := c.Client.LSet(key, index, value).Result()
	return err
}

//从list中删除指定元素
//注意：
// count > 0 : 从表头开始向表尾搜索，移除与 VALUE 相等的元素，数量为 COUNT 。
// count < 0 : 从表尾开始向表头搜索，移除与 VALUE 相等的元素，数量为 COUNT 的绝对值。
// count = 0 : 移除表中所有与 VALUE 相等的值
//输入：
// key：list的key
// count：要删除的数量
// value：要删除的元素,该元素不存在时，无错误返回
//输出：
// 错误
func (c *Redis) LDel(key string, count int64, value string) error {
	if valid, err := validateKey(key); !valid || err != nil {
		return err
	}
	_, err := c.Client.LRem(key, count, value).Result()
	return err
}

//添加元素到集合中
func (c *Redis) SAdd(key string, members ...interface{}) error {
	_, err := c.Client.SAdd(key, members...).Result()

	return err
}

//移除集合中一个或多个元素
func (c *Redis) SRem(key string, members ...interface{}) error {
	_, err := c.Client.SRem(key, members...).Result()

	return err
}

//移除哈希表
func (c *Redis) HDel(key string, fields []string) error {
	_, err := c.Client.HDel(key, fields...).Result()

	return err
}

//查询集合中所有元素
func (c *Redis) Smembers(key string) ([]string, error) {
	members, err := c.SGetMembers(key)
	return members, err
}

//自增
func (c *Redis) Incr(key string) (int64, error) {
	return c.Client.Incr(key).Result()
}

//自增并设置过期时间
func (c *Redis) IncrWithEx(key string, ex time.Duration) (int64, error) {
	i, err := c.Client.Incr(key).Result()
	if err != nil {
		return i, err
	}
	ok, err := c.Client.Expire(key, ex).Result()
	if err != nil || !ok {
		return 0, err
	}
	return i, nil
}

//设置key的过期时间,如果key不存在，返回false,nil
func (c *Redis) KeyExpire(key string, ex time.Duration) (bool, error) {
	ok, err := c.Client.Expire(key, ex).Result()
	if err != nil {
		if err == redis.Nil {
			return false, nil
		}
		return ok, err
	}
	return ok, nil
}

//如果ip为ipv6,则加上"[]"
func maybeIpv6(ip string) string {
	i := strings.LastIndex(ip, ":")
	rIp := ip[:i]
	if strings.Contains(rIp, ":") {
		return "[" + rIp + "]" + ip[i:]
	}
	return ip
}
