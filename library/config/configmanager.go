/**
 *描述：配置管理
 *      在此文件声明整个平台所有需要用到的配置项
 *      从配置中心同步配置信息，提供给各服务的其他文件访问配置项
 *      没有配置中心的情况下，从本地配置文件中读取配置项
 *      ConfigGroup：配置项组，存放一个或多个配置项，Apollo配置中心的namespace、一个配置文件都是配置项组
 *作者：江洪
 *时间：2019-5-20
 */
package config

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/fsnotify/fsnotify"
	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/api/watch"
	"github.com/shima-park/agollo"
	"github.com/tidwall/gjson"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"
	"library_Test/common"
	"library_Test/library/log"
	"library_Test/library/servicediscovery/consul"
	"library_Test/library/utils"
)

var (
	manager       *ConfigManager           //配置管理器
	apolloConnect map[string]string        //Apollo连接配置
	consulConnect map[string]string        //consul连接配置
	otherAgollo   map[string]agollo.Agollo //其他Apollo客户端
)

type ConfigManager struct {
	ConfigMap           sync.Map
	ApolloOptions       *ApolloConfigOptions                        //Apollo客户端可选项
	FileConfigOptions   *FileConfigOptions                          //通过文件读取配置可选项
	ConsulConfigOptions *ConsulConfigOptions                        //通过文件读取配置可选项
	ApolloConnectConfig *connectConfig                              //apollo连接配置
	Change              chan Change                                 //配置变更通知channel
	GetConfig           func(configGroup string, key string) string //获取指定Apollo namespace或本地配置文件
	LocalConfig         string                                      //当前服务私有配置，Apollo namespace或文件
	ConsulClient        *consul.ConsulClient                        //consul客户端
}

type Change struct {
	ConfigGroup string //配置项组
	Items       map[ChangeType][]ChangeItem
}

type ChangeItem struct {
	Type     ChangeType
	Key      string //配置项
	OldValue interface{}
	NewValue interface{}
}

type ChangeType string

const (
	ChangeTypeAdd    ChangeType = "add"
	ChangeTypeUpdate ChangeType = "update"
	ChangeTypeDelete ChangeType = "delete"
)

type connectConfig struct {
	Apollo map[string]string `json:"apollo"`
	Consul map[string]string `json:"consul"`
}

//读取Apollo配置可选项
type ApolloConfigOptions struct {
	ConfigFile          string   //连接Apollo配置中心的配置文件路径
	PreloadNamespaces   []string //预加载的namespace
	HasDefaultNamespace bool     //为true，将预加载namespace为application的配置
	LocalNamespace      string   //本地配置项集合，可以理解为当前服务自己的私有配置
	OtherAppId          []string //Apollo配置中心项目AppId
}

//读取配置文件可选项
type FileConfigOptions struct {
	ConfigPath        string   //配置文件目录
	ConfigFiles       []string //指定有效的配置文件名
	IgnoreConfigFiles []string //忽略某些配置文件
	LocalFile         string   //私有配置
}

//配置文件信息
type configFileInfo struct {
	RealPath string //真实路径
	Content  []byte //文件内容
}

//consul配置中心可选项
type ConsulConfigOptions struct {
	ConfigFile string //连接consul配置中心的配置
	ConsulAddr string //consul地址
	KeyPrefix  string //key的前缀，监听所有以该前缀开头的key
	LocalKey   string //私有配置
}

type ConsulConfigChange struct {
	Key string
	Old []byte
	New []byte
}

/*
 *从配置文件中读取连接配置中心的参数
 *输入：
 *     name - 配置中心配置文件路径
 *返回： 加载配置结果
 */
func LoadCenterConfigFormJsonFile(name string) (*connectConfig, error) {
	f, err := os.Open(name)
	if err != nil {
		fmt.Println("err:", err)
		return nil, err
	}
	defer f.Close()

	var ret connectConfig
	if err := json.NewDecoder(f).Decode(&ret); err != nil {
		return nil, err
	}
	apolloConnect = ret.Apollo
	consulConnect = ret.Consul
	return &ret, nil
}

/**
 *加载配置
 *输入：
 *     configFile - apollo配置文件路径
 *返回： 加载配置结果
 */
func LoadConfigWithApollo(option *ApolloConfigOptions) (*ConfigManager, error) {
	if option == nil {
		option = &ApolloConfigOptions{}
	}
	manager = &ConfigManager{
		ConfigMap: sync.Map{}, ApolloOptions: option, LocalConfig: option.LocalNamespace,
		GetConfig: func(configGroup string, key string) string {
			return agollo.Get(key, agollo.WithDefault(""), agollo.WithNamespace(configGroup))
		},
	}
	configFile := option.ConfigFile
	if configFile == "" {
		configFile = common.DefaultCenterConfigFile
	}
	cConf, err := LoadCenterConfigFormJsonFile(configFile)
	if err != nil {
		return nil, err
	}
	manager.ApolloConnectConfig = cConf
	if option.HasDefaultNamespace {
		option.PreloadNamespaces = append(option.PreloadNamespaces, "application")
	}
	if option.LocalNamespace != "" {
		option.PreloadNamespaces = append(option.PreloadNamespaces, option.LocalNamespace)
	}

	err = agollo.Init(cConf.Apollo["MetaServer"], cConf.Apollo["AppId"],
		agollo.Cluster(cConf.Apollo["Cluster"]),
		agollo.PreloadNamespaces(option.PreloadNamespaces...),
		agollo.WithLogger(agollo.NewLogger(agollo.LoggerWriter(os.Stdout))),
		agollo.AutoFetchOnCacheMiss(),
		agollo.FailTolerantOnBackupExists(),
		agollo.BackupFile(common.ApolloConfigBackupFile),
	)
	if err != nil {
		return nil, err
	}
	//其他APPid
	if len(option.OtherAppId) > 0 {
		otherAgollo = make(map[string]agollo.Agollo)
		for _, id := range option.OtherAppId {
			ag, err := agollo.New(cConf.Apollo["MetaServer"], id,
				agollo.Cluster(cConf.Apollo["Cluster"]),
				agollo.AutoFetchOnCacheMiss(),
				agollo.FailTolerantOnBackupExists(),
				agollo.BackupFile(common.ApolloConfigBackupFile+"-"+id))
			if err != nil {
				return nil, err
			}
			otherAgollo[id] = ag
			ag.Start()
		}
	}
	// 如果想监听并同步服务器配置变化，启动apollo长轮训
	// 返回一个期间发生错误的error channel,按照需要去处理
	errorCh := agollo.Start()

	// 监听apollo配置更改事件
	// 返回namespace和其变化前后的配置,以及可能出现的error
	watchCh := agollo.Watch()

	Change := make(chan Change)
	manager.Change = Change

	go func() {
		for {
			select {
			case err := <-errorCh:
				fmt.Println("Error:", err)
			case resp := <-watchCh:
				fmt.Println("Watch Apollo:", resp)
				if resp.Error != nil {
					log.Logger.Error(resp.Error)
					continue
				}
				change := getChangeForApollo(resp.Namespace, resp.Changes)
				err := UpdateConfigMap(resp.Namespace, resp.NewValue, false)
				if err != nil {
					log.Logger.Error(err)
					return
				}
				Change <- change
				//if resp.Namespace != common.ApolloNamespaceServicehosts {
				//	Change <- resp.Namespace
				//	continue
				//}
				//for i := range resp.Changes {
				//	if resp.Changes[i].Type != agollo.ChangeTypeUpdate {
				//		continue
				//	}
				//	s := fmt.Sprintf("%s_%s", resp.Namespace, resp.Changes[i].Key)
				//	Change <- s
				//}
			}
		}
	}()

	return manager, nil
}

//Apollo配置中心配置变更
func getChangeForApollo(namespace string, changes agollo.Changes) Change {
	change := Change{ConfigGroup: namespace, Items: map[ChangeType][]ChangeItem{}}
	for _, c := range changes {
		items := change.Items[ChangeType(c.Type)]
		if items == nil {
			items = []ChangeItem{}
		}
		oldValue, _ := GetString(namespace, c.Key)
		items = append(items, ChangeItem{Type: ChangeType(c.Type), Key: c.Key, OldValue: oldValue, NewValue: c.Value})
		change.Items[ChangeType(c.Type)] = items
	}
	return change
}

//没有配置中心的情况下从指定目录下读取配置文件，
// 配置文件名必须以数字或大小写字母开头,
// 仅支持内容为json格式的配置文件
//TODO 支持properties、yaml等配置文件
func LoadConfigWithoutConfigCenter(option *FileConfigOptions) (*ConfigManager, error) {
	if option == nil {
		option = &FileConfigOptions{}
	}
	if option.ConfigPath == "" {
		option.ConfigPath = common.DefaultConfigPath
	} else if !strings.HasSuffix(option.ConfigPath, "/") {
		option.ConfigPath += "/"
	}
	fileRealPathMap := map[string]configFileInfo{}
	for _, f := range option.ConfigFiles {
		fileRealPathMap[f] = configFileInfo{}
	}
	ignore := map[string]string{}
	for _, f := range option.IgnoreConfigFiles {
		ignore[f] = f
		delete(fileRealPathMap, f)
	}
	fileInfos, err := ioutil.ReadDir(option.ConfigPath)
	if err != nil {
		return nil, err
	}
	manager = &ConfigManager{
		ConfigMap: sync.Map{}, FileConfigOptions: option,
		LocalConfig: option.LocalFile, Change: make(chan Change),
		GetConfig: func(configGroup string, key string) string {
			return ""
		},
	}
	rex := "^[0-9a-zA-Z][0-9a-zA-Z-_]*$"
	for _, info := range fileInfos {
		if info.IsDir() {
			continue
		}
		if !regexp.MustCompile(rex).MatchString(info.Name()) {
			continue
		}
		if _, ok := ignore[info.Name()]; ok {
			continue
		}
		realPath, _ := filepath.EvalSymlinks(option.ConfigPath + info.Name())
		bs, err := ioutil.ReadFile(realPath)
		if err != nil {
			return nil, err
		}
		if !json.Valid(bs) {
			err = errors.New(info.Name() + " is not a json file")
		}
		if len(option.ConfigFiles) != 0 {
			if _, ok := fileRealPathMap[info.Name()]; !ok {
				continue
			}
			if err != nil {
				return nil, err
			}
		} else {
			if err != nil {
				fmt.Println(err)
				continue
			}
			fileRealPathMap[info.Name()] = configFileInfo{RealPath: realPath, Content: bs}
		}
		m := map[string]interface{}{}
		_ = json.Unmarshal(bs, &m)
		for k, v := range m {
			//value必须为string
			if _, ok := v.(string); !ok {
				return nil, errors.New(fmt.Sprintf("%s.%s must be type string", info.Name(), k))
			}
		}
		err = UpdateConfigMap(info.Name(), m, true)
		if err != nil {
			return nil, err
		}
	}
	watchDir(option.ConfigPath, fileRealPathMap, func(file string) {
		fmt.Println(file + " has changed")
		m := map[string]interface{}{}
		_ = json.Unmarshal(fileRealPathMap[file].Content, &m)
		change := getChange(file, m)
		err := UpdateConfigMap(file, m, true)
		if err != nil {
			return
		}
		manager.Change <- change
	})
	return manager, nil
}

//配置变更
func getChange(configGroup string, m map[string]interface{}) Change {
	return Change{ConfigGroup: configGroup, Items: diffOldAndNew(GetAll(configGroup), m)}
}

//新旧配置对比
func diffOldAndNew(old, new map[string]interface{}) map[ChangeType][]ChangeItem {
	if old == nil {
		old = map[string]interface{}{}
	}
	m := map[ChangeType][]ChangeItem{}
	for k, newValue := range new {
		oldValue, found := old[k]
		if found {
			changes := m[ChangeTypeUpdate]
			if !reflect.DeepEqual(oldValue, newValue) {
				changes = append(changes, ChangeItem{
					Type:     ChangeTypeUpdate,
					Key:      k,
					NewValue: newValue,
					OldValue: oldValue,
				})
				m[ChangeTypeUpdate] = changes
			}
		} else {
			changes := m[ChangeTypeAdd]
			changes = append(changes, ChangeItem{
				Type:     ChangeTypeAdd,
				Key:      k,
				NewValue: newValue,
			})
			m[ChangeTypeAdd] = changes
		}
	}

	for k, oldValue := range old {
		_, found := new[k]
		if !found {
			changes := m[ChangeTypeDelete]
			changes = append(changes, ChangeItem{
				Type:     ChangeTypeDelete,
				Key:      k,
				OldValue: oldValue,
			})
			m[ChangeTypeDelete] = changes
		}
	}
	return m
}

//监听指定目录下的多个配置文件
//输入：
//	path: 监听目录
//	files：被监听的文件信息
//	onChange：文件内容发生改变时触发该函数，删除与不在files的文件不触发
func watchDir(path string, fileRealPathMap map[string]configFileInfo, onChange func(file string)) {
	initWG := sync.WaitGroup{}
	initWG.Add(1)
	doChange := func(fileName, currRealPath string) {
		oldContent := fileRealPathMap[fileName].Content
		currContent, _ := ioutil.ReadFile(currRealPath)
		if len(currContent) == 0 {
			return
		}
		if bytes.Equal(oldContent, currContent) {
			return
		}
		fileRealPathMap[fileName] = configFileInfo{
			RealPath: currRealPath,
			Content:  currContent,
		}
		onChange(fileName)
	}
	go func() {
		watcher, err := fsnotify.NewWatcher()
		if err != nil {
			fmt.Println(utils.GetDebugStack(err, debug.Stack()))
			return
		}
		defer watcher.Close()
		configDir := path

		eventsWG := sync.WaitGroup{}
		eventsWG.Add(1)
		go func() {
			for {
				select {
				case event, ok := <-watcher.Events:
					if !ok { // 'Events' channel is closed
						eventsWG.Done()
						return
					}
					const writeOrCreateMask = fsnotify.Write | fsnotify.Create
					_, file := filepath.Split(event.Name)
					_, exist := fileRealPathMap[file]
					if event.Op&writeOrCreateMask != 0 && exist {
						doChange(file, path+file)
						continue
					}
					//k8s config map挂载的文件
					for k, v := range fileRealPathMap {
						currentConfigFile, _ := filepath.EvalSymlinks(path + k)
						if currentConfigFile != "" && currentConfigFile != v.RealPath {
							doChange(k, currentConfigFile)
							continue
						}
					}
				case err, ok := <-watcher.Errors:
					if ok { // 'Errors' channel is not closed
						fmt.Printf("watcher error: %v\n", err)
					}
					eventsWG.Done()
					return
				}
			}
		}()
		_ = watcher.Add(configDir)
		initWG.Done()   // done initializing the watch in this go routine, so the parent routine can move on...
		eventsWG.Wait() // now, wait for event loop to end in this go-routine...
	}()
	initWG.Wait() // make sure that the go routine above fully ended before returning
}

//从consul加载配置,每一个配置的值都是json格式的字符串
//TODO 支持properties、yaml、hcl等
func LoadConfigWithConsul(options *ConsulConfigOptions) (*ConfigManager, error) {
	if options == nil {
		options = &ConsulConfigOptions{}
	}
	var consulAddr, keyPrefix, datacenter, token string
	configFile := options.ConfigFile
	if configFile == "" {
		configFile = common.DefaultCenterConfigFile
	}
	cConf, err := LoadCenterConfigFormJsonFile(configFile)
	if err != nil {
		return nil, err
	}
	if cConf != nil && cConf.Consul != nil {
		consulAddr = cConf.Consul["Addr"]
		datacenter = cConf.Consul["Datacenter"]
		token = cConf.Consul["Token"]
		keyPrefix = cConf.Consul["KeyPrefix"]
	}
	if options.ConsulAddr != "" {
		consulAddr = options.ConsulAddr
	}
	if options.KeyPrefix != "" {
		keyPrefix = options.KeyPrefix
	}
	if consulAddr == "" {
		host := os.Getenv("HOSTIP")
		if host == "" {
			host = common.DefaultConsulAddr
		}
		host = utils.MaybeIpv6(host)
		consulAddr = host + ":" + common.DefaultConsulPort
	}
	if keyPrefix == "" {
		keyPrefix = GetServiceConfigPrefix()
	}
	fmt.Println("keyPrefix = ", keyPrefix)
	if bts, err := base64.StdEncoding.DecodeString(token); err == nil {
		token = string(bts)
	}
	var (
		//缓存
		cache = map[string][]byte{}
		//通知通道
		changes           = make(chan ConsulConfigChange, 5)
		notifyInitialized = make(chan struct{})
	)
	//创建consul客户端
	consulClient, err := consul.NewConsulClient(consulAddr,
		consul.DataCenterOption(datacenter),
		consul.TokenOption(token))
	if err != nil {
		return nil, err
	}
	//创建consul kv监听计划
	plan, err := watch.Parse(map[string]interface{}{
		"type": "keyprefix", "prefix": keyPrefix,
		"datacenter": datacenter, "token": token,
	})
	if err != nil {
		return nil, err
	}
	manager = &ConfigManager{
		ConfigMap: sync.Map{}, ConsulConfigOptions: options,
		LocalConfig: options.LocalKey, Change: make(chan Change),
		GetConfig: func(configGroup string, key string) string {
			return ""
		}, ConsulClient: consulClient,
	}
	//开始监听时会触发一次,加载前缀为ConsulConfigOptions.KeyPrefix的所有key
	plan.Handler = func(idx uint64, raw interface{}) {
		if raw == nil { // nil is a valid return value
			fmt.Println(options.KeyPrefix + " changed, raw is nil")
			return
		}
		vs, ok := raw.(api.KVPairs)
		if !ok {
			return
		}
		initialized := false
		if len(cache) == 0 {
			initialized = true
		}
		for _, v := range vs {
			if v.Value == nil {
				continue
			}
			kv := v.Value
			if lastV, ok := cache[v.Key]; !ok {
				//初始化
				cache[v.Key] = kv
				_ = initOrUpdate(v.Key, kv, false)
				continue
			} else if bytes.Equal(lastV, kv) {
				continue
			}

			//变化
			changes <- ConsulConfigChange{
				Key: v.Key,
				Old: cache[v.Key],
				New: kv,
			}
			cache[v.Key] = kv
		}

		if initialized {
			select {
			case <-notifyInitialized:
				//pass
			default:
				close(notifyInitialized)
			}
		}
	}
	go func() {
		defer func() {
			err := recover()
			fmt.Println("watch plan run: ", err)
		}()
		if err := plan.Run(consulAddr); err != nil {
			fmt.Printf("consul config err: %v", err)
		}
	}()
	go func() {
		defer func() {
			err := recover()
			fmt.Println("config change: ", err)
		}()
		for {
			c := <-changes
			fmt.Println("key =", c.Key, " is changed")
			err := initOrUpdate(c.Key, c.New, true)
			if err != nil {
				continue
			}
		}
	}()
	//等待初始化完成
	fmt.Println("Waiting to read the config from the consul")
	<-notifyInitialized
	return manager, nil
}

//初始化或更新缓存到内存的consul配置
//input:
//	fullKey: consul完整的key
//	value: fullKey对应的值
//	notify: 是否通知配置发生变化，true为通知
func initOrUpdate(fullKey string, value []byte, notify bool) error {
	s := strings.Split(fullKey, "/")
	keySuffix := s[len(s)-1]
	//更新
	m := map[string]interface{}{}
	err := json.Unmarshal(value, &m)
	if err != nil {
		fmt.Println("Unable to unmarshal the value of " + fullKey + " to map, only json is supported")
		return err
	}
	change := getChange(keySuffix, m)
	fmt.Println(keySuffix)
	err = UpdateConfigMap(keySuffix, m, true)
	if err != nil {
		fmt.Println(err)
		return err
	}
	if notify {
		manager.Change <- change
	}
	return nil
}

/**
 *根据配置项名称获取配置项值
 *输入：
 *     configGroup - 配置项组
 *     key - 配置项名称
 *返回： 配置项值, default = ""
 */
func GetString(configGroup, key string) (string, error) {
	if configGroup == "" {
		return "", nil
	}
	var nsConfig *sync.Map
	if nsMap, ok := manager.ConfigMap.Load(configGroup); ok {
		nsConfig = nsMap.(*sync.Map)
		if v, ok := nsConfig.Load(key); ok {
			return v.(string), nil
		}
	} else {
		nsConfig = &sync.Map{}
		manager.ConfigMap.Store(configGroup, nsConfig)
	}
	v := manager.GetConfig(configGroup, key)
	v = strings.TrimSpace(v)
	if v == "" {
		return v, errors.New(fmt.Sprintf("%s.%s does not exist or is empty string.", configGroup, key))
	}
	if strings.HasPrefix(v, common.DecryptConfigValuePrefix) {
		v = strings.Split(v, common.DecryptConfigValuePrefix)[1]
		nv, err := utils.CommonDecrypt(v)
		if err == nil {
			v = nv
		}
	}
	nsConfig.Store(key, v)
	return v, nil
}

//配置值为 "1", "t", ”T", "TRUE", "true", "True",则返回true,nil
// 0, f, F, FALSE, false, False,则返回false,nil
func GetBool(configGroup, key string) (bool, error) {
	v, err := GetString(configGroup, key)
	if err != nil {
		return false, err
	}
	return strconv.ParseBool(v)
}

//配置值转int类型
func GetUint(configGroup, key string) (uint, error) {
	v, err := GetString(configGroup, key)
	if err != nil {
		return 0, err
	}
	i, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return 0, err
	}
	return uint(i), nil
}

//配置值转int类型
func GetInt(configGroup, key string) (int, error) {
	v, err := GetString(configGroup, key)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(v)
}

func GetInt64(configGroup, key string) (int64, error) {
	v, err := GetString(configGroup, key)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(v, 10, 64)
}

func GetFloat64(configGroup, key string) (float64, error) {
	v, err := GetString(configGroup, key)
	if err != nil {
		return 0, err
	}
	return strconv.ParseFloat(v, 64)
}

//将配置值转成time.Duration
func GetDuration(configGroup, key string) (d time.Duration, err error) {
	var s string
	s, err = GetString(configGroup, key)
	if err != nil {
		return
	}
	if strings.ContainsAny(s, "nsuµmh") {
		d, _ = time.ParseDuration(s)
	} else {
		d, _ = time.ParseDuration(s + "ns")
	}
	return
}

//Apollo: 获取默认APPid下指定名称空间下的所有配置
//Consul: 获取指定配置组下的所有配置
//本地文件: 获取指定配置文件下的所有配置
//如果不存在则返回nil
func GetAll(configGroup string) map[string]interface{} {
	v, ok := manager.ConfigMap.Load(configGroup)
	if !ok {
		return nil
	}
	g := v.(*sync.Map)
	m := map[string]interface{}{}
	g.Range(func(key, value interface{}) bool {
		m[key.(string)] = value
		return true
	})
	return m
}

//获取指定APPid下指定名称空间下的所有配置
func GetNamespaceByAppId(ns, appid string) (map[string]interface{}, error) {
	if _, ok := otherAgollo[appid]; ok {
		return otherAgollo[appid].GetNameSpace(ns), nil
	}
	return nil, errors.New("the Apollo config instance corresponding to " + appid + " was not initialized")
}

/**
 *根据配置项名称获取当前服务的配置项值
 *输入：
 *     configGroup - 配置项组
 *     key - 配置项名称
 *返回： 配置项值, default = ""
 */
func GetLocalConfig(key string) (string, error) {
	return GetString(manager.LocalConfig, key)
}

func GetLocalConfigForBool(key string) (bool, error) {
	return GetBool(manager.LocalConfig, key)
}

//配置值转int类型
func GetLocalConfigForUint(key string) (uint, error) {
	return GetUint(manager.LocalConfig, key)
}

//配置值转int类型
func GetLocalConfigForInt(key string) (int, error) {
	return GetInt(manager.LocalConfig, key)
}

func GetLocalConfigForInt64(key string) (int64, error) {
	return GetInt64(manager.LocalConfig, key)
}

func GetLocalConfigForFloat64(key string) (float64, error) {
	return GetFloat64(manager.LocalConfig, key)
}

//将配置值转成time.Duration
func GetLocalConfigForDuration(key string) (time.Duration, error) {
	return GetDuration(manager.LocalConfig, key)
}

//获取本地所有配置
func GetAllLocalConfig() map[string]interface{} {
	return GetAll(manager.LocalConfig)
}

//更新配置
//	coverAll: true表示覆盖所有配置项，false则以有增无减的方式更新配置
func UpdateConfigMap(configGroup string, resp map[string]interface{}, coverAll bool) error {
	//var group *sync.Map
	//v, ok := manager.ConfigMap.Load(configGroup)
	//if ok{
	//
	//}
	if coverAll {
		manager.ConfigMap.Delete(configGroup)
	}
	for key, _ := range resp {
		var group *sync.Map
		if groupMap, ok := manager.ConfigMap.Load(configGroup); !ok {
			group = &sync.Map{}
			manager.ConfigMap.Store(configGroup, group)
		} else {
			group = groupMap.(*sync.Map)
		}
		v, ok := resp[key].(string)
		if !ok {
			fmt.Println(configGroup + "." + key + " must be type string")
			continue
		}
		v = strings.TrimSpace(v)
		if strings.HasPrefix(v, common.DecryptConfigValuePrefix) {
			v = strings.Split(v, common.DecryptConfigValuePrefix)[1]
			nv, err := utils.CommonDecrypt(v)
			if err == nil {
				v = nv
			}
		}
		group.Store(key, v)
	}
	return nil
}

//判断两个map是否不同
func hasDiff(m1 map[string]interface{}, m2 map[string]interface{}) bool {
	if len(m1) != len(m2) {
		return true
	}
	for k, v := range m1 {
		mv, ok := m2[k]
		if !ok {
			return true
		}
		if mv.(string) != v.(string) {
			return true
		}
	}
	return false
}

type Format string

var (
	Properties          Format = "properties"
	Xml                 Format = "xml"
	Json                Format = "json"
	Yaml                Format = "yaml"
	Yml                 Format = "yml"
	DataChangeCreatedBy        = "apollo"
)

//只对APPid=ThirdPartyApp生效
type Namespace struct {
	Name                  string        `json:"name"`                //配置项集合
	NamespaceName         string        `json:"namespaceName"`       //配置项集合，在获取所有namespace时才有值
	Format                Format        `json:"format"`              //配置格式，properties、xml、json、yml、yaml
	IsPublic              bool          `json:"isPublic"`            //是否是公共文件
	AppId                 string        `json:"appId"`               //Apollo配置中心项目应用id，默认ThirdPartyApp
	DataChangeCreatedBy   string        `json:"dataChangeCreatedBy"` //namespace的创建人,默认apollo
	token                 string        `json:"-"`                   //认证token
	cluster               string        `json:"-"`
	Items                 []ConfigItems `json:"items"`
	AppendNamespacePrefix bool          `json:"appendNamespacePrefix"` //namespace是否添加前缀
}

type ConfigItems struct {
	Key                      string `json:"key"`
	Value                    string `json:"value"`
	Comment                  string `json:"comment"`
	DataChangeCreatedBy      string `json:"dataChangeCreatedBy"`      //namespace的创建人,默认apollo
	DataChangeLastModifiedBy string `json:"dataChangeLastModifiedBy"` //namespace的创建人,默认apollo
}

type ReleaseConfig struct {
	ReleaseTitle   string `json:"releaseTitle"`   //此次发布的标题，长度不能超过64个字符
	ReleaseComment string `json:"releaseComment"` //发布的备注，长度不能超过256个字符
	//发布人，域账号，注意：如果ApolloConfigDB.ServerConfig中的namespace.lock.switch设置为true的话（默认是false），
	// 那么该环境不允许发布人和编辑人为同一人。所以如果编辑人是zhanglea，发布人就不能再是zhanglea。
	ReleasedBy string `json:"releasedBy"`
}

type NamespaceOptions struct {
	AppId               string
	DataChangeCreatedBy string
	Format              Format
	Cluster             string
}

type NsOption func(options *NamespaceOptions)

func SetAppId(id string) NsOption {
	return func(options *NamespaceOptions) {
		options.AppId = id
	}
}

func SetCreatedBy(createBy string) NsOption {
	return func(options *NamespaceOptions) {
		options.DataChangeCreatedBy = createBy
	}
}

func SetFormat(fmt Format) NsOption {
	return func(options *NamespaceOptions) {
		options.Format = fmt
	}
}

func SetCluster(cluster string) NsOption {
	return func(options *NamespaceOptions) {
		options.Cluster = cluster
	}
}

//定义namespace
//输入:
//	name:名称
//	isPublic:是否公开。true表示公开，所有客户端都可见；false表示不公开，仅当前APPid的客户端可见，建议false。
//	options: 可选项，SetAppId(),SetCluster()...
func NewNamespace(name string, isPublic bool, options ...NsOption) (*Namespace, error) {
	appId := common.ApolloAppCommon
	dataChangeCreatedBy := DataChangeCreatedBy
	format := Properties
	cluster := "default"
	opt := new(NamespaceOptions)
	for i := range options {
		options[i](opt)
	}
	if opt.AppId != "" {
		appId = opt.AppId
	}
	if opt.DataChangeCreatedBy != "" {
		dataChangeCreatedBy = opt.DataChangeCreatedBy
	}
	if opt.Format != "" {
		format = opt.Format
	}
	if opt.Cluster != "" {
		cluster = opt.Cluster
	}
	token, err := GetString(common.ApolloNamespaceThirdAppToken, appId)
	if err != nil {
		return nil, err
	}
	return &Namespace{
		Name: name, Format: format, IsPublic: isPublic, cluster: cluster,
		AppId: appId, DataChangeCreatedBy: dataChangeCreatedBy, token: token,
	}, nil
}

//通过Apollo open api创建namespace
//只有被授权的Apollo项目/应用才能操作
func CreateNamespace(ns *Namespace) error {
	url := fmt.Sprintf("%s/openapi/v1/apps/%s/appnamespaces",
		apolloConnect["Portal"], ns.AppId)
	bs, _ := json.Marshal(ns)
	return ns.doApolloRequest("create namespace", http.MethodPost, url, bytes.NewBuffer(bs))
}

//Apollo open api不提供删除namespace的接口，
// 可以通过管理员工具删除namespace，但需要先用管理员登录，
//管理员的用户名与密码放在当前服务的配置中
//输入：
//	appid：默认为common.ApolloAppCommon
//	ns: apollo namespace
//func DeleteNamespace(appid, ns string) error {
//	if appid == "" {
//		appid = common.ApolloAppCommon
//	}
//	var portal string
//	var ok bool
//	if portal, ok = apolloConnect["Portal"]; !ok || portal == "" {
//		return errors.New("apollo portal addr must be not empty")
//	}
//	username, err := GetLocalConfig("ApolloAdmin")
//	if err != nil {
//		return err
//	}
//	pwd, err := GetLocalConfig("ApolloAdminPassword")
//	if err != nil {
//		return err
//	}
//	//login to get JSESSIONID
//	sessionId, err := apolloLogin(portal, username, pwd)
//	if err != nil {
//		return err
//	}
//	deleleUrl := fmt.Sprintf("%s/apps/%s/appnamespaces/%s", portal, appid, ns)
//	err = deleteNamespace(deleleUrl, sessionId)
//	if err != nil {
//		return err
//	}
//	return nil
//}
//
////删除namespace
//func deleteNamespace(url, sessionId string) error {
//	req, err := http.NewRequest(http.MethodDelete, url, nil)
//	if err != nil {
//		return err
//	}
//	cookie := http.Cookie{Name: "JSESSIONID", Value: sessionId}
//	req.AddCookie(&cookie)
//	resp, err := new(http.Client).Do(req)
//	if err != nil {
//		return err
//	}
//	defer resp.Body.Close()
//	if resp.StatusCode == 200 {
//		return nil
//	}
//	bytes, err := ioutil.ReadAll(resp.Body)
//	if err != nil {
//		return err
//	}
//	action := "delete namespace"
//	log.Logger.Info(action, " result:", string(bytes))
//	if strings.Contains(string(bytes), "status") {
//		rs := gjson.GetManyBytes(bytes, "status", "message")
//		return OpenApiError{Action: action, Status: int(rs[0].Int()), Message: rs[1].String()}
//	}
//	return errors.New("unknown error")
//}

//使用管理员登录Apollo配置中心
//func apolloLogin(portal, username, password string) (string, error) {
//	client := new(http.Transport)
//	data := neturl.Values{}
//	data.Set("username", username)
//	data.Set("password", password)
//	req, err := http.NewRequest(http.MethodPost, portal+"/signin", strings.NewReader(data.Encode()))
//	if err != nil {
//		return "", err
//	}
//	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
//	resp, err := client.RoundTrip(req)
//	if err != nil {
//		return "", err
//	}
//	if resp.StatusCode != 302 {
//		return "", errors.New("login apollo failed, get JSESSIONID failed, status code = " + strconv.Itoa(resp.StatusCode))
//	}
//	//获取sessionId
//	var sessionId string
//	for _, c := range resp.Cookies() {
//		if c.Name == "JSESSIONID" {
//			sessionId = c.Value
//			break
//		}
//	}
//	if sessionId == "" {
//		return "", errors.New("login apollo failed, get JSESSIONID failed, JSESSIONID is empty")
//	}
//
//	//验证sessionId是否有效
//	req, err = http.NewRequest(http.MethodGet, portal, nil)
//	if err != nil {
//		return "", err
//	}
//	req.Header.Add("Cookie", "JSESSIONID="+sessionId)
//	resp, err = client.RoundTrip(req)
//	if err != nil {
//		return "", err
//	}
//	if resp.StatusCode != 200 {
//		return "", errors.New("login apollo failed, JSESSIONID is invalid, status code = " + strconv.Itoa(resp.StatusCode))
//	}
//	return sessionId, nil
//}

//设置认证请求头
func (ns *Namespace) setAuthHeader(req *http.Request) {
	req.Header.Set("Content-Type", "application/json;charset=UTF-8")
	req.Header.Set("Authorization", ns.token)
}

//创建一个配置
func (ns *Namespace) CreateConfig(item *ConfigItems) error {
	url := fmt.Sprintf("%s/openapi/v1/envs/%s/apps/%s/clusters/%s/namespaces/%s/items",
		apolloConnect["Portal"], apolloConnect["Env"], ns.AppId,
		ns.cluster, ns.Name)
	if item.DataChangeCreatedBy == "" {
		item.DataChangeCreatedBy = ns.DataChangeCreatedBy
	}
	bs, _ := json.Marshal(item)
	return ns.doApolloRequest("create apollo config items", http.MethodPost, url, bytes.NewBuffer(bs))
}

//删除一个配置
//输入:
//	key:指定配置的key
//	operator: 操作者
func (ns *Namespace) DeleteConfig(key string, operator ...string) error {
	oper := ns.DataChangeCreatedBy
	if len(operator) != 0 && operator[0] != "" {
		oper = operator[0]
	}
	url := fmt.Sprintf("%s/openapi/v1/envs/%s/apps/%s/clusters/%s/namespaces/%s/items/%s?operator=%s",
		apolloConnect["Portal"], apolloConnect["Env"], ns.AppId,
		ns.cluster, ns.Name, key, oper)
	return ns.doApolloRequest("delete apollo config by key", http.MethodDelete, url, nil)
}

//修改一个配置
func (ns *Namespace) UpdateConfig(item *ConfigItems) error {
	url := fmt.Sprintf("%s/openapi/v1/envs/%s/apps/%s/clusters/%s/namespaces/%s/items/%s",
		apolloConnect["Portal"], apolloConnect["Env"], ns.AppId,
		ns.cluster, ns.Name, item.Key)
	if item.DataChangeLastModifiedBy == "" {
		item.DataChangeLastModifiedBy = ns.DataChangeCreatedBy
	}
	bs, _ := json.Marshal(item)
	return ns.doApolloRequest("update apollo config items", http.MethodPut, url, bytes.NewBuffer(bs))
}

//发布一个配置
func (ns *Namespace) ReleaseConfig(r *ReleaseConfig) error {
	url := fmt.Sprintf("%s/openapi/v1/envs/%s/apps/%s/clusters/%s/namespaces/%s/releases",
		apolloConnect["Portal"], apolloConnect["Env"], ns.AppId,
		ns.cluster, ns.Name)
	if r.ReleasedBy == "" {
		r.ReleasedBy = ns.DataChangeCreatedBy
	}
	bs, _ := json.Marshal(r)
	return ns.doApolloRequest("release apollo config", http.MethodPost, url, bytes.NewBuffer(bs))
}

//发起Apollo请求
//输入:
//	acton: 动作描述
//	method：请求方式
//	url：请求地址
//	body：请求体
//输出:
//	操作结果
func (ns *Namespace) doApolloRequest(action, method, url string, body io.Reader) error {
	client := new(http.Client)
	log.Logger.Info(action, " request url:", url)
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return err
	}
	ns.setAuthHeader(req)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	bts, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	log.Logger.Info(action, " result:", string(bts))
	if strings.Contains(string(bts), "status") {
		rs := gjson.GetManyBytes(bts, "status", "message")
		return OpenApiError{Action: action, Status: int(rs[0].Int()), Message: rs[1].String()}
	}
	return nil
}

//获取指定APPid下所有配置,仅限open api
func UseOpenApiGetNamespacesByAppId(appid string) ([]Namespace, error) {
	if _, ok := otherAgollo[appid]; !ok {
		return nil, errors.New("the Apollo config instance corresponding to " + appid + " was not initialized")
	}
	token, err := GetString(common.ApolloNamespaceThirdAppToken, appid)
	if err != nil {
		return nil, err
	}
	url := fmt.Sprintf("%s/openapi/v1/envs/%s/apps/%s/clusters/%s/namespaces",
		apolloConnect["Portal"], apolloConnect["Env"], appid, apolloConnect["Cluster"])
	client := new(http.Client)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json;charset=UTF-8")
	req.Header.Set("Authorization", token)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	bs, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	action := "get all namespace"
	log.Logger.Info(action, " result:", string(bs))
	if strings.Contains(string(bs), "status") {
		rs := gjson.GetManyBytes(bs, "status", "message")
		return nil, OpenApiError{Action: action, Status: int(rs[0].Int()), Message: rs[1].String()}
	}
	var res []Namespace
	err = json.Unmarshal(bs, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

//操作Apollo开放平台接口错误
type OpenApiError struct {
	Action  string
	Message string
	Status  int
}

func (oae OpenApiError) Error() string {
	return fmt.Sprintf("OpenApiError{action=%s, message=%s, status=%d}", oae.Action, oae.Message, oae.Status)
}

//Apollo开放平台接口404错误
func IsNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	if e, ok := err.(OpenApiError); ok {
		return e.IsNotFound()
	}
	return false
}

func (oae OpenApiError) IsNotFound() bool {
	if oae.Status == 404 {
		return true
	}
	return false
}

//Apollo开放平台接口400错误
func IsAlreadyExistsError(err error) bool {
	if err == nil {
		return false
	}
	if e, ok := err.(OpenApiError); ok {
		return e.IsAlreadyExists()
	}
	return false
}

func (oae OpenApiError) IsAlreadyExists() bool {
	if oae.Status == 400 && strings.Contains(oae.Message, "already exists") {
		return true
	}
	return false
}

//Apollo开放平台接口服务内部错误
func IsInternalServerError(err error) bool {
	if err == nil {
		return false
	}
	if e, ok := err.(OpenApiError); ok {
		return e.IsInternalError()
	}
	return false
}

func (oae OpenApiError) IsInternalError() bool {
	if oae.Status == 500 {
		return true
	}
	return false
}

//Apollo开放平台接口未认证错误
func IsUnauthorizedError(err error) bool {
	if err == nil {
		return false
	}
	if e, ok := err.(OpenApiError); ok {
		return e.IsUnauthorized()
	}
	return false
}

func (oae OpenApiError) IsUnauthorized() bool {
	if oae.Status == 401 {
		return true
	}
	return false
}

//Apollo开放平台接口未授权错误
func IsForbiddenError(err error) bool {
	if err == nil {
		return false
	}
	if e, ok := err.(OpenApiError); ok {
		return e.IsForbidden()
	}
	return false
}

func (oae OpenApiError) IsForbidden() bool {
	if oae.Status == 403 {
		return true
	}
	return false
}

//获取consul配置组实例
func GetConsulConfigGroup(groupName string) (*ConsulConfigGroup, error) {
	v, ok := manager.ConfigMap.Load(groupName)
	if !ok {
		return nil, errors.New(fmt.Sprintf("config group %s is not exsit", groupName))
	}
	g := v.(*sync.Map)
	return &ConsulConfigGroup{syncMap: g, Group: groupName}, nil
}

//consul配置组
type ConsulConfigGroup struct {
	Group   string
	syncMap *sync.Map
}

//转map
func (g *ConsulConfigGroup) ToMap() map[string]interface{} {
	items := map[string]interface{}{}
	g.syncMap.Range(func(key, value interface{}) bool {
		items[key.(string)] = value
		return true
	})
	return items
}

//新增或更新指定配置
//输入：
// name:要新增或更新的配置
// value：新值
//输出：
// 错误
func (g *ConsulConfigGroup) AddOrUpdateItem(name, value string) error {
	g.syncMap.Store(name, value)
	return g.sendConsul()
}

//删除指定配置
//输入：
// name:要删除的配置
//输出：
// 错误
func (g *ConsulConfigGroup) DelItem(name string) error {
	g.syncMap.Delete(name)
	return g.sendConsul()
}

//将配置发送到consul
func (g *ConsulConfigGroup) sendConsul() error {
	m := g.ToMap()
	bts, _ := json.Marshal(&m)
	kvPair := &api.KVPair{
		Key:   g.Group,
		Value: bts,
	}
	//center := consulConnect["Datacenter"]
	//token := consulConnect["Token"]
	//opts := &api.WriteOptions{}
	//if center != "" {
	//	opts.Datacenter = center
	//}
	//if token != "" {
	//	opts.Token = token
	//}
	_, err := manager.ConsulClient.Client.KV().Put(kvPair, &api.WriteOptions{})
	return err
}

//获取环境变量PROJECT，如果为空，返回公共变量env-config
func GetEnvProject() string {
	project := os.Getenv("PROJECT")
	if project == "" {
		return common.Project
	}
	return project
}

//获取环境变量CONFIG_SPACE，如果为空，返回公共变量common.DefaultConfigSpace
func GetEnvConfigSpace() string {
	space := os.Getenv("CONFIG_SPACE")
	if space == "" {
		return common.DefaultConfigSpace
	}
	return space
}

//获取环境变量CONFIG_PROJECT，如果为空，返回公共变量common.DefaultConfigProject
func GetEnvConfigProject() string {
	project := os.Getenv("CONFIG_PROJECT")
	if project == "" {
		return common.DefaultConfigProject
	}
	return project
}

//获取环境变量SERVICE_TAG，如果为空，返回公共变量common.ConsulServiceTag
func GetEnvServiceTag() string {
	tag := os.Getenv("SERVICE_TAG")
	if tag == "" {
		return common.ConsulServiceTag
	}
	return tag
}

//获取consul kv配置路径
func GetServiceConfigPrefix() string {
	return fmt.Sprintf("%s/%s/", GetEnvConfigSpace(), GetEnvConfigProject())
}
