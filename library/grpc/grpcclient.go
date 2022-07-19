/**
 *@Author: pangj
 *@Description:
 *@File: grpcclient
 *@Version:
 *@Date: 2022/07/11/11:16
 */

package grpc

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/keepalive"
	"library_Test/common"
	"library_Test/library/config"
	"library_Test/library/log"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrConnShutdown = errors.New("grpc conn shutdown")
)

type GRPC struct {
	lock   *sync.Mutex
	Pool   *sync.Map
	Config *GrpcClientConfig
}

type ClientOption struct {
	ClientPoolConnSize int
	DialTimeOut        time.Duration
	KeepAlive          time.Duration
	KeepAliveTimeout   time.Duration
	Protocol           Protocol //协议，默认是tcp
}

type ClientPool struct {
	target string
	option *ClientOption
	next   int64
	cap    int64

	sync.Mutex

	conns []*grpc.ClientConn
}

//type Client struct {
//	Client *grpc.ClientConn
//}

//连接池配置
type GrpcClientConfig struct {
	DisableReload bool              //禁止重新加载资源。当配置改变时，默认会重新加载资源，设置为true可以禁用它
	AddrFrom      ServiceAddrFrom   //从哪里获取服务地址，默认是从配置中心
	ServerAddress map[string]string //rpc服务地址，key为rpc服务名称，value为rpc服务地址, addrFrom 为Invoke时必选
	ServerNames   []string          //服务信息，服务名称，必选
}

func (p Protocol) String() string {
	return string(p)
}

type ServiceAddrFrom string

const (
	FromConfigCenter ServiceAddrFrom = "ConfigCenter" //rpc服务地址来自于配置中心
	FromInvoker      ServiceAddrFrom = "Invoker"      //rpc服务地址由调用者提供
)

func (r *GRPC) GetGrpcRemoteClient(serviceName string) (*grpc.ClientConn, error) {
	k := serviceName
	var addr string

	var pool *ClientPool
	if v, ok := r.Pool.Load(k); !ok {
		//return nil, errors.New(fmt.Sprintf("Uninitialized rpc client pool of %s %s", serviceName, servicePath))
		if r.Config.AddrFrom == FromInvoker {
			return nil, errors.New(fmt.Sprintf("uninitialized rpc client pool of %s\n", serviceName))
		}
		//创建连接池
		p, err := r.CreateClientPool(serviceName, addr)
		if err != nil {
			return nil, err
		}
		pool = p

	} else {
		pool = v.(*ClientPool)
	}

	c, err := pool.getConn()
	if err != nil {
		return nil, err
	}
	//TODO
	if log.TracerIsEnabled() {
		//p := &client.OpenTracingPlugin{}
		//pc := client.NewPluginContainer()
		//pc.Add(p)
		//c.SetPlugins(pc)
	}
	return c, nil
}

func (r *GRPC) GetGrpcRemoteClientWithAddr(addr string) (*grpc.ClientConn, error) {
	k := addr

	var pool *ClientPool
	if v, ok := r.Pool.Load(k); !ok {
		//创建连接池
		pool = NewClientPoolWithOption(addr, &ClientOption{})
	} else {
		pool = v.(*ClientPool)
	}

	c, err := pool.getConn()
	if err != nil {
		return nil, err
	}
	//TODO
	if log.TracerIsEnabled() {
	}
	return c, nil
}

//根据服务名称、服务路径、创建客户端连接池
//如果AddrFrom==FromInvoker,则服务地址不能为空
func (r *GRPC) CreateClientPool(serviceName, serviceAddress string) (*ClientPool, error) {
	return createClientPool(r, serviceName, serviceAddress)
}

//创建连接池
func createClientPool(r *GRPC, serviceName, address string) (*ClientPool, error) {
	key := serviceName
	r.lock.Lock()
	defer r.lock.Unlock()
	if v, ok := r.Pool.Load(key); ok {
		return v.(*ClientPool), nil
	}
	log.Logger.WithField("ServiceName", serviceName).Info("createClientPool")
	var addr string
	var err error
	switch r.Config.AddrFrom {
	case FromInvoker:
		if address == "" {
			err := errors.New("address cannot be empty, because AddrFrom is " + string(r.Config.AddrFrom))
			return nil, err
		}
		addr = address
	case FromConfigCenter:
		if address == "" {
			addr, err = config.GetString(common.ConsulServicehosts, serviceName)
			if err != nil {
				return nil, err
			}
		} else {
			addr = address
		}
	default:
		return nil, errors.New("unsupported AddrFrom")
	}
	pool := NewClientPoolWithOption(addr, &ClientOption{})
	r.Pool.Store(key, pool)
	return pool, nil
}

/**
 *初始化grpc客户端，对每一个rpc server的每一个servicePath建立一个连接池
 *serverInfo：
 *		key: serviceName 服务名称, 通过该名称从ServiceHosts配置中获取服务地址
 *		value: 服务路径
 */
func InitGrpcClientPool(conf *GrpcClientConfig) (*GRPC, error) {

	if conf.AddrFrom == "" {
		//默认从配置中心
		conf.AddrFrom = FromConfigCenter
	}

	r := GRPC{Pool: &sync.Map{}, Config: conf, lock: &sync.Mutex{}}

	for _, serviceName := range conf.ServerNames {
		var addr string
		var err error
		switch conf.AddrFrom {
		case FromInvoker:
			if conf.ServerAddress == nil {
				return nil, errors.New("GrpcClientPoolConf.ServerAddress must be not nil")
			}
			//自定义服务地址
			var ok bool
			if addr, ok = conf.ServerAddress[serviceName]; !ok {
				return nil, errors.New("the address of rpc server " + serviceName + " is not exist")
			}
		case FromConfigCenter:
			//从配置中心ServiceHosts配置中读取服务地址
			addr, err = config.GetString(common.ConsulServicehosts, serviceName)
			if err != nil {
				return nil, err
			}
		default:
			return nil, errors.New("unsupported AddrFrom")
		}
		if addr == "" {
			return nil, errors.New("the address of rpc server " + serviceName + " is not exist")
		}
		pool := NewClientPoolWithOption(addr, &ClientOption{})
		r.Pool.Store(serviceName, pool)
	}

	return &r, nil
}

//关闭rpc
func (r *GRPC) Close() error {
	if r.Pool == nil {
		return nil
	}
	r.Pool.Range(func(key, value interface{}) bool {
		if value == nil {
			return true
		}
		pool := value.(*ClientPool)
		pool.Close()
		log.Logger.Info("close rpc client pool, name = ", key)
		return true
	})
	return nil
}

func NewClientPoolWithOption(target string, option *ClientOption) *ClientPool {

	if (option.ClientPoolConnSize) <= 0 {
		option.ClientPoolConnSize = common.DefaultRpcClientPoolSize
	}

	if option.DialTimeOut <= 0 {
		option.DialTimeOut = common.DefaultRpcDialTimeout
	}

	if option.KeepAlive <= 0 {
		option.KeepAlive = common.DefaultRpcClientTimeout
	}

	if option.KeepAliveTimeout <= 0 {
		option.KeepAliveTimeout = common.DefaultRpcKeepAliveTimeout
	}

	if option.Protocol == "" {
		option.Protocol = Tcp
	}

	return &ClientPool{
		target: target,
		option: option,
		cap:    int64(option.ClientPoolConnSize),
		conns:  make([]*grpc.ClientConn, option.ClientPoolConnSize),
	}
}

func (cp *ClientPool) Close() {
	cp.Lock()
	defer cp.Unlock()

	for _, conn := range cp.conns {
		if conn == nil {
			continue
		}

		conn.Close()
	}
}

func (cc *ClientPool) getConn() (*grpc.ClientConn, error) {
	var (
		idx  int64
		next int64
		err  error
	)

	next = atomic.AddInt64(&cc.next, 1)
	idx = next % cc.cap
	conn := cc.conns[idx]
	if conn != nil && cc.checkState(conn) == nil {
		return conn, nil
	}

	//关掉异常的连接
	if conn != nil {
		conn.Close()
	}

	cc.Lock()
	defer cc.Unlock()

	//检查连接状态
	if conn != nil && cc.checkState(conn) == nil {
		return conn, nil
	}

	conn, err = cc.connect()
	if err != nil {
		return nil, err
	}

	cc.conns[idx] = conn
	return conn, nil
}

func (cc *ClientPool) checkState(conn *grpc.ClientConn) error {
	state := conn.GetState()
	switch state {
	case connectivity.TransientFailure, connectivity.Shutdown:
		return ErrConnShutdown
	}

	return nil
}

func (cc *ClientPool) connect() (*grpc.ClientConn, error) {
	ctx, cal := context.WithTimeout(context.TODO(), cc.option.DialTimeOut)
	defer cal()
	conn, err := grpc.DialContext(ctx,
		cc.target,
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    cc.option.KeepAlive,
			Timeout: cc.option.KeepAliveTimeout,
		}))
	if err != nil {
		return nil, err
	}

	return conn, nil
}
