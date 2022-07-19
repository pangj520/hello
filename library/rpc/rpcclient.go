/**
 *描述：rpc客户端
 *      实现获取远程rpc engpoint对象
 *      向其他文件提供获得rpc远程接口方法
 *作者：江洪
 *时间：2019-5-20
 */

package rpc

import (
	"context"
	"errors"
	"fmt"
	"github.com/smallnest/rpcx/client"
	"library_Test/common"
	"library_Test/library/config"
	"library_Test/library/log"
	"library_Test/library/servicediscovery"
	"sync"
	"time"
)

type RPC struct {
	lock *sync.Mutex
	Pool *sync.Map
	//Pool map[string]*client.XClientPool
	Config *RpcClientPoolConfig
}

func NewRPC(lock *sync.Mutex, pool *sync.Map, config *RpcClientPoolConfig) *RPC {
	return &RPC{lock: lock, Pool: pool, Config: config}
}

type Client struct {
	client client.XClient
}

func NewClient(c client.XClient) *Client {
	return &Client{
		client: c,
	}
}

//连接池配置
type RpcClientPoolConfig struct {
	DisableReload bool //禁止重新加载资源。当配置改变时，默认会重新加载资源，设置为true可以禁用它
	//Deprecated: 已被AddrFrom代替
	//false，使用配置中心获取rpc服务地址，效果与FromConfigCenter相同
	// true，从ServerAddress中获取，效果与FromInvoker相同
	NoApolloConfigCenter bool
	AddrFrom             ServiceAddrFrom     //从哪里获取服务地址，默认是从配置中心
	ServerAddress        map[string]string   //rpc服务地址，key为rpc服务名称，value为rpc服务地址
	ServerInfo           map[string][]string //服务信息，key为服务名称，value为服务路径，服务路径可以有多个
	PoolSize             int                 //连接池大小，默认是common.DefaultRpcClientPoolSize
	Protocol             Protocol            //协议，默认是tcp

	//服务发现实例，可选，如果AddrFrom=FromDiscovery时，必选
	Discovery servicediscovery.ServiceDiscovery

	//服务过滤自定义配置，key为服务名称，value为过滤配置，AddrFrom=FromDiscovery时有效
	CustomServiceFilter map[string]*servicediscovery.ServiceFilterOptions
}

//连接配置
type RpcClientConfig struct {
	AddrFrom      ServiceAddrFrom                        //从哪里获取服务地址，默认是从配置中心
	ServiceName   string                                 //服务名称
	ServerPath    string                                 //服务路径
	Protocol      Protocol                               //协议，默认是tcp
	ServerAddress string                                 //rpc服务地址，可选，如果AddrFrom=FromInvoker时，必选
	Discovery     servicediscovery.ServiceDiscovery      //服务发现实例，可选，如果AddrFrom=FromDiscovery时，必选
	ServiceFilter *servicediscovery.ServiceFilterOptions //服务过滤可选项，可选，AddrFrom=FromDiscovery时有效
}

type ServiceAddrFrom string

const (
	FromDiscovery    ServiceAddrFrom = "Discovery"    //rpc服务地址来自于服务发现
	FromConfigCenter ServiceAddrFrom = "ConfigCenter" //rpc服务地址来自于配置中心
	FromInvoker      ServiceAddrFrom = "Invoker"      //rpc服务地址由调用者提供
)

/**
 *初始化rpc客户端，对每一个rpc server的每一个servicePath建立一个连接池
 *serverInfo：
 *		key: serviceName 服务名称, 通过该名称从ServiceHosts配置中获取服务地址
 *		value: 服务路径
 */
func InitRpcClientPool(conf *RpcClientPoolConfig) (*RPC, error) {
	if conf.PoolSize == 0 {
		conf.PoolSize = common.DefaultRpcClientPoolSize
	}
	if conf.Protocol == "" {
		conf.Protocol = Tcp
	}
	//从何处获取服务地址
	var addrFrom ServiceAddrFrom
	if conf.NoApolloConfigCenter {
		addrFrom = FromInvoker
	} else {
		addrFrom = FromConfigCenter
	}
	if conf.AddrFrom != "" {
		addrFrom = conf.AddrFrom
	}
	conf.AddrFrom = addrFrom

	r := RPC{Pool: &sync.Map{}, Config: conf, lock: &sync.Mutex{}}
	//r := RPC{Pool: map[string]*client.XClientPool{}, Config: conf}
	for k := range conf.ServerInfo {
		var addr string
		var err error
		switch addrFrom {
		case FromInvoker:
			if conf.ServerAddress == nil {
				return nil, errors.New("RpcClientPoolConfig.ServerAddress must be not nil")
			}
			//自定义服务地址
			var ok bool
			if addr, ok = conf.ServerAddress[k]; !ok {
				return nil, errors.New("the address of rpc server " + k + " is not exist")
			}
		case FromConfigCenter:
			//从配置中心ServiceHosts配置中读取服务地址
			addr, err = config.GetString(common.ApolloNamespaceServicehosts, k)
			if err != nil {
				return nil, err
			}
		case FromDiscovery:
			if conf.Discovery == nil {
				return nil, errors.New("RpcClientPoolConfig.Discovery must be not nil")
			}
			//获取目标服务所有地址
			var filter *servicediscovery.ServiceFilterOptions
			if conf.CustomServiceFilter != nil {
				if cFilter, ok := conf.CustomServiceFilter[k]; ok {
					filter = cFilter
					if len(filter.Tags) == 0 {
						filter.Tags = []string{config.GetEnvServiceTag(), common.ConsulRpcServiceTag}
					}
				}
			}
			if filter == nil {
				filter = &servicediscovery.ServiceFilterOptions{Tags: []string{config.GetEnvServiceTag(), common.ConsulRpcServiceTag}}
			}
			//为所有健康的服务创建客户端连接池
			services, err := conf.Discovery.ServiceDiscover(k, filter)
			if err != nil {
				return nil, err
			}
			if len(services) == 0 {
				return nil, errors.New("no rpc service was found")
			}
			keyFmt := "%s_%s_%s"
			for _, s := range services {
				address := fmt.Sprintf("%s:%d", s.Address, s.Port)
				d, _ := client.NewPeer2PeerDiscovery(conf.Protocol.String()+"@"+address, "")
				for _, path := range conf.ServerInfo[k] {
					key := fmt.Sprintf(keyFmt, k, path, address)
					pool := client.NewXClientPool(conf.PoolSize,
						path, client.Failtry, client.RandomSelect, d, client.DefaultOption)
					//r.Pool[key] =
					r.Pool.Store(key, pool)
				}
			}
			return &r, nil
		default:
			return nil, errors.New("unsupported AddrFrom")
		}
		if addr == "" {
			return nil, errors.New("the address of rpc server " + k + " is not exist")
		}
		d, _ := client.NewPeer2PeerDiscovery(conf.Protocol.String()+"@"+addr, "")
		for _, path := range conf.ServerInfo[k] {
			pool := client.NewXClientPool(conf.PoolSize,
				path, client.Failtry, client.RandomSelect, d, client.DefaultOption)
			//r.Pool[k+"_"+path] =
			r.Pool.Store(k+"_"+path, pool)
		}
	}
	return &r, nil
}

//关闭rpc
func (r *RPC) Close() error {
	if r.Pool == nil {
		return nil
	}
	r.Pool.Range(func(key, value interface{}) bool {
		if value == nil {
			return true
		}
		pool := value.(*client.XClientPool)
		pool.Close()
		log.Logger.Info("close rpc client pool, name = ", key)
		return true
	})
	return nil
}

func (r *RPC) UnInit() {
	_ = r.Close()
	r.Pool = &sync.Map{}
}

/**
 * 获取远程rpc接口对象
 * 输入：
 *     serviceName:服务名称
 *     servicePath:服务路径
 *     opts:过滤选项，只取第一个
 */
func (r *RPC) GetRemoteClient(serviceName, servicePath string, opts ...*servicediscovery.ServiceFilterOptions) (*Client, error) {
	k := serviceName + "_" + servicePath
	var addr string
	if r.Config.AddrFrom == FromDiscovery {
		address, err := r.discoveryOne(serviceName, opts...)
		if err != nil {
			return nil, err
		}
		addr = address
		log.Logger.WithField("ServiceName", serviceName).Info("select one, address = ", addr)
		k += "_" + addr
	}
	var pool *client.XClientPool
	if v, ok := r.Pool.Load(k); !ok {
		//return nil, errors.New(fmt.Sprintf("Uninitialized rpc client pool of %s %s", serviceName, servicePath))
		if r.Config.AddrFrom == FromInvoker {
			return nil, errors.New(fmt.Sprintf("uninitialized rpc client pool of %s %s", serviceName, servicePath))
		}
		//创建连接池
		p, err := r.CreateAndReturnClientPool(serviceName, servicePath, addr, opts...)
		if err != nil {
			return nil, err
		}
		pool = p
	} else {
		pool = v.(*client.XClientPool)
	}
	c := pool.Get()
	if log.TracerIsEnabled() {
		//p := &client.OpenTracingPlugin{}
		//pc := client.NewPluginContainer()
		//pc.Add(p)
		//c.SetPlugins(pc)
	}
	return &Client{client: c}, nil
}

//根据服务名称及过滤条件发现一个服务地址
func (r *RPC) discoveryOne(serviceName string, opts ...*servicediscovery.ServiceFilterOptions) (string, error) {
	if r.Config.AddrFrom != FromDiscovery {
		return "", nil
	}
	var filter *servicediscovery.ServiceFilterOptions
	if len(opts) == 0 {
		filter = &servicediscovery.ServiceFilterOptions{
			Tags:          []string{config.GetEnvServiceTag(), common.ConsulRpcServiceTag},
			UseSelectMode: true, SelectMode: servicediscovery.RandomSelect}
	} else {
		filter = opts[0]
	}
	if len(filter.Tags) == 0 {
		filter.Tags = []string{config.GetEnvServiceTag(), common.ConsulRpcServiceTag}
	}
	services, err := r.Config.Discovery.ServiceDiscover(serviceName, filter)
	if err != nil {
		return "", err
	}
	if len(services) == 0 {
		return "", errors.New("no rpc service was found")
	}
	return fmt.Sprintf("%s:%d", services[0].AddressToIpv6(), services[0].Port), nil
}

//根据服务名称、服务路径、服务地址创建客户端连接池
//如果AddrFrom==FromInvoker,则服务地址不能为空
func (r *RPC) CreateClientPool(serviceName, servicePath, serviceAddress string, opts ...*servicediscovery.ServiceFilterOptions) error {
	_, err := createClientPool(r, serviceName, servicePath, serviceAddress, opts...)
	return err
}

//根据服务名称、服务路径、服务地址创建客户端连接池
//如果AddrFrom==FromInvoker,则服务地址不能为空
func (r *RPC) CreateAndReturnClientPool(serviceName, servicePath, serviceAddress string, opts ...*servicediscovery.ServiceFilterOptions) (*client.XClientPool, error) {
	return createClientPool(r, serviceName, servicePath, serviceAddress, opts...)
}

//创建连接池
func createClientPool(r *RPC, serviceName, servicePath, address string, opts ...*servicediscovery.ServiceFilterOptions) (*client.XClientPool, error) {
	key := serviceName + "_" + servicePath
	if r.Config.AddrFrom == FromDiscovery {
		if address == "" {
			addr, err := r.discoveryOne(serviceName, opts...)
			if err != nil {
				log.Logger.WithField("ServiceName", serviceName).Error("failed to discoveryOne, err: ", err)
				return nil, err
			}
			address = addr
		}
		key += "_" + address
	}
	r.lock.Lock()
	defer r.lock.Unlock()
	if v, ok := r.Pool.Load(key); ok {
		return v.(*client.XClientPool), nil
	}
	log.Logger.WithField("ServiceName", serviceName).Info("createClientPool")
	var addr string
	var err error
	switch r.Config.AddrFrom {
	case FromDiscovery, FromInvoker:
		if address == "" {
			err := errors.New("address cannot be empty, because AddrFrom is " + string(r.Config.AddrFrom))
			return nil, err
		}
		addr = address
	case FromConfigCenter:
		if address == "" {
			addr, err = config.GetString(common.ApolloNamespaceServicehosts, serviceName)
			if err != nil {
				return nil, err
			}
		} else {
			addr = address
		}
	default:
		return nil, errors.New("unsupported AddrFrom")
	}
	d, _ := client.NewPeer2PeerDiscovery(r.Config.Protocol.String()+"@"+addr, "")
	pool := client.NewXClientPool(r.Config.PoolSize,
		servicePath, client.Failtry, client.RandomSelect, d, client.DefaultOption)
	r.Pool.Store(key, pool)
	return pool, nil
}

//同步请求rpc服务端,不支持jaeger
//输入：
//	method: rpc服务端函数
//	request: 客户端请求参数
//	response: 服务端响应结果
//	timeout: 请求超时时间
func (rc *Client) SyncRequest(method string, request, response interface{}, timeout time.Duration) error {
	return rc.SyncRequestWithCtx(context.Background(), method, request, response, timeout)
}

//同步请求rpc服务端
//输入：
// ctx: 当前上下文，如果启用了jaeger，可以从JaegerSpan.GetContext()获取，完成追踪链
//	method: rpc服务端函数
//	request: 客户端请求参数
//	response: 服务端响应结果
//	timeout: 请求超时时间
func (rc *Client) SyncRequestWithCtx(ctx context.Context, method string, request, response interface{}, timeout time.Duration) error {
	ctx, _ = context.WithTimeout(ctx, timeout)
	return rc.client.Call(ctx, method, request, response)
}

//异步请求rpc服务端,不支持jaeger
//输入：
//	method: rpc服务端函数
//	request: 客户端请求参数
//	response: 服务端响应结果
//	timeout: 请求超时时间
func (rc *Client) AsyncRequest(method string, request, response interface{}, timeout time.Duration) error {
	return rc.AsyncRequestWithCtx(context.Background(), method, request, response, timeout)
}

//异步请求rpc服务端
//输入：
// ctx: 当前上下文，如果启用了jaeger，可以从JaegerSpan.GetContext()获取，完成追踪链
//	method: rpc服务端函数
//	request: 客户端请求参数
//	response: 服务端响应结果
//	timeout: 请求超时时间
func (rc *Client) AsyncRequestWithCtx(ctx context.Context, method string, request, response interface{}, timeout time.Duration) error {
	ctx, _ = context.WithTimeout(ctx, timeout)
	call, err := rc.client.Go(ctx, method, request, response, nil)
	if err != nil {
		//log.Logger.Info("failed to async request: %v \n", err)
		return err
	}
	select {
	case <-time.After(timeout):
		return errors.New("context deadline exceeded")
	case resCall := <-call.Done:
		return resCall.Error
	}
}

func (rc *Client) Close() error {
	return rc.client.Close()
}

//创建自定义rpc客户端
func NewCustomClient(clientConfig *RpcClientConfig) (*Client, error) {
	if clientConfig.ServiceName == "" {
		return nil, errors.New("RpcClientConfig.ServiceName must be not empty")
	}
	if clientConfig.ServerPath == "" {
		return nil, errors.New("RpcClientConfig.ServerPath must be not empty")
	}
	var serviceAddr string
	switch clientConfig.AddrFrom {
	case FromInvoker:
		if clientConfig.ServerAddress == "" {
			return nil, errors.New("RpcClientConfig.ServerAddress must be not empty")
		}
		serviceAddr = clientConfig.ServerAddress
	case FromDiscovery:
		if clientConfig.Discovery == nil {
			return nil,
				errors.New("when RpcClientConfig.AddrFrom = FromDiscovery, RpcClientConfig.Discovery must not be nil")
		}
		filter := clientConfig.ServiceFilter
		if filter == nil {
			filter = &servicediscovery.ServiceFilterOptions{
				Tags:          []string{config.GetEnvServiceTag(), common.ConsulRpcServiceTag},
				UseSelectMode: true,
				SelectMode:    servicediscovery.RandomSelect,
			}
		}
		addr, err := GetRpcServerAddrByDiscovery(clientConfig.ServiceName, clientConfig.Discovery, filter)
		if err != nil {
			return nil, err
		}
		serviceAddr = addr
	default:
		addr, err := config.GetString(common.ApolloNamespaceServicehosts, clientConfig.ServiceName)
		if err != nil {
			return nil, err
		}
		serviceAddr = addr
	}
	if clientConfig.Protocol == "" {
		clientConfig.Protocol = Tcp
	}
	d, _ := client.NewPeer2PeerDiscovery(fmt.Sprintf("%s@%s", clientConfig.Protocol, serviceAddr), "")
	xclient := client.NewXClient(clientConfig.ServerPath, client.Failtry, client.RandomSelect, d, client.DefaultOption)
	return &Client{client: xclient}, nil
}

//服务发现rpc服务地址
func GetRpcServerAddrByDiscovery(serviceName string, discovery servicediscovery.ServiceDiscovery, filter *servicediscovery.ServiceFilterOptions) (string, error) {
	services, err := discovery.ServiceDiscover(serviceName, filter)
	if err != nil {
		return "", err
	}
	serviceCount := len(services)
	if serviceCount == 0 {
		return "", errors.New("no found service in discovery")
	}
	s := services[0]
	//if serviceCount == 1 {
	//	return fmt.Sprintf("%s:%d", services[0].Address, services[0].Port), nil
	//}
	//rand.Seed(time.Now().UnixNano())
	//i := rand.Intn(serviceCount)
	//s := services[i]
	return fmt.Sprintf("%s:%d", s.Address, s.Port), nil
}
