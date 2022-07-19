package consul

import (
	"context"
	"errors"
	"fmt"
	"github.com/hashicorp/consul/api"
	"library_Test/common"
	"library_Test/library/log"
	"library_Test/library/servicediscovery"
	"os"
	"sort"
	"strings"
	"sync"
)

//consul客户端
type ConsulClient struct {
	Address    string
	DataCenter string
	Token      string
	Client     *api.Client
	watch      *sync.Map //key:serviceName[_tag1_tag2_...],value:serviceWatch
}

//consul客户端初始化可选项
type ConsulOptions struct {
	DataCenter string
	Token      string
}

type ConsulOption func(opts *ConsulOptions)

//consul服务注册结构体
type AgentServiceRegistration struct {
	api.AgentServiceRegistration
}

//consul服务发现结构体
type AgentService struct {
	api.AgentService
}

//服务注销结构体
type AgentServiceDeRegistration struct {
	ServiceId string
}

//配置consul数据中心
func DataCenterOption(center string) ConsulOption {
	return func(opts *ConsulOptions) {
		opts.DataCenter = center
	}
}

//配置连接consul客户端/服务端的认证token
func TokenOption(token string) ConsulOption {
	return func(opts *ConsulOptions) {
		opts.Token = token
	}
}

//初始化ConsulClient实例
func NewConsulClient(address string, copts ...ConsulOption) (*ConsulClient, error) {
	address = InitAddr(address)
	fmt.Println("consul addrss:", address)
	opts := ConsulOptions{}
	for _, fn := range copts {
		fn(&opts)
	}
	client := &ConsulClient{Address: address, watch: &sync.Map{}}
	if opts.DataCenter != "" {
		client.DataCenter = opts.DataCenter
	}
	if opts.Token != "" {
		client.Token = opts.Token
	}
	cfg := api.Config{
		Address:    client.Address,
		Datacenter: client.DataCenter,
		Token:      client.Token,
	}
	apiClient, err := api.NewClient(&cfg)
	if err != nil {
		log.Logger.Error(err)
		return nil, err
	}
	client.Client = apiClient
	return client, nil
}

//初始化服务发现地址
func InitAddr(address string) string {
	var host, port string
	if address == "" {
		host = os.Getenv("HOSTIP")
		if host == "" {
			host = common.DefaultConsulAddr
		}
		port = common.DefaultConsulPort
	} else {
		host, port = GetIpAndPort(address)
	}
	if isIpv6(host) {
		host = "[" + host + "]"
	}
	if port == "" {
		address = host
	} else {
		address = host + ":" + port
	}
	return address
}

func isIpv6(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == ':' {
			return true
		}
	}
	return false
}

func GetIpAndPort(address string) (host string, port string) {
	i := strings.LastIndex(address, ":")
	if i == -1 {
		//没有端口？可能使用了80端口
		host = address
		return
	}
	i2 := strings.LastIndex(address, "]")
	if i2 > i {
		//ipv6且端口为80,去掉"["和"]"
		host = address[1 : len(address)-1]
		return
	}
	host = address[:i]
	port = address[i+1:]
	if i2 != -1 {
		//ipv6,去掉"["和"]"
		host = host[1 : len(host)-1]
	}
	return
}

//将Service转换为consul服务注册实体
func TransferToRegistration(s servicediscovery.Service) AgentServiceRegistration {
	return AgentServiceRegistration{
		AgentServiceRegistration: api.AgentServiceRegistration{
			Kind:    api.ServiceKindTypical,
			ID:      s.ID,
			Name:    s.Name,
			Tags:    s.Tags,
			Port:    s.Port,
			Address: s.Address,
			Meta:    s.Meta,
			Check: &api.AgentServiceCheck{
				CheckID:                        s.Check.ID,
				Name:                           s.Check.Name,
				Args:                           s.Check.Args,
				Interval:                       s.Check.Interval,
				Timeout:                        s.Check.Timeout,
				HTTP:                           s.Check.HTTP,
				Header:                         s.Check.Header,
				Method:                         s.Check.Method,
				Body:                           s.Check.Body,
				TCP:                            s.Check.TCP,
				Status:                         s.Check.Status,
				DeregisterCriticalServiceAfter: s.Check.DeregisterCriticalServiceAfter,
			},
		},
	}
}

//将consul AgentService转换为Service
func TransferToService(s *api.AgentService) servicediscovery.Service {
	return servicediscovery.Service{
		ID:      s.ID,
		Name:    s.Service,
		Address: s.Address,
		Port:    s.Port,
		Tags:    s.Tags,
		Meta:    s.Meta,
		Check:   servicediscovery.HealthCheck{},
	}
}

func (c *ConsulClient) formatKey(name string, opts *servicediscovery.ServiceFilterOptions) string {
	var tagStr string
	if len(opts.Tags) > 0 {
		sort.Slice(opts.Tags, func(i, j int) bool {
			return opts.Tags[i] < opts.Tags[j]
		})
		tagStr = strings.Join(opts.Tags, "_")
	}
	key := name
	if tagStr != "" {
		key = fmt.Sprintf("%s_%s", key, tagStr)
	}
	return key
}

//服务注册
//输入:
//	s: 要注册的服务，实际类型为*AgentServiceRegistration
func (c *ConsulClient) RegisterService(s servicediscovery.Service) error {
	registration := TransferToRegistration(s)
	return c.Client.Agent().ServiceRegister(&registration.AgentServiceRegistration)
}

//服务发现
//输入:
//	opts: 服务发现过滤条件
//输出:
//	符合条件的所有服务, 如果启用路由选择，则只返回选中的,如果不符合或异常则返回nil
//	错误
func (c *ConsulClient) ServiceDiscover(name string, opts *servicediscovery.ServiceFilterOptions) ([]servicediscovery.Service, error) {
	if strings.TrimSpace(name) == "" {
		return nil, errors.New("name cannot be empty")
	}
	//从本地获取服务
	services := c.tryGetServicesInLocal(name, opts)
	if len(services) == 0 {
		fmt.Printf("%s:no services were found locally,try using the consul api\n", name)
		//访问consul获取服务信息
		ss, err := c.serviceDiscover(name, opts)
		if err != nil {
			return nil, err
		}
		services = ss
	}
	fmt.Printf("%s:find %d\n", name, len(services))
	//过滤被排除的服务
	if len(opts.ExcludedServices) > 0 {
		services = removeExcluded(services, opts.ExcludedServices)
		fmt.Printf("%s:match %d\n", name, len(services))
	}
	if len(services) == 0 || len(services) == 1 {
		return services, nil
	}
	//是否启用路由
	if !opts.UseSelectMode {
		return services, nil
	}
	fmt.Println(name, "select mode:", opts.SelectMode)
	one := servicediscovery.NewSelector(opts.SelectMode, services).Select(context.Background())
	if one.IsNull() {
		return services, nil
	}
	return []servicediscovery.Service{one}, nil
}

/**
发现并返回所有符合条件服务,仅作ExcludedServices的过滤,不作是否启用路由选择的判断
*/
func (c *ConsulClient) ServiceDiscoverAll(name string, opts *servicediscovery.ServiceFilterOptions) ([]servicediscovery.Service, bool, error) {
	fromConsulAPI := false
	if strings.TrimSpace(name) == "" {
		return nil, fromConsulAPI, errors.New("name cannot be empty")
	}
	//从本地获取服务
	services := c.tryGetServicesInLocal(name, opts)
	if len(services) == 0 {
		fmt.Printf("%s:no services were found locally,try using the consul api", name)
		//访问consul获取服务信息
		fromConsulAPI = true
		ss, err := c.serviceDiscover(name, opts)
		if err != nil {
			return nil, fromConsulAPI, err
		}
		services = ss
	}
	fmt.Printf("%s:find %d\n", name, len(services))

	//过滤被排除的服务
	if len(opts.ExcludedServices) > 0 {
		services = removeExcluded(services, opts.ExcludedServices)
		fmt.Printf("%s:match %d\n", name, len(services))
	}
	return services, fromConsulAPI, nil
}

/**
根据路由选择模式,返回一个service
*/
func (c *ConsulClient) SelectOneService(services []servicediscovery.Service, opts *servicediscovery.ServiceFilterOptions) *servicediscovery.Service {
	if len(services) == 0 {
		return nil
	} else if len(services) == 1 {
		return &services[0]
	}

	one := servicediscovery.NewSelector(opts.SelectMode, services).Select(context.Background())
	if one.IsNull() {
		return &services[0]
	}
	return &one
}

/**
获取服务发现的watch监听对象
*/
func (c *ConsulClient) GetDiscoverChan(name string, opts *servicediscovery.ServiceFilterOptions) chan []servicediscovery.Service {
	key := c.formatKey(name, opts)
	if v, ok := c.watch.Load(key); ok {
		sw := v.(*serviceWatch)
		return sw.changeChan
	}
	return nil
}

//通过api发现服务
func (c *ConsulClient) serviceDiscover(name string, opts *servicediscovery.ServiceFilterOptions) ([]servicediscovery.Service, error) {
	filter := opts
	if filter == nil {
		filter = &servicediscovery.ServiceFilterOptions{}
	}
	tagCount := len(opts.Tags)
	var tag string
	if tagCount == 1 {
		tag = opts.Tags[0]
	}
	log.Logger.WithField("Consul", name).Debugf("%s:try to get services from consul api, tag:%s", name, tag)

	//这里tag为空，是因为api仅支持单个tag
	var services []servicediscovery.Service
	entries, _, err := c.Client.Health().Service(name, tag, true, &api.QueryOptions{})
	if err != nil || len(entries) == 0 {
		return services, err
	}
	log.Logger.WithField("Consul", name).Debugf("consul api return service:%d", len(entries))
	for i, info := range entries {
		services = append(services, TransferToService(info.Service))
		log.Logger.WithField("Consul", name).Debugf("No %d: %s:%d", i, info.Service.Address, info.Service.Port)
	}
	//根据多个标签过滤服务
	log.Logger.WithField("Consul", name).Debugf("filter.Tags len:%d", len(filter.Tags))
	if len(filter.Tags) > 1 {
		services = filterByTags(filter.Tags, services)
	}
	return services, nil
}

//从本地获取服务
func (c *ConsulClient) tryGetServicesInLocal(serviceName string, opts *servicediscovery.ServiceFilterOptions) []servicediscovery.Service {
	log.Logger.WithField("Consul", serviceName).Debugf("%s:try to get services from local", serviceName)

	//判断是否启动服务监听
	key := c.formatKey(serviceName, opts)
	v, ok := c.watch.Load(key)
	if ok {
		//已启动
		sw := v.(*serviceWatch)
		return sw.GetServices()
	}
	//未启动，则创建监听
	sw := NewServiceWatch(c.Address, serviceName, DataCenter(c.DataCenter), Token(c.Token), Tags(opts.Tags))
	c.watch.Store(key, sw)
	go func() {
		err := sw.Do()
		if err != nil {
			fmt.Println(serviceName+" service watch err: ", err)
			c.watch.Delete(key)
		}
	}()
	return nil
}

//服务注销
//输入:
//	s: 要注销的服务，实际类型为*AgentServiceDeRegistration
func (c *ConsulClient) DeregisterService(s servicediscovery.Service) error {
	if s.ID == "" {
		return errors.New("ID must be not empty")
	}
	return c.Client.Agent().ServiceDeregister(s.ID)
}

func (a *AgentServiceRegistration) ToString() string {
	return fmt.Sprintf("%+v", a)
}

func (a *AgentService) ToString() string {
	return fmt.Sprintf("%+v", a)
}

func (a *AgentServiceDeRegistration) ToString() string {
	return fmt.Sprintf("%+v", a)
}

//根据标签过滤服务
func filterByTags(tags []string, services []servicediscovery.Service) []servicediscovery.Service {
	if len(services) == 0 {
		return services
	}
	var fs []servicediscovery.Service
	for _, s := range services {
		if isSubset(s.Tags, tags) {
			fs = append(fs, s)
		}
	}
	return fs
}

//判断dst是否为src的子集
func isSubset(src []string, dst []string) bool {
	m := make(map[string]string)
	for _, t := range src {
		m[t] = t
	}
	for _, t := range dst {
		if _, ok := m[t]; !ok {
			return false
		}
	}
	return true
}

//移除被排除的服务
func removeExcluded(src []servicediscovery.Service, excluded []servicediscovery.Service) []servicediscovery.Service {
	if len(excluded) == 0 || len(src) == 0 {
		return src
	}
	var newSrc []servicediscovery.Service
	for _, s := range src {
		var has bool
		for _, s2 := range excluded {
			if s.Equal(s2) {
				has = true
				break
			}
		}
		if has {
			continue
		}
		newSrc = append(newSrc, s)
	}
	return newSrc
}
