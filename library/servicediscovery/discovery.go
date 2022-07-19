package servicediscovery

import (
	"library_Test/library/utils"
)

//服务发现解决方案必须实现该接口
type ServiceDiscovery interface {
	//服务变更通道
	GetDiscoverChan(name string, opts *ServiceFilterOptions) chan []Service
	//服务注册
	RegisterService(s Service) error
	//服务发现
	ServiceDiscover(name string, opts *ServiceFilterOptions) ([]Service, error)
	//服务发现All
	ServiceDiscoverAll(name string, opts *ServiceFilterOptions) ([]Service, bool, error)
	//根据路由模式选择一个服务
	SelectOneService(services []Service, opts *ServiceFilterOptions) *Service
	//服务注销
	DeregisterService(s Service) error
}

//服务
type Service struct {
	ID      string
	Name    string
	Address string
	Port    int
	Tags    []string
	Meta    map[string]string
	Check   HealthCheck
}

func (s Service) IsNull() bool {
	return s.ID == ""
}

//如果服务地址为ipv6，则加上"[]"，否则返回原地址
func (s Service) AddressToIpv6() string {
	return utils.MaybeIpv6(s.Address)
}

//判断两个服务是否一致，符合以下情况则认为两个服务相同
//	ID一致
//	ID为空，Name、Address、Port一致
//	ID、Name为空，Address、Port一致
func (s Service) Equal(s2 Service) bool {
	if s2.ID != "" {
		return s.ID == s2.ID
	}
	if s2.Name != "" {
		if s2.Name != s.Name {
			return false
		}
	}
	if s2.Address != "" && s2.Port != 0 {
		return s2.Address == s.Address && s2.Port == s.Port
	}
	return false
}

type HealthCheck struct {
	ID       string
	Name     string
	Args     []string
	Interval string
	Timeout  string
	HTTP     string
	Header   map[string][]string
	Method   string
	Body     string
	TCP      string
	Status   string
	//当consul健康检查到服务异常后，会在DeregisterCriticalServiceAfter后注销服务
	DeregisterCriticalServiceAfter string
}

//服务过滤参数可选项
type ServiceFilterOptions struct {
	Tags             []string   `json:"Tags"`             //根据标签过滤
	UseSelectMode    bool       `json:"UseSelectMode"`    //启用路由选择
	SelectMode       SelectMode `json:"SelectMode"`       //路由选择器
	ExcludedServices []Service  `json:"ExcludedServices"` //被排除的服务，服务发现结果不包括这些服务
}
