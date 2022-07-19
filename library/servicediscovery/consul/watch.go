package consul

import (
	"errors"
	"fmt"
	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/api/watch"
	"sync"
	"library_Test/library/servicediscovery"
)

//监听指定服务的变更
type serviceWatch struct {
	lock        *sync.RWMutex
	consulAddr  string
	serviceName string
	tags        []string
	dc          string
	token       string
	services    []servicediscovery.Service
	plan        *watch.Plan
	changeChan  chan []servicediscovery.Service //服务发现变更通知通道
}

func NewServiceWatch(consulAddr, serviceName string, opts ...WatchOption) *serviceWatch {
	wOpts := &WatchOptions{}
	for _, opt := range opts {
		opt(wOpts)
	}
	return &serviceWatch{lock: &sync.RWMutex{},
		consulAddr: consulAddr, serviceName: serviceName,
		tags: wOpts.Tags, dc: wOpts.DataCenter, token: wOpts.Token}
}

//获取健康的服务
func (sw *serviceWatch) GetServices() []servicediscovery.Service {
	sw.lock.RLock()
	defer sw.lock.RUnlock()
	return sw.services
}

//更新
func (sw *serviceWatch) UpdateServices(s []servicediscovery.Service) {
	sw.lock.Lock()
	defer sw.lock.Unlock()
	sw.services = s
}

//关闭监听
func (sw *serviceWatch) Close() error {
	if sw.plan.IsStopped() {
		return nil
	}
	sw.plan.Stop()
	return nil
}

//检查监听是否已经关闭
func (sw *serviceWatch) IsClosed() bool {
	return sw.plan.IsStopped()
}

//创建监听计划
func (sw *serviceWatch) createPlan() error {
	//初始化变更通道
	if sw.changeChan == nil {
		sw.changeChan = make(chan []servicediscovery.Service)
	}

	//创建consul监听计划
	params := map[string]interface{}{
		"type":        "service",
		"service":     sw.serviceName,
		"passingonly": true, //只监听健康的服务
	}
	if len(sw.tags) != 0 {
		params["tag"] = sw.tags
	}
	if sw.dc != "" {
		params["datacenter"] = sw.dc
	}
	if sw.dc != "" {
		params["token"] = sw.token
	}
	plan, err := watch.Parse(params)
	if err != nil {
		return err
	}
	plan.Handler = sw.Handler
	sw.plan = plan
	return nil
}

//处理变更
func (sw *serviceWatch) Handler(idx uint64, raw interface{}) {
	if raw == nil {
		return // ignore
	}
	v, ok := raw.([]*api.ServiceEntry)
	if !ok {
		return // ignore
	}

	var services []servicediscovery.Service
	for _, s := range v {
		services = append(services, TransferToService(s.Service))
	}
	sw.UpdateServices(services)

	fmt.Println(sw.serviceName + " is change")
	sw.changeChan <- services
}

//开始监听
func (sw *serviceWatch) Do() error {
	if sw.consulAddr == "" {
		return errors.New("consulAddr must be not empty")
	}
	err := sw.createPlan()
	if err != nil {
		return err
	}
	err = sw.plan.Run(sw.consulAddr)
	if err != nil {
		return err
	}
	return nil
}

type WatchOptions struct {
	DataCenter string
	Tags       []string
	Token      string
}

type WatchOption func(opts *WatchOptions)

func DataCenter(dc string) WatchOption {
	return func(opts *WatchOptions) {
		opts.DataCenter = dc
	}
}

func Tags(tags []string) WatchOption {
	return func(opts *WatchOptions) {
		opts.Tags = tags
	}
}

func Token(token string) WatchOption {
	return func(opts *WatchOptions) {
		opts.Token = token
	}
}
