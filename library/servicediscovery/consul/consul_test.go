package consul

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/api/watch"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"net"
	"sync"
	"library_Test/common"
	"library_Test/library/config"
	"library_Test/library/servicediscovery"
	"testing"
	"time"
)

var (
	consulAddr = "http://172.20.101.15:35926"
	datacenter = "guangzhou_dc1"
	token      = "db02cbec-90d7-d525-626e-c70c6e5542cf"
)

func TestConsulClient_RegisterService(t *testing.T) {
	c, err := NewConsulClient(consulAddr, DataCenterOption(datacenter), TokenOption(token))
	assert.NoError(t, err)
	addr := "127.0.0.1"
	port := 8081
	rand.Seed(time.Now().UnixNano())
	num := rand.Int()
	err = c.RegisterService(servicediscovery.Service{
		ID:      fmt.Sprintf("web_%d", num),
		Name:    "web",
		Tags:    []string{"web-tag"},
		Port:    port,
		Address: addr,
		Meta: map[string]string{
			"weight": "1",
		},
		Check: servicediscovery.HealthCheck{
			ID:                             "web-check",
			Name:                           "web-check",
			Interval:                       "10s",
			Timeout:                        "1s",
			HTTP:                           fmt.Sprintf("http://%s:%d", addr, port),
			Method:                         "GET",
			Status:                         "passing",
			DeregisterCriticalServiceAfter: "30s",
		},
	})
	assert.NoError(t, err)
}

func TestConsulClient_ServiceDiscover(t *testing.T) {
	c, err := NewConsulClient(consulAddr, DataCenterOption(datacenter), TokenOption(token))
	assert.NoError(t, err)
	for {
		services, err := c.ServiceDiscover(common.ServiceNameSOGW, &servicediscovery.ServiceFilterOptions{
			Tags: []string{common.ConsulRpcServiceTag, config.GetEnvServiceTag()},
		})
		assert.NoError(t, err)
		for _, s := range services {
			fmt.Printf("%+v\n", s)
		}
		time.Sleep(time.Second)
	}
}

func TestConsulClient_ServiceWatch(t *testing.T) {
	//创建consul kv监听计划
	plan, err := watch.Parse(map[string]interface{}{
		"type":       "service",
		"service":    "web",
		"datacenter": datacenter,
		"token":      token,
		//"passingonly": true,
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	var (
		notifyCh = make(chan struct{})
	)

	plan.Handler = func(idx uint64, raw interface{}) {
		if raw == nil {
			return // ignore
		}
		v, ok := raw.([]*api.ServiceEntry)
		if !ok {
			return // ignore
		}

		ss, err := json.Marshal(v)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println("Service changed: ", string(ss))
		notifyCh <- struct{}{}
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := plan.Run(consulAddr); err != nil {
			fmt.Printf("consul config err: %v", err)
		}
	}()

	for {
		<-notifyCh
		{
			fmt.Println("notifyCh")
		}
	}

	defer plan.Stop()
	wg.Wait()
}

func TestParseIpv6(t *testing.T) {
	ip := net.ParseIP("240e:180:260:40::13")
	ip2 := net.ParseIP("[240e:180:260:40::13]")
	ip3 := net.ParseIP("localhost")
	if ip == nil {
		fmt.Println("invalid ip")
	} else {
		fmt.Println(ip.String())
	}
	if ip2 == nil {
		fmt.Println("invalid ip2")
	} else {
		fmt.Println(ip2.String())
	}
	if ip3 == nil {
		fmt.Println("invalid ip3")
	} else {
		fmt.Println(ip3.String())
	}
}

func TestInitAddr(t *testing.T) {
	type args struct {
		address string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{"测试给定ipv4地址", args{address: "192.168.36.175:8080"},
			"192.168.36.175:8080"},
		{"测试给定ipv4地址(不带端口)", args{address: "192.168.36.175"},
			"192.168.36.175"},
		{"测试不给定地址", args{}, "localhost:8500"},
		{"测试给定ipv6地址(不带[]带端口)", args{address: "240e:180:260:40::13:8080"},
			"[240e:180:260:40::13]:8080"},
		{"测试给定ipv6地址(不带[]不带端口)", args{address: "240e:180:260:40::13"},
			"[240e:180:260:40:]:13"},
		{"测试给定ipv6地址(带[]带端口)", args{address: "[240e:180:260:40::13]:8080"},
			"[240e:180:260:40::13]:8080"},
		{"测试给定ipv6地址(带[]不带端口)", args{address: "[240e:180:260:40::13]"},
			"[240e:180:260:40::13]"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := InitAddr(tt.args.address); got != tt.want {
				t.Errorf("InitAddr() = %v, want %v", got, tt.want)
			} else {
				fmt.Println(got)
			}
		})
	}
}