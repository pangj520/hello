package consul

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"library_Test/library/config"
	"testing"
	"time"
	"library_Test/common"
)

func TestNewServiceWatch(t *testing.T) {
	type args struct {
		consulAddr  string
		serviceName string
		opts        []WatchOption
	}
	tests := []struct {
		name string
		args args
		want *serviceWatch
	}{
		{"测试监听", args{
			consulAddr:  consulAddr,
			serviceName: common.ServiceNameSOGW,
			opts:        []WatchOption{DataCenter(datacenter), Token(token), Tags([]string{config.GetEnvServiceTag(), common.ConsulRpcServiceTag})},
		}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewServiceWatch(tt.args.consulAddr, tt.args.serviceName, tt.args.opts...)
			go func() {
				err := got.Do()
				assert.NoError(t, err)
				fmt.Println("watch closed")
			}()
			stopI := 0
			for {
				//在一分钟内手动触发服务注册、服务宕掉等操作
				if stopI == 30 {
					break
				}
				fmt.Printf("%+v\n", got.GetServices())
				time.Sleep(time.Second * 2)
				stopI++
			}
			err := got.Close()
			assert.NoError(t, err)
			time.Sleep(time.Second * 5)
		})
	}
}