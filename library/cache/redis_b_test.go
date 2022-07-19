package cache

import (
	"fmt"
	"gotest.tools/assert"
	"sync"
	"testing"
	"time"
	"library_Test/common"
	"library_Test/library/log"
)

var rc *Redis
var once sync.Once
var (
	sessionId = "test"
	userId    = "28"
)

func BenchmarkRedis(b *testing.B) {
	b.N = 10000
	do()
	wg := sync.WaitGroup{}
	sig := make(chan bool)
	fmt.Println(b.N)
	for i := 0; i < b.N; i++ { // b.N，测试循环次数
		wg.Add(1)
		go query3(b, rc, sig, &wg)
		//go get(b, rc, sig, &wg)
	}
	s := time.Now()
	b.ResetTimer()
	close(sig)
	wg.Wait()
	w := time.Now().Sub(s)
	fmt.Println(w.Seconds())
	//fmt.Println("done")
}

func get(b *testing.B, c *Redis, sig chan bool, wg *sync.WaitGroup) {
	defer wg.Done()
	<-sig
	v, err := c.Get("test")
	assert.NilError(b, err)
	assert.Equal(b, v, "a")
}

func query3(b *testing.B, c *Redis, sig chan bool, wg *sync.WaitGroup) {
	defer wg.Done()
	<-sig
	//s := time.Now()
	_, err := c.Get("ApiOperationCachePenetration.3gpp-monitoring-event.1a655b7b43b262sL/subscriptions")
	//assert.NilError(b, err)
	_, err = c.Get("ApiCachePenetration.3gpp-monitoring-event")
	//assert.NilError(b, err)
	_, err = c.HGetAll("ApiInfo.30")
	//assert.NilError(b, err)
	_, err = c.SGetMembers("ApiList")
	//assert.NilError(b, err)
	_, err = c.HGetAll("ApiOperation.66")
	//assert.NilError(b, err)
	_, err = c.SGetMembers("ApiOprList.30")
	//assert.NilError(b, err)
	_, err = c.HGetAll("ApiGateway.28")
	//assert.NilError(b, err)
	_, err = c.SGetMembers("InvokerAtGw.28")
	//assert.NilError(b, err)
	_, err = c.HGetAll("ApiInvokerAuthInfo.1a655b951ba6QLiN")
	//assert.NilError(b, err)
	_, err = c.HGetAll("ApiInvokerAuthInfo.1a655b7b43b262sL")
	//assert.NilError(b, err)

	//authapi
	_, err = c.IsExist(common.NEFUserToken + sessionId)
	//assert.NilError(b, err)
	//assert.Equal(b, v4, true)

	_, err = c.HGet(common.NEFUserToken+sessionId, "userId")
	assert.NilError(b, err)
	//assert.Equal(b, v5, "28")
}

func do() {
	once.Do(func() {
		err := log.InitLog(log.CreateDefaultLoggerConfig())
		if err != nil {
			panic(err)
		}
		r, err := Connect(&RedisConfig{
			//Points:                []string{"192.168.36.175:26380"},
			Points:   []string{"192.168.37.57:26379", "192.168.37.58:26379", "192.168.37.59:26379"},
			UserName: "mymaster",
			Password: "dianxin",
			Type:     RedisTypeSentinel,
			PoolSize: 400,
			//MinIdleConns: 100,
		})
		if err != nil {
			panic(err)
		}
		rc = r
	})
}

func _Benchmark_GetSlave(b *testing.B) {
	//423 ns/op
	b.N = 3000000
	do()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rc.GetSlave()
	}
}
