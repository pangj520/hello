package cache

import (
	"fmt"
	"github.com/go-redis/redis"
	"gotest.tools/assert"
	"gotest.tools/assert/cmp"
	"sync"
	"testing"
	"time"
	"library_Test/library/log"
)

var c *Redis

var TestRedisKey = "st1"

func TestConnect(t *testing.T) {
	err := log.InitLog(log.CreateDefaultLoggerConfig())
	assert.NilError(t, err)
	r, err := Connect(&RedisConfig{
		Points:   []string{"192.168.37.57:26379", "192.168.37.58:26379", "192.168.37.59:26379"},
		UserName: "mymaster",
		Password: "dianxin",
		Type:     RedisTypeSentinel,
	})
	assert.NilError(t, err)
	c = r
	//info, err := c.Client.Info().Result()
	//assert.NilError(t, err)
	//fmt.Println(info)

	cmd := redis.NewSliceCmd("info")
	_ = c.Client.Process(cmd)
	slaveInfoBlobs, err := cmd.Result()
	assert.NilError(t, err)
	for _, slaveInfoBlob := range slaveInfoBlobs {
		//slaveInfo := reflect.ValueOf(slaveInfoBlob)
		//slaveIP := fmt.Sprintf("%+v", slaveInfo.Index(<some-index>))
		fmt.Println(slaveInfoBlob)
	}
}

func TestConnect2(t *testing.T) {
	err := log.InitLog(log.CreateDefaultLoggerConfig())
	assert.NilError(t, err)
	r, err := Connect(&RedisConfig{
		Points: []string{"192.168.36.175:26380"},
		//Points:                []string{"192.168.37.57:26379","192.168.37.58:26379","192.168.37.59:26379"},
		UserName: "mymaster",
		Password: "dianxin",
		Type:     RedisTypeSentinel,
	})
	assert.NilError(t, err)
	c = r

	var k, v = "a", "b"
	err = c.Set(k, v)
	assert.NilError(t, err)
	s, err := c.Get(k)
	assert.NilError(t, err)
	assert.Equal(t, s, v)
	//cmd := redis.NewSliceCmd("SENTINEL", "slaves", "mymaster")
	//_ = c.Client.Process(cmd)
	//slaveInfoBlobs, err := cmd.Result()
	//assert.NilError(t, err)
	//for _, slaveInfoBlob := range slaveInfoBlobs {
	//	infos := slaveInfoBlob.([]interface{})
	//	var ip, port string
	//	for i, info := range infos{
	//		s := info.(string)
	//		if s == "ip"{
	//			ip = infos[i+1].(string)
	//		}else if s == "port"{
	//			port = infos[i+1].(string)
	//		}
	//	}
	//	fmt.Println(ip, ":", port)
	//}
}

func TestRedisLock_Lock(t *testing.T) {
	TestConnect(t)
	data := 50
	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			for {
				lock := c.GetLock("abc", time.Second*1)
				lock.Lock()
				if data == 0 {
					break
				}
				data--
				fmt.Println(fmt.Sprintf("t%d: %d", index, data))
				lock.UnLock()
			}
		}(i)
	}
	wg.Wait()
	fmt.Println(data)
}

func TestRedis_Del(t *testing.T) {
	_, err := c.Del(TestRedisKey)
	assert.NilError(t, err)
}

func TestRedis_SAddMembers(t *testing.T) {
	TestConnect(t)
	err := c.SAddMembers(TestRedisKey, "", "nil", "null")
	assert.NilError(t, err)
}

func SInitAndClean(t *testing.T, f func()) {
	TestRedis_SAddMembers(t)
	defer TestRedis_Del(t)
	if f == nil {
		return
	}
	f()
}

//redis set测试

func TestRedis_SWithoutKey(t *testing.T) {
	TestConnect(t)
	count, err := c.SGetCount("")
	assert.Error(t, err, "the key must be not empty")
	assert.Equal(t, count, int64(0))
}

func TestRedis_SAddMembersAndDelKey(t *testing.T) {
	SInitAndClean(t, func() {
		err := c.SAddMembers(TestRedisKey, nil)
		assert.Error(t, err, "all members could be nil")
	})
}

func TestRedis_SGetCount(t *testing.T) {
	SInitAndClean(t, func() {
		count, err := c.SGetCount(TestRedisKey)
		assert.NilError(t, err)
		assert.Equal(t, count, int64(3))
	})
}

func TestRedis_SGetMembers(t *testing.T) {
	SInitAndClean(t, func() {
		members, err := c.SGetMembers(TestRedisKey)
		assert.NilError(t, err)
		assert.Equal(t, len(members), 3)
		cmp.Contains(members, "")
		cmp.Contains(members, "nil")
		cmp.Contains(members, "null")
	})
}

func TestRedis_SIsExist(t *testing.T) {
	SInitAndClean(t, func() {
		exist, err := c.SIsExist(TestRedisKey, "")
		assert.NilError(t, err)
		assert.Equal(t, exist, true)

		exist, err = c.SIsExist(TestRedisKey, "nil")
		assert.NilError(t, err)
		assert.Equal(t, exist, true)

		exist, err = c.SIsExist(TestRedisKey, "null")
		assert.NilError(t, err)
		assert.Equal(t, exist, true)
	})
}

func TestRedis_SPopOne(t *testing.T) {
	SInitAndClean(t, func() {
		for i := 0; i < 3; i++ {
			m, err := c.SPopOne(TestRedisKey)
			assert.NilError(t, err)
			fmt.Println(m)
		}
		//key不存在
		m, err := c.SPopOne(TestRedisKey)
		assert.Error(t, err, redis.Nil.Error())
		fmt.Println(m)
	})
}

func TestRedis_SRandomGetNMembers(t *testing.T) {
	SInitAndClean(t, func() {
		ms, err := c.SRandomGetNMembers(TestRedisKey, 2)
		assert.NilError(t, err)
		assert.Equal(t, len(ms), 2)

		//set成员数量小于count，返回所有成员
		ms, err = c.SRandomGetNMembers(TestRedisKey, 4)
		assert.NilError(t, err)
		fmt.Println(len(ms))

		//key不存在
		ms, err = c.SRandomGetNMembers("sdfjfdk", 4)
		assert.NilError(t, err)
		//ms=[]string{}
		fmt.Println(len(ms))
	})
}

func TestRedis_SRemove(t *testing.T) {
	SInitAndClean(t, func() {
		err := c.SRemove(TestRedisKey, "")
		assert.NilError(t, err)

		err = c.SRemove(TestRedisKey, "", "nil")
		assert.NilError(t, err)

		err = c.SRemove(TestRedisKey, "sjdflkdj")
		assert.NilError(t, err)

		//key不存在
		err = c.SRemove("laskdjflsjdf", "")
		assert.NilError(t, err)
	})
}

func TestRedis_SInter(t *testing.T) {
	SInitAndClean(t, func() {
		otherKey := "st2"
		err := c.SAddMembers(otherKey, "nil", "aaa")
		assert.NilError(t, err)

		ms, err := c.SInter(TestRedisKey, otherKey)
		assert.NilError(t, err)
		assert.Equal(t, len(ms), 1)
		assert.Check(t, cmp.Contains(ms, "nil"))
		fmt.Println(ms)

		//otherKey不存在
		ms, err = c.SInter(TestRedisKey, "aldkjljsd")
		assert.NilError(t, err)
		fmt.Println(ms)

		_, _ = c.Del(otherKey)
	})
}

func TestRedis_SDiff(t *testing.T) {
	SInitAndClean(t, func() {
		otherKey := "st2"
		err := c.SAddMembers(otherKey, "nil", "aaa", "bbbbb")
		assert.NilError(t, err)

		otherKey2 := "st3"
		err = c.SAddMembers(otherKey2, "bbbbb", "asdfgaa")
		assert.NilError(t, err)

		//otherKey与setKey的差集["aaa", "bbbbb"], ["aaa", "bbbbb"]与otherKey2的差集["aaa"]
		ms, err := c.SDiff(otherKey, TestRedisKey, otherKey2)
		assert.NilError(t, err)
		assert.Equal(t, len(ms), 1)
		assert.Check(t, cmp.Contains(ms, "aaa"))
		fmt.Println(ms)

		//有一个key不存在
		ms, err = c.SInter(TestRedisKey, "aldkjljsd")
		assert.NilError(t, err)
		//ms = []
		fmt.Println(ms)

		//有一个key不存在
		ms, err = c.SInter(TestRedisKey, "aldkjljsd", otherKey)
		assert.NilError(t, err)
		//ms = []
		fmt.Println(ms)

		_, _ = c.Del(otherKey)
		_, _ = c.Del(otherKey2)
	})
}

func TestRedis_SUnion(t *testing.T) {
	SInitAndClean(t, func() {
		otherKey := "st2"
		err := c.SAddMembers(otherKey, "nil", "aaa", "bbbbb")
		assert.NilError(t, err)

		defer func() {
			_, _ = c.Del(otherKey)
		}()

		ms, err := c.SUnion(TestRedisKey, otherKey)
		assert.NilError(t, err)
		assert.Equal(t, len(ms), 5)
		fmt.Println(ms)

		//key不存在
		ms, err = c.SUnion(TestRedisKey, "askhfshf")
		assert.NilError(t, err)
		//m = setKey的所有成员
		fmt.Println(ms)
	})
}

func TestRedis_LInsert(t *testing.T) {
	TestConnect(t)
	err := c.LInsert(ListLeft, TestRedisKey, "", "nil", "null")
	assert.NilError(t, err)
}

func LInitAndClean(t *testing.T, f func()) {
	TestRedisKey = "lt1"
	TestRedis_LInsert(t)
	defer TestRedis_Del(t)
	if f == nil {
		return
	}
	f()
}

func TestRedis_LLength(t *testing.T) {
	LInitAndClean(t, func() {
		l, err := c.LLength(TestRedisKey)
		assert.NilError(t, err)
		assert.Equal(t, l, int64(3))
	})

	//key不存在
	l, err := c.LLength("KJHKHKHKH")
	assert.NilError(t, err)
	fmt.Println(l)
}

func TestRedis_LInsertNotAllowNil(t *testing.T) {
	LInitAndClean(t, func() {
		err := c.LInsertNotAllowNil(ListLeft, TestRedisKey, nil)
		assert.Error(t, err, "all values could be nil")
	})
}

func TestRedis_LGetByIndex(t *testing.T) {
	LInitAndClean(t, func() {
		v, err := c.LGetByIndex(TestRedisKey, 1)
		assert.NilError(t, err)
		assert.Equal(t, v, "nil")

		//索引为负
		v, err = c.LGetByIndex(TestRedisKey, -1)
		assert.NilError(t, err)
		assert.Equal(t, v, "")

		//索引越界
		v, err = c.LGetByIndex(TestRedisKey, 19)
		//返回redis.Nil错误
		assert.Error(t, err, redis.Nil.Error())
	})
}

func TestRedis_LGetAndDel(t *testing.T) {
	LInitAndClean(t, func() {
		v, err := c.LGetAndDel(ListLeft, TestRedisKey)
		assert.NilError(t, err)
		assert.Equal(t, v, "null")

		v, err = c.LGetAndDel(ListLeft, TestRedisKey)
		assert.NilError(t, err)
		assert.Equal(t, v, "nil")

		v, err = c.LGetAndDel(ListLeft, TestRedisKey)
		assert.NilError(t, err)
		assert.Equal(t, v, "")

		//key不存在,返回redis.Nil错误
		v, err = c.LGetAndDel(ListLeft, TestRedisKey)
		assert.Error(t, err, redis.Nil.Error())
	})
}

func TestRedis_LGetAndDelUntilTimeout(t *testing.T) {
	LInitAndClean(t, func() {
		k2 := "lt2"
		_ = c.LInsertLeft(k2, "a", "b", "c")
		defer func() {
			_, _ = c.Del(k2)
		}()
		for {
			//key不存在，按顺序操作其他key
			vs, err := c.LGetAndDelUntilTimeout(ListLeft, time.Second*3, "adh", TestRedisKey, k2)
			if err != nil && err.Error() == redis.Nil.Error() {
				//等待超时
				break
			}
			fmt.Println(vs)
		}
	})
}

func TestRedis_LUpdateByIndex(t *testing.T) {
	LInitAndClean(t, func() {
		newValue := "空字符串修改为中文"
		err := c.LUpdateByIndex(TestRedisKey, 2, newValue)
		assert.NilError(t, err)

		v, err := c.LGetByIndex(TestRedisKey, 2)
		assert.NilError(t, err)
		assert.Equal(t, v, newValue)

		//索引不存在,索引越界错误
		err = c.LUpdateByIndex(TestRedisKey, 5, newValue)
		assert.Error(t, err, "ERR index out of range")
	})
}

func TestRedis_LDel(t *testing.T) {
	LInitAndClean(t, func() {
		err := c.LDel(TestRedisKey, 1, "")
		assert.NilError(t, err)

		//移除不存在的value
		err = c.LDel(TestRedisKey, 0, "")
		assert.NilError(t, err)
	})
}

func TestRedis_SwitchMaster(t *testing.T) {
	err := log.InitLog(log.CreateDefaultLoggerConfig())
	assert.NilError(t, err)
	r, err := Connect(&RedisConfig{
		//Points:                []string{"192.168.36.175:26380"},
		//Points:                []string{"172.20.101.15:36380"},
		Points:   []string{"192.168.37.57:26379", "192.168.37.58:26379", "192.168.37.59:26379"},
		UserName: "mymaster",
		Password: "dianxin",
		Type:     RedisTypeSentinel,
	})
	assert.NilError(t, err)
	c = r
	var k, v = "a", "b"
	err = c.Set(k, v)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for {
			assert.NilError(t, err)
			s, err := c.Get(k)
			if err != nil {
				fmt.Println(err)
			} else {
				assert.Equal(t, s, v)
			}
			time.Sleep(time.Second)
		}
	}()
	wg.Wait()
}