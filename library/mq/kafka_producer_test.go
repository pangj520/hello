package mq

import (
	"fmt"
	"strings"
	"testing"
	"library_Test/library/log"
)

var (
	brokers = "192.168.37.60:9092,192.168.37.61:9092,192.168.37.62:9092"
	version = "2.4.0"
	topic   = "Jason"
	key     = ""
)

func TestAsyncProducer(t *testing.T) {
	_ = log.InitLog(log.CreateDefaultLoggerConfig())
	conf, err := CreateKafkaConfig(strings.Split(brokers, ","), version)
	if err != nil {
		panic(err)
	}
	conf.SaramaConfig.Producer.Return.Successes = true
	p, err := CreateAsyncProducer(conf)
	if err != nil {
		panic(err)
	}
	//key = "abc"
	for i := 0; i < 120; i++ {
		p.SendMessage(topic, key, fmt.Sprintf("msg_%d", i))
		fmt.Println("发送成功")
		//time.Sleep(time.Second)
	}
	p.Close()
}

func TestSyncProducer(t *testing.T) {
	conf, err := CreateKafkaConfig(strings.Split("192.168.36.140:9092,192.168.36.141:9092", ","), "2.0.0")
	if err != nil {
		panic(err)
	}
	conf.SaramaConfig.Producer.Return.Successes = true
	p, err := CreateSyncProducer(conf)
	if err != nil {
		panic(err)
	}
	defer p.Close()

	for i := 0; i < 100; i++ {
		_, _, err := p.SendMessage(topic, key, fmt.Sprintf("msg_%d", i))
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println("发送成功")
	}
}
