package mq

import (
	"fmt"
	"github.com/Shopify/sarama"
	"strings"
	"testing"
	"time"
	"library_Test/library/log"
)

func InitDomain() {

}

func TestConsumer(t *testing.T) {
	f := log.CreateDefaultLoggerConfig()
	//f.Disable = "true"
	f.DisableReportCaller = true
	_ = log.InitLog(f)
	conf, err := CreateKafkaConfig(strings.Split(brokers, ","), version)
	if err != nil {
		panic(err)
	}
	//conf.SaramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	groupConfig := &ConsumerGroupConfig{Topics: []string{topic}, Group: "Jason", KafkaConfig: conf}
	c, err := CreateConsumerGroupClient(groupConfig)
	if err != nil {
		panic(err)
	}
	c.Start(func(message *sarama.ConsumerMessage) {
		fmt.Println("consumer1: ", string(message.Value), ", partition = ", message.Partition)
		//time.Sleep(time.Second)
	})

	//并发消费
	c2, err := CreateConsumerGroupClient(groupConfig)
	if err != nil {
		panic(err)
	}
	c2.Start(func(message *sarama.ConsumerMessage) {
		fmt.Println("consumer2: ", string(message.Value), ", partition = ", message.Partition)
		//time.Sleep(time.Second)
	})

	time.Sleep(time.Second * 20)
	c.Close()
	c2.Close()
	//err = c.Reconnect()
	//if err != nil {
	//	panic(err)
	//}
	//go c.Start(func(message *sarama.ConsumerMessage) {
	//	fmt.Println("consumer2: ", string(message.Value))
	//	time.Sleep(time.Second)
	//})
}

type MyConsumer struct{}

func (consumer *MyConsumer) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (consumer *MyConsumer) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (consumer *MyConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for m := range claim.Messages() {
		fmt.Println("consumer2: ", m.Value)
		session.MarkMessage(m, "")
	}
	return nil
}
