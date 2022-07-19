package mq

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/twinj/uuid"
	"library_Test/common"
	"library_Test/common/svcinit/domain"
	"library_Test/library/config"
	"library_Test/library/log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type ConsumerGroupClient struct {
	Client         sarama.ConsumerGroup
	Config         *ConsumerGroupConfig
	ClientID       string
	MessageHandler func(*sarama.ConsumerMessage) //消息处理函数
	SigTerm        chan os.Signal                //信号，用于关闭客户端
	Closed         chan bool                     //客户端是否完全关闭
	SubId          string
}

type ConsumerGroupConfig struct {
	Topics      []string
	Group       string
	KafkaConfig *KafkaConfig
}

//创建消费组客户端
//当producer产生的消息指定key，则只有一个消费组客户端能消费到消息，不指定则都能消费到；
//并发消费同一个group需创建多个消费组客户端
func CreateConsumerGroupClient(groupConfig *ConsumerGroupConfig) (*ConsumerGroupClient, error) {
	var err error
	if groupConfig.KafkaConfig == nil {
		conf, err := CreateKafkaConfig()
		if err != nil {
			return nil, err
		}
		groupConfig.KafkaConfig = conf
	}
	c := &ConsumerGroupClient{Config: groupConfig}

	err = createConsumerGroup(c)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func createConsumerGroup(c *ConsumerGroupClient) error {
	var oldClientId string
	if c.Config.KafkaConfig.SaramaConfig.ClientID != "" {
		oldClientId = c.Config.KafkaConfig.SaramaConfig.ClientID
	}
	c.ClientID = uuid.NewV4().String()
	c.Config.KafkaConfig.SaramaConfig.ClientID = c.ClientID
	client, err := sarama.NewConsumerGroup(c.Config.KafkaConfig.Brokers,
		c.Config.Group, c.Config.KafkaConfig.SaramaConfig)
	if err != nil {
		c.Config.KafkaConfig.SaramaConfig.ClientID = oldClientId
		return err
	}
	c.Client = client
	c.Closed = make(chan bool, 1)
	c.SigTerm = make(chan os.Signal, 1)
	signal.Notify(c.SigTerm, syscall.SIGINT, syscall.SIGTERM, os.Kill)
	//监听kafka错误
	go func() {
		for err := range c.Client.Errors() {
			if err != nil {
				log.Logger.Error(err)
			}
		}
	}()
	c.Subscribe()
	return nil
}

//开始消费
//handler: 消息处理函数， func(*sarama.ConsumerMessage)
func (c *ConsumerGroupClient) Start(handler func(*sarama.ConsumerMessage)) {
	c.MessageHandler = handler
	go c.consume()
}

//消费消息，想并发消费，需创建多个消费组客户端
func (c *ConsumerGroupClient) consume() {
	log.Logger.Info("consumer client id: ", c.Config.KafkaConfig.SaramaConfig.ClientID)
	ctx, cancel := context.WithCancel(context.Background())
	h := &Consumer{DealFunc: c.MessageHandler, ready: make(chan bool)}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			err := c.Client.Consume(ctx, c.Config.Topics, h)
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				log.Logger.Error("ctx.Err()!= nil  and return")
				return
			}
			if err == nil {
				log.Logger.Error("err == nil and return")
				h.ready = make(chan bool)
				continue
			}
			if err == sarama.ErrClosedClient {
				log.Logger.Error("err == sarama.ErrClosedClient and return")
				return
			}
			log.Logger.Warn(err)
			if c.Config.KafkaConfig.SaramaConfig.Metadata.Retry.Backoff == common.KafkaRetryIntervalMax {
				continue
			}
			c.Config.KafkaConfig.SaramaConfig.Metadata.Retry.Backoff += time.Second
		}
	}()
	<-h.ready // Await till the consumer has been set up
	log.Logger.Info("Sarama consumer up and running!...")

	select {
	case <-ctx.Done():
		log.Logger.Info("terminating: context cancelled")
	case <-c.SigTerm:
		log.Logger.Info("terminating: via signal")
	}
	cancel()
	wg.Wait()
	if err := c.Client.Close(); err != nil {
		log.Logger.Error("Error closing client: %v", err)
	} else {
		log.Logger.Info("consumer group client closed.")
	}
	close(c.Closed)
}

//停止从kafka读取消息
func (c *ConsumerGroupClient) Close() {
	c.SigTerm <- syscall.SIGTERM
	<-c.Closed
	c.UnSubscribe()
}

//重连客户端
func (c *ConsumerGroupClient) Reconnect() error {
	log.Logger.Info("Wait for the old connection to close")
	c.Close()
	log.Logger.Info("The old connection is closed. Reconnection is starting")
	oldBrokers := c.Config.KafkaConfig.Brokers
	oldVersion := c.Config.KafkaConfig.Version
	oldKafkaVersion := c.Config.KafkaConfig.SaramaConfig.Version
	if oldVersion != domain.DCtx.Options.KafkaVersion {
		v, err := sarama.ParseKafkaVersion(domain.DCtx.Options.KafkaVersion)
		if err != nil {
			return err
		}
		c.Config.KafkaConfig.SaramaConfig.Version = v
	}
	c.Config.KafkaConfig.Brokers = domain.DCtx.Options.KafkaBrokers
	c.Config.KafkaConfig.Version = domain.DCtx.Options.KafkaVersion
	err := createConsumerGroup(c)
	if err != nil {
		c.Config.KafkaConfig.Brokers = oldBrokers
		c.Config.KafkaConfig.Version = oldVersion
		c.Config.KafkaConfig.SaramaConfig.Version = oldKafkaVersion
		return err
	}
	go c.consume()
	log.Logger.Info("Reconnection to complete")
	return nil
}

//订阅配置变更
func (c *ConsumerGroupClient) Subscribe() {
	c.SubId = domain.SubscriMqChange(func(change config.Change) {
		err := c.Reconnect()
		if err != nil {
			log.Logger.Error(err)
		}
	})
}

func (c *ConsumerGroupClient) UnSubscribe() {
	if c.SubId == "" {
		return
	}
	domain.UnSubscriMqChange(c.SubId)
	c.SubId = ""
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready       chan bool
	DealFunc    func(*sarama.ConsumerMessage)
	LastMessage *sarama.ConsumerMessage //最后一条消息
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(session sarama.ConsumerGroupSession) error {
	close(consumer.ready)
	if consumer.LastMessage != nil {
		log.Logger.Info("new session start，last offset=", consumer.LastMessage.Offset)
		//旧的会话未提交的offset会被丢弃，在这里重新设置，阻止重复消费
		session.MarkOffset(consumer.LastMessage.Topic,
			consumer.LastMessage.Partition,
			consumer.LastMessage.Offset+1, "")
	}
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	log.Logger.Info("session end.")
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for m := range claim.Messages() {
		log.Logger.Info("high offset: ", claim.HighWaterMarkOffset(), ", curr offset：", m.Offset)
		consumer.LastMessage = m
		consumer.DealFunc(m)
		session.MarkMessage(m, "")
	}
	return nil
}
