package mq

import (
	"github.com/Shopify/sarama"
	"github.com/twinj/uuid"
	"sync"
	"library_Test/common/svcinit/domain"
	"library_Test/library/config"
	"library_Test/library/log"
	"library_Test/library/utils"
)

//创建同步生产者,不建议每生产一条消息创建一个SyncProducer
func CreateSyncProducer(kafkaConfig *KafkaConfig) (*SyncProducer, error) {
	if kafkaConfig == nil {
		c, err := CreateKafkaConfig()
		if err != nil {
			return nil, err
		}
		kafkaConfig = c
	}
	id := uuid.NewV4().String()
	kafkaConfig.SaramaConfig.ClientID = id
	p, err := sarama.NewSyncProducer(kafkaConfig.Brokers, kafkaConfig.SaramaConfig)
	if err != nil {
		return nil, err
	}
	sp := &SyncProducer{Producer: p, Conf: kafkaConfig, ClientID: id}
	sp.Subscribe()
	return sp, nil
}

//创建异步生产者，不建议每生产一条消息创建一个AsyncProducer
func CreateAsyncProducer(kafkaConfig *KafkaConfig) (*AsyncProducer, error) {
	if kafkaConfig == nil {
		c, err := CreateKafkaConfig()
		if err != nil {
			return nil, err
		}
		kafkaConfig = c
	}
	id := uuid.NewV4().String()
	kafkaConfig.SaramaConfig.ClientID = id
	p, err := sarama.NewAsyncProducer(kafkaConfig.Brokers, kafkaConfig.SaramaConfig)
	if err != nil {
		return nil, err
	}
	ap := &AsyncProducer{Producer: p, Conf: kafkaConfig, ClientID: id, wg: sync.WaitGroup{}}
	go ap.dealProduceRes()
	ap.Subscribe()
	return ap, nil
}

//处理异步生产者生产消息结果
func (p *AsyncProducer) dealProduceRes() {
	p.closed = make(chan bool, 1)
	p.wg.Add(2)
	go func() {
		defer p.wg.Done()
		for r := range p.Producer.Successes() {
			if r == nil {
				continue
			}
			if len(r.Headers) == 0 {
				log.Logger.Info("Message sent successfully, res = ", r)
				continue
			}
			producerId := ""
			for i := range r.Headers {
				if utils.Bytes2str(r.Headers[i].Key) != "ProducerId" {
					continue
				}
				producerId = utils.Bytes2str(r.Headers[i].Value)
			}
			log.Logger.Info("Async message sent successfully,  ProducerId = ", producerId)
		}
	}()

	go func() {
		defer p.wg.Done()
		for e := range p.Producer.Errors() {
			if e == nil || e.Err == nil {
				continue
			}
			if e.Msg == nil {
				log.Logger.Info("Message sent failed, err = ", e)
				continue
			}
			if len(e.Msg.Headers) == 0 {
				log.Logger.Info("Message sent failed, err = ", e)
				continue
			}
			producerId := ""
			for i := range e.Msg.Headers {
				if utils.Bytes2str(e.Msg.Headers[i].Key) != "ProducerId" {
					continue
				}
				producerId = utils.Bytes2str(e.Msg.Headers[i].Value)
			}
			log.Logger.Info("Async message sent failed,  ProducerId = ", producerId)
		}
	}()
	p.wg.Wait()
	close(p.closed)
}

type AsyncProducer struct {
	Producer sarama.AsyncProducer
	Conf     *KafkaConfig
	ClientID string
	wg       sync.WaitGroup
	closed   chan bool
	SubId    string
}

//指定key能保证消息被顺序消费，不指定则乱序,
// 但指定key，producer只会往一个broker发送消息，存在多个同名消费组消费时，只有连接了这个broker的消费组能读取到消息
func (p *AsyncProducer) SendMessage(topic, key, value string) {
	ProducerId := uuid.NewV4().String()
	msg := &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(value),
		Headers: []sarama.RecordHeader{{Key: utils.Str2bytes("ProducerId"),
			Value: utils.Str2bytes(ProducerId)}}}
	if key != "" {
		msg.Key = sarama.StringEncoder(key)
	}
	select {
	case p.Producer.Input() <- msg:
		log.Logger.Info("Async message sent, ProducerId = " + ProducerId)
	}
}

//指定key能保证消息被顺序消费，不指定则乱序,
// 但指定key，producer只会往一个broker发送消息，存在多个同名消费组消费时，只有连接了这个broker的消费组能读取到消息
func (p *AsyncProducer) SendByte(topic, key string, value []byte) {
	ProducerId := uuid.NewV4().String()
	msg := &sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(value),
		Headers: []sarama.RecordHeader{{Key: utils.Str2bytes("ProducerId"),
			Value: utils.Str2bytes(ProducerId)}}}
	if key != "" {
		msg.Key = sarama.StringEncoder(key)
	}
	select {
	case p.Producer.Input() <- msg:
		log.Logger.Info("Async message sent, ProducerId = " + ProducerId)
	}
}

func (p *AsyncProducer) Close() {
	p.Producer.AsyncClose()
	<-p.closed
	p.UnSubscribe()
}

func (p *AsyncProducer) Reconnect() error {
	log.Logger.Info("Wait for the old connection to close")
	p.Close()
	log.Logger.Info("The old connection is closed. Reconnection is starting")
	oldBrokers := p.Conf.Brokers
	oldClientId := p.Conf.SaramaConfig.ClientID
	p.Conf.Brokers = domain.DCtx.Options.KafkaBrokers
	id := uuid.NewV4().String()
	p.Conf.SaramaConfig.ClientID = id
	np, err := sarama.NewAsyncProducer(p.Conf.Brokers, p.Conf.SaramaConfig)
	if err != nil {
		p.Conf.Brokers = oldBrokers
		p.Conf.SaramaConfig.ClientID = oldClientId
		return err
	}
	p.Producer = np
	go p.dealProduceRes()
	p.Subscribe()
	log.Logger.Info("Reconnection to complete")
	return nil
}

//订阅配置变更
func (p *AsyncProducer) Subscribe() {
	p.SubId = domain.SubscriMqChange(func(change config.Change) {
		err := p.Reconnect()
		if err != nil {
			log.Logger.Error(err)
		}
	})
}

func (p *AsyncProducer) UnSubscribe() {
	if p.SubId == "" {
		return
	}
	domain.UnSubscriMqChange(p.SubId)
	p.SubId = ""
}

type SyncProducer struct {
	Producer sarama.SyncProducer
	Conf     *KafkaConfig
	ClientID string
	SubId    string
}

//指定key能保证consumer按顺序消费, 不指定则乱序
func (p *SyncProducer) SendMessage(topic string, key, value string) (partition int32, offset int64, err error) {
	ProducerId := uuid.NewV4().String()
	defer func() {
		log.Logger.Info("Sync message sent, ProducerId = " + ProducerId)
	}()
	msg := &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(value),
		Headers: []sarama.RecordHeader{{Key: utils.Str2bytes("ProducerId"),
			Value: utils.Str2bytes(ProducerId)}}}
	if key != "" {
		msg.Key = sarama.StringEncoder(key)
	}
	return p.Producer.SendMessage(msg)
}

//指定key能保证consumer按顺序消费, 不指定则乱序
func (p *SyncProducer) SendByte(topic string, key string, value []byte) (partition int32, offset int64, err error) {
	ProducerId := uuid.NewV4().String()
	defer func() {
		log.Logger.Info("Sync message sent, ProducerId = " + ProducerId)
	}()
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(value),
		Headers: []sarama.RecordHeader{{
			Key:   utils.Str2bytes("ProducerId"),
			Value: utils.Str2bytes(ProducerId)}},
	}
	if key != "" {
		msg.Key = sarama.StringEncoder(key)
	}
	return p.Producer.SendMessage(msg)
}

func (p *SyncProducer) Close() error {
	defer p.UnSubscribe()
	return p.Producer.Close()
}

func (p *SyncProducer) Reconnect() error {
	log.Logger.Info("Wait for the old connection to close")
	err := p.Close()
	if err != nil {
		return err
	}
	log.Logger.Info("The old connection is closed. Reconnection is starting")
	oldBrokers := p.Conf.Brokers
	oldClientId := p.Conf.SaramaConfig.ClientID
	p.Conf.Brokers = domain.DCtx.Options.KafkaBrokers
	id := uuid.NewV4().String()
	p.Conf.SaramaConfig.ClientID = id
	np, err := sarama.NewSyncProducer(p.Conf.Brokers, p.Conf.SaramaConfig)
	if err != nil {
		p.Conf.Brokers = oldBrokers
		p.Conf.SaramaConfig.ClientID = oldClientId
		return err
	}
	p.Producer = np
	p.Subscribe()
	log.Logger.Info("Reconnection to complete")
	return nil
}

//订阅配置变更
func (p *SyncProducer) Subscribe() {
	p.SubId = domain.SubscriMqChange(func(change config.Change) {
		err := p.Reconnect()
		if err != nil {
			log.Logger.Error(err)
		}
	})
}

func (p *SyncProducer) UnSubscribe() {
	if p.SubId == "" {
		return
	}
	domain.UnSubscriMqChange(p.SubId)
	p.SubId = ""
}
