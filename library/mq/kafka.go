/**
 *描述：kafka客户端
 *      实现以连接kafka集群，实现producer端和consumer端
 *作者：江洪
 *时间：2019-5-20
 */

package mq

import (
	"errors"
	"github.com/Shopify/sarama"
	"library_Test/common/svcinit/domain"
	"time"
	"library_Test/library/log"
)

type KafkaConfig struct {
	SaramaConfig *sarama.Config
	Brokers      []string
	Version      string
}

/**
创建kafka默认配置
args[0]: kafka brokers, []string
args[1]: kafka version, string
*/
func CreateKafkaConfig(args ...interface{}) (*KafkaConfig, error) {
	var brokers []string
	var version string
	if len(args) >= 2 {
		brokers = args[0].([]string)
		version = args[1].(string)
	} else {
		brokers = domain.DCtx.Options.KafkaBrokers
		version = domain.DCtx.Options.KafkaVersion
	}
	if len(brokers) == 0 {
		return nil, errors.New("kafka brokers is empty")
	}
	if version == "" {
		return nil, errors.New("kafka version is empty")
	}
	sarama.Logger = log.Logger.WithField("source", "kafka")
	v, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		return nil, err
	}

	/**
	 * Construct a new Sarama configuration.
	 * The Kafka cluster version has to be defined before the consumer/producer is initialized.
	 */
	conf := KafkaConfig{Brokers: brokers, Version: version}
	conf.SaramaConfig = DefaultSaramaConfig()
	conf.SaramaConfig.Version = v
	return &conf, nil
}

//默认的sarama配置
func DefaultSaramaConfig() *sarama.Config {
	conf := sarama.NewConfig()
	conf.Consumer.Group.Session.Timeout = 20 * time.Second
	//心跳检查间隔，保证会话处于活动状态，并且保证消费者加入或者退出消费组时重新平衡
	conf.Consumer.Group.Heartbeat.Interval = 6 * time.Second
	//用户对消息的最大处理时长
	conf.Consumer.MaxProcessingTime = 500 * time.Millisecond
	//消费异常时返回错误
	conf.Consumer.Return.Errors = true
	conf.Producer.Return.Errors = true
	conf.Producer.Return.Successes = true
	//连接kafka失败,最大重试次数
	conf.Consumer.Group.Rebalance.Retry.Max = 4

	//连接kafka失败，重试间隔，该值越小，则在kafka出现问题时，会大量写错误日志，默认250ms,建议适当延长
	conf.Metadata.Retry.Backoff = time.Second

	//offset提交间隔，必须大于0
	conf.Consumer.Offsets.AutoCommit.Interval = time.Millisecond * 100
	//OffsetNewest,只消费当前时间起产生的消息，已存在的消费组从上次提交的offset开始消费
	//OffsetOldest,从头开始消费(消费topic的所有消息)，已存在的消费组从上次提交的offset开始消费
	conf.Consumer.Offsets.Initial = sarama.OffsetNewest
	conf.Consumer.IsolationLevel = sarama.ReadUncommitted
	return conf
}
