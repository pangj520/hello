package es

import (
	"context"
	"library_Test/library/utils"
	"time"

	"github.com/olivere/elastic/v7"
)

type ESInterface interface {
	//SearchOneByQuery 搜索单条记录
	SearchOneByQuery(context.Context, string, elastic.Query, interface{}) (*elastic.SearchResult, error)
	//SearchByQueryWithAggs 通过条件搜索获取数据
	//  aggs    - 需要聚合统计时则传入
	//  hitSize - 搜索结果信息体条目数
	SearchByQueryWithAggs(context.Context, string, elastic.Query, map[string]*elastic.Aggregation, int) (*elastic.SearchResult, error)
	//PageByQuery 获取分页数据
	PageByQuery(context.Context, ESPage) (*elastic.SearchResult, error)
	//UpdateById 通过id更新
	UpdateById(context.Context, string, string, elastic.Query, interface{}, bool) error
	//UpdateByQuery 通过筛选条件更新
	UpdateByQuery(context.Context, string, elastic.Query, *elastic.Script) error
	//DeleteById 删除记录
	DeleteById(ctx context.Context, index, id string) error
}

type ESDataInterface interface {
	// TypeName() string
}

// Es实例
type Es struct {
	Cli  *elastic.Client
	Conf *EsConfig
}

// Es连接配置
type EsConfig struct {
	DisableReload bool // 关闭重载
	Auth          bool // 是否需要鉴权登录
	Username      string
	Password      string
	Host          []string      // es ip:port
	Sniff         bool          // 是否开启嗅探
	Healthcheck   bool          // 是否需要开启健康检查
	HealthTimeOut time.Duration // 健康检查超时时间
}

// ESPage 搜索用传参
type ESPage struct {
	utils.Page                                  // 基础分页信息
	DataName    string                          // 索引名称或别名
	Query       elastic.Query                   // 条件filter
	Aggregation map[string]*elastic.Aggregation // 聚合统计
}
