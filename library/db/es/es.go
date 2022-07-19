package es

import (
	"context"
	"encoding/json"

	"github.com/olivere/elastic/v7"
)

var _ ESInterface = &Es{}

func Connect(conf *EsConfig) (*Es, error) {
	elasticSet := []elastic.ClientOptionFunc{
		elastic.SetSniff(conf.Sniff),             // 集群嗅探。
		elastic.SetURL(conf.Host...),             // URL地址
		elastic.SetHealthcheck(conf.Healthcheck), // 健康检查
	}

	if conf.Healthcheck {
		elasticSet = append(elasticSet, []elastic.ClientOptionFunc{
			elastic.SetHealthcheckTimeout(conf.HealthTimeOut),
		}...)
	}
	if conf.Auth {
		elasticSet = append(elasticSet, elastic.SetBasicAuth(conf.Username, conf.Password)) // 账号密码
	}

	// 新建连接
	client, err := elastic.NewClient(
		elasticSet...,
	)
	if err != nil {
		return nil, err
	}
	return &Es{Cli: client, Conf: conf}, nil
}

//Search es搜索主体
//  index - 索引
//  query - 搜索筛选
//  obj   - 解析数据到结构体
func (es *Es) SearchOneByQuery(ctx context.Context, index string, query elastic.Query, obj interface{}) (*elastic.SearchResult, error) {
	resp, err := es.Cli.Search(index).Query(query).Size(ESHitSizeFrist).TrackTotalHits(ESTotalHits).Do(ctx)
	if err != nil {
		return nil, err
	}
	if len(resp.Hits.Hits) > 0 {
		err = json.Unmarshal(resp.Hits.Hits[0].Source, &obj)
	}

	return resp, err
}

//SearchByQueryWithAggs 通过条件搜索获取数据
//  aggs    - 需要聚合统计时则传入 map[string]*elastic.Aggregation {string：聚合所得的字段名称， *elastic.Aggregation：聚合条件}
//  hitSize - 搜索结果信息体条目数
func (es *Es) SearchByQueryWithAggs(ctx context.Context, index string, query elastic.Query, aggs map[string]*elastic.Aggregation, hitSize int) (*elastic.SearchResult, error) {
	esBoned := es.Cli.Search(index).Query(query).TrackTotalHits(ESTotalHits)
	if hitSize > 0 {
		esBoned = esBoned.Size(hitSize)
	} else {
		esBoned = esBoned.Size(ESDefaultSize)
	}
	for aggsKey, aggsInfo := range aggs {
		esBoned = esBoned.Aggregation(aggsKey, *aggsInfo)
	}

	return esBoned.Do(ctx)
}

//PageByQuery 根据赛选获取分页信息
func (es *Es) PageByQuery(ctx context.Context, in ESPage) (*elastic.SearchResult, error) {
	esBoned := es.Cli.Search(in.DataName).Query(in.Query).TrackTotalHits(ESTotalHits)
	if in.PageSize > 0 {
		esBoned = esBoned.From(in.PageNum * in.PageSize).Size(in.PageSize)
	} else {
		esBoned = esBoned.From(in.PageNum * ESDefaultSize).Size(ESDefaultSize)
	}
	for aggsKey, aggsInfo := range in.Aggregation {
		esBoned = esBoned.Aggregation(aggsKey, *aggsInfo)
	}

	return esBoned.Do(ctx)
}

//UpdateById 通过id进行文档更新
func (es *Es) UpdateById(ctx context.Context, index string, id string,
	query elastic.Query, data interface{}, create bool) error {
	_, err := es.Cli.Update().
		Index(index).
		Id(id).
		// data为结构体或map, 需注意的是如果使用结构体零值也会去更新原记录
		Upsert(data).
		// true 无则插入, 有则更新, 设置为false时记录不存在将报错
		DocAsUpsert(create).
		Do(ctx)

	return err
}

//UpdateByQuery 通过筛选条件更新
func (es *Es) UpdateByQuery(ctx context.Context, index string, query elastic.Query, script *elastic.Script) error {
	_, err := es.Cli.UpdateByQuery().Index(index).Query(query).Script(script).Do(ctx)
	return err
}

//DeleteById 删除记录
func (es *Es) DeleteById(ctx context.Context, index, id string) error {
	_, err := es.Cli.Delete().Index(index).Id(id).Refresh("true").Do(ctx)
	return err
}
