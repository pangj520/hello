/**
 *描述：mongo客户端
 *      实现以连接mongoDB集群
 *      向其他文件提供读写MongoDB表方法
 *作者：江洪
 *时间：2019-5-20
 */

package nosql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"library_Test/common"
	"library_Test/library/log"
	"library_Test/library/utils"
	"time"

	"github.com/mitchellh/mapstructure"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"go.mongodb.org/mongo-driver/x/bsonx"
)

//多个mongo实例
//	支持不同库不同DB
type MongoInstances struct {
	instances       *sync.Map
	defaultInstance string
}

//mongodb实例
type MongoDB struct {
	lock             *sync.Mutex
	Client           *mongo.Client
	DataBase         *mongo.Database
	Config           *MongoConfig
	DefaultDbName    string
	DefaultDBOptions []*options.DatabaseOptions
}

//mongodb集合实例
type MongoCollection struct {
	Collection         *mongo.Collection
	DefaultTimeout     time.Duration
	RbSleepIncUnit     time.Duration //回滚重试失败后协程睡眠时长增长单位
	MaxRbSleepInterval time.Duration //回滚重试最大睡眠时长
}

//mongodb连接配置
type MongoConfig struct {
	DisableAuth           bool          //不需要鉴权
	Type                  MongoType     `check:"required"`
	Brokers               string        `check:"required"`
	UserName              string        //`check:"required"`
	Password              string        //`check:"required"`
	DbName                string        `check:"required"`
	Auth                  string        //`check:"required"`
	MaxPoolSize           int           //默认为0
	MinPoolSize           int           //默认为100，-1表示math.MaxInt64
	MaxConnIdleTime       time.Duration //默认为0
	ReplicaSetName        string
	DBOptions             []*options.DatabaseOptions
	EnableConcernMajority bool
}

type MongoType string

const (
	Standalone MongoType = "STANDALONE" //单实例
	ReplicaSet MongoType = "REPLICASET" //副本集
	Cluster    MongoType = "CLUSTER"    //分片集群
)

type MongoPage struct {
	utils.Page                                              //分页基础字段
	Rows        []interface{}                               //数据记录
	IterateFunc func(cur *mongo.Cursor, p *MongoPage) error //迭代查询结果函数
}

const (
	RollbackForInsert  = iota //回滚插入的数据
	RollbackForDel            //回滚删除的数据
	RollbackForUpdate         //回滚更新的数据
	RollbackForReplace        //回滚替换的数据
)

//mongodb json类型的数据
type MongoJson struct {
	Obj interface{}
}

/**
 *连接MongoDB集群，初始化连接对象
 */
func Connect(conf *MongoConfig) (*MongoDB, error) {
	log.Logger.Debugf("MongoConfig: %+v", *conf)
	err := check(conf)
	if err != nil {
		return nil, err
	}
	ctx, _ := context.WithTimeout(context.Background(), common.MongoConnectTimeout)
	var uri string
	if !conf.DisableAuth {
		if conf.UserName == "" {
			return nil, errors.New("MongoConfig.UserName must be not empty")
		}
		if conf.Password == "" {
			return nil, errors.New("MongoConfig.Password must be not empty")
		}
		if conf.Auth == "" {
			return nil, errors.New("MongoConfig.Auth must be not empty")
		}
		uri = fmt.Sprintf("mongodb://%s:%s@%s/%s", conf.UserName, conf.Password, conf.Brokers, conf.Auth)
	} else {
		uri = fmt.Sprintf("mongodb://%s", conf.Brokers)
	}
	clientOptions := options.Client().ApplyURI(uri)
	if conf.MaxPoolSize == 0 {
		conf.MaxPoolSize = 100
	} else if conf.MaxPoolSize < 0 {
		conf.MaxPoolSize = 0
	}
	if conf.MinPoolSize < 0 {
		conf.MinPoolSize = 0
	}
	switch conf.Type {
	case ReplicaSet:
		clientOptions.SetReplicaSet(conf.ReplicaSetName)
		want, err := readpref.New(readpref.SecondaryMode)
		if err != nil {
			return nil, err
		}
		clientOptions.SetMaxPoolSize(uint64(conf.MaxPoolSize))
		clientOptions.SetMinPoolSize(uint64(conf.MinPoolSize))
		clientOptions.SetMaxConnIdleTime(conf.MaxConnIdleTime)
		clientOptions.SetReadPreference(want) //表示只使用辅助节点
		if conf.EnableConcernMajority {
			//注意：以下两个谨慎使用，启用后会影响read与write性能
			clientOptions.SetReadConcern(readconcern.Majority())                      //指定查询应返回实例的最新数据确认为，已写入副本集中的大多数成员
			clientOptions.SetWriteConcern(writeconcern.New(writeconcern.WMajority())) //请求确认写操作传播到大多数mongod实例
		}
	case Cluster:
		//pass
	default:
		//pass
	}
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, err
	}
	ctx, _ = context.WithTimeout(ctx, common.MongoPingTimeout)
	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		return nil, err
	}
	db := client.Database(conf.DbName, conf.DBOptions...)
	return &MongoDB{Client: client, Config: conf,
		DataBase: db, DefaultDbName: conf.DbName,
		DefaultDBOptions: conf.DBOptions, lock: &sync.Mutex{}}, err
}

func check(mc *MongoConfig) error {
	err := utils.CheckRequiredStringField("MongoConfig", mc)
	if err != nil {
		return err
	}
	if mc.Type == ReplicaSet && mc.ReplicaSetName == "" {
		return errors.New("MongoConfig.ReplicaSetName must be not empty")
	}
	return nil
}

func NewMongoInstances() *MongoInstances {
	return &MongoInstances{instances: &sync.Map{}}
}

func (m *MongoInstances) Close() error {
	m.instances.Range(func(key, value interface{}) bool {
		err := value.(*MongoDB).Close()
		if err != nil {
			fmt.Println("failed to close mongo connection, name = ", key, ", err = ", err)
		}
		return true
	})
	return nil
}

//切换到指定的mongo实例
func (m *MongoInstances) AddInstance(instanceName string, ins *MongoDB, isDefault bool) {
	m.instances.Store(instanceName, ins)
	if isDefault {
		m.defaultInstance = instanceName
	}
	return
}

//切换到指定的mongo实例
//	如果实例不存在则返回nil
func (m *MongoInstances) GetInstance(instanceName string) *MongoDB {
	v, ok := m.instances.Load(instanceName)
	if !ok {
		fmt.Printf("the MongoInstance of %s is not exist\n", instanceName)
		return nil
	}
	return v.(*MongoDB)
}

//切换到默认的database
//	如果不存在则返回nil
func (m *MongoInstances) Default() *MongoDB {
	return m.GetInstance(m.defaultInstance)
}

//获取默认mongodb实例的指定collection
func (m *MongoInstances) GetCollection(name string, opts ...*options.CollectionOptions) *MongoCollection {
	return m.Default().GetCollection(name, opts...)
}

func (m *MongoDB) Close() error {
	ctx, _ := context.WithTimeout(context.Background(), common.MongoDisconnectTimeout)
	return m.Client.Disconnect(ctx)
}

//切换到指定的database
//	注意：必须有权限操作该DB
func (m *MongoDB) SwitchDb(name string, options ...*options.DatabaseOptions) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if len(options) != 0 {
		m.Config.DBOptions = append(m.Config.DBOptions, options...)
	}
	m.DataBase = m.Client.Database(name, m.Config.DBOptions...)
}

//切换到默认的database
func (m *MongoDB) SwitchToDefaultDb() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.DataBase = m.Client.Database(m.DefaultDbName, m.DefaultDBOptions...)
}

//获取指定collection
func (m *MongoDB) GetCollection(name string, opts ...*options.CollectionOptions) *MongoCollection {
	m.lock.Lock()
	defer m.lock.Unlock()
	c := m.DataBase.Collection(name, opts...)
	return &MongoCollection{Collection: c, DefaultTimeout: common.MongoCollectionDefaultTimeout,
		RbSleepIncUnit: common.MongoRbSleepIncUnit, MaxRbSleepInterval: common.MongoMaxRbSleepInterval}
}

//获取超时时间
//def:
//	如果超时时间未设置，将使用def作为超时时间
func (m *MongoCollection) GetTimeout(def *time.Duration) context.Context {
	var timeout time.Duration
	if def != nil {
		timeout = *def
	} else {
		timeout = m.DefaultTimeout
	}
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	return ctx
}

//插入一条数据
//doc:
//	map[string]*
//	common
//	bson.Marshal()生成的[]byte
//	bson.M()生成
func (m *MongoCollection) InsertOne(doc interface{}, timeout *time.Duration, opts ...*options.InsertOneOptions) (string, error) {
	if doc == nil {
		return "", errors.New("the parameter 'doc' cannot be nil")
	}
	res, err := m.Collection.InsertOne(m.GetTimeout(timeout), doc, opts...)
	if err != nil {
		return "", err
	}
	return res.InsertedID.(primitive.ObjectID).Hex(), nil
}

//更新一条数据
//filter:
//	map[string]*
//	common
//	bson.Marshal()生成的[]byte
//	bson.M()生成
//update:
//	map[string]*
//	common
//	bson.Marshal()生成的[]byte
//	bson.M()生成
//返回：
//	string：如果UpdateOptions.Upsert为true，则为更新插入后的id
//	err：错误
func (m *MongoCollection) UpdateOne(filter, update interface{}, timeout *time.Duration, opts ...*options.UpdateOptions) (string, error) {
	if update == nil {
		return "", errors.New("the parameter 'update' cannot be nil")
	}
	res, err := m.Collection.UpdateOne(m.GetTimeout(timeout), filter, update, opts...)
	if err != nil {
		return "", err
	}
	if res.UpsertedID != nil {
		return res.UpsertedID.(primitive.ObjectID).Hex(), nil
	}
	return "", nil
}

//更新多条数据。
// 如果filter没有指定任何过滤条件，则更新所有。
// 如果根据filter未能找到匹配的数据，则忽略更新
//filter:
//	不能为nil
//	map[string]*
//	common
//	bson.Marshal()生成的[]byte
//	bson.M()生成
//update:
//	不能为nil
//	map[string]*
//	common
//	bson.Marshal()生成的[]byte
//	bson.M()生成
//返回：
//	err：错误
func (m *MongoCollection) UpdateMany(filter, update interface{}, timeout *time.Duration, opts ...*options.UpdateOptions) error {
	if update == nil {
		return errors.New("the parameter 'update' cannot be nil")
	}
	if filter == nil {
		return errors.New("the parameter 'filter' cannot be nil")
	}
	_, err := m.Collection.UpdateMany(m.GetTimeout(timeout), filter, update, opts...)
	if err != nil {
		return err
	}
	return nil
}

//批量插入，不推荐使用
// 一旦有一条插入不成功，已经插入成功的不会回滚
func (m *MongoCollection) InsertMany(docs []interface{}, timeout *time.Duration, opts ...*options.InsertManyOptions) error {

	if len(docs) == 0 {
		return errors.New("the parameter 'docs' must be a non-empty slice")
	}
	res, err := m.Collection.InsertMany(m.GetTimeout(timeout), docs, opts...)
	if err != nil {
		return err
	}
	if len(res.InsertedIDs) != len(docs) {
		return errors.New(fmt.Sprintf("Batch insert document failed, total: %d, success: %d", len(docs), len(res.InsertedIDs)))
	}
	return nil
}

//条件查询列表
//filter类型:
//	map[string]*
//	common
//	bson.Marshal()生成的[]byte
//	bson.M()生成
//opts:
// 	mongo查询参数，如需排序或其他操作，请看options.FindOptions
func (m *MongoCollection) Find(filter interface{}, it func(cur *mongo.Cursor) (interface{}, error), timeout *time.Duration,
	opts ...*options.FindOptions) ([]interface{}, error) {
	if it == nil {
		return nil, errors.New("the parameter 'it' cannot be nil")
	}
	if filter == nil {
		filter = bson.D{}
	}
	ctx := m.GetTimeout(timeout)
	cur, err := m.Collection.Find(ctx, filter, opts...)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	var out []interface{}
	for cur.Next(ctx) {
		var r interface{}
		r, err = it(cur)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if err = cur.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

//分页查询
//	opts:
//		mongo查询参数，如需排序或其他操作，请看options.FindOptions
func (m *MongoCollection) FindByPage(filter interface{}, page *MongoPage, timeout *time.Duration,
	opts ...*options.FindOptions) error {
	if page == nil {
		return errors.New("the parameter 'page' cannot be nil")
	}
	if filter == nil {
		filter = bson.D{}
	}
	ctx := m.GetTimeout(timeout)

	totalCount, err := m.Count(filter, timeout)
	page.TotalCount = totalCount
	if err != nil {
		return err
	}
	// 当 page.PageNum 为0时，不分页
	if page.PageNum > 0 {
		skip := int64((page.GetPageNum() - 1) * page.GetPageSize())
		limit := int64(page.GetPageSize())

		opts = append(opts, &options.FindOptions{Skip: &skip, Limit: &limit})
	}
	cur, err := m.Collection.Find(ctx, filter, opts...)
	if err != nil {
		return err
	}
	defer cur.Close(ctx)
	if page.Rows == nil {
		page.Rows = []interface{}{}
	}
	for cur.Next(ctx) {
		err = page.IterateFunc(cur, page)
		if err != nil {
			return err
		}
	}
	if err = cur.Err(); err != nil {
		return err
	}
	return nil
}

//条件查询一条记录
//参数：
// filter: 条件参数
// out: 指针, 当out为struct指针时, struct中interface类型的字段为primitive.D或runtime.map或primitive.A, 慎用
func (m *MongoCollection) FindOne(filter interface{}, out interface{}, timeout *time.Duration,
	opts ...*options.FindOneOptions) error {
	if out == nil {
		return errors.New("the parameter 'out' cannot be nil")
	}
	if filter == nil {
		filter = bson.D{}
	}
	ctx := m.GetTimeout(timeout)
	res := m.Collection.FindOne(ctx, filter, opts...)
	err := res.Decode(out)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil
		}
		return err
	}
	return nil
}

//条件查询一条记录
//参数：
// filter: 条件参数
// out: 指针, 当out为struct指针时, struct中interface类型的字段为primitive.D或runtime.map或primitive.A, 慎用,
//无记录返回mongo.ErrNoDocuments
func (m *MongoCollection) TryFindOne(filter interface{}, out interface{}, timeout *time.Duration,
	opts ...*options.FindOneOptions) error {
	if out == nil {
		return errors.New("the parameter 'out' cannot be nil")
	}
	if filter == nil {
		filter = bson.D{}
	}
	ctx := m.GetTimeout(timeout)
	res := m.Collection.FindOne(ctx, filter, opts...)
	err := res.Decode(out)
	if err != nil {
		return err
	}
	return nil
}

//条件查询一条记录, 无记录返回mongo.ErrNoDocuments
func (m *MongoCollection) TryFindOneForMap(filter interface{}, timeout *time.Duration,
	opts ...*options.FindOneOptions) (out map[string]interface{}, err error) {
	//out = map[string]interface{}{}
	err = m.TryFindOne(filter, &out, timeout, opts...)
	return
}

//条件查询一条记录,
//struct中interface类型的字段为map[string]interface{}或[]interface{}
//参数：
// filter: 条件参数
// out: common 指针
//无记录返回mongo.ErrNoDocuments
func (m *MongoCollection) TryFindOneForStruct(filter interface{}, out interface{}, timeout *time.Duration,
	opts ...*options.FindOneOptions) error {
	mp, err := m.TryFindOneForMap(filter, timeout, opts...)
	if err != nil {
		return err
	}
	err = mapstructure.Decode(&mp, out)
	if err != nil {
		return err
	}
	return nil
}

//条件查询一条记录
func (m *MongoCollection) FindOneForMap(filter interface{}, timeout *time.Duration,
	opts ...*options.FindOneOptions) (out map[string]interface{}, err error) {
	//out = map[string]interface{}{}
	err = m.FindOne(filter, &out, timeout, opts...)
	return
}

//条件查询一条记录,
//struct中interface类型的字段为map[string]interface{}或[]interface{}
//参数：
// filter: 条件参数
// out: common 指针
func (m *MongoCollection) FindOneForStruct(filter interface{}, out interface{}, timeout *time.Duration,
	opts ...*options.FindOneOptions) error {
	mp, err := m.FindOneForMap(filter, timeout, opts...)
	if err != nil {
		return err
	}
	err = mapstructure.Decode(&mp, out)
	if err != nil {
		return err
	}
	return nil
}

//查找并删除一条记录
//	filter: 查询条件
//	out: 被删除的记录
func (m *MongoCollection) FindAndDelete(filter interface{}, out interface{}, timeout *time.Duration, opts ...*options.FindOneAndDeleteOptions) error {
	if filter == nil {
		filter = bson.D{}
	}
	ctx := m.GetTimeout(timeout)
	res := m.Collection.FindOneAndDelete(ctx, filter, opts...)
	err := res.Err()
	if err != nil {
		return err
	}
	err = res.Decode(out)
	if err != nil {
		return err
	}
	return nil
}

//查找并更新一条记录
//	filter:查询条件
//	update:更新的数据，bson.D或bson.M或map[string]interface{}，且包含$set操作符
//	out: 被更新的记录
//	timeout：超时时间
//	opts:额外参数
func (m *MongoCollection) FindAndUpdate(filter interface{}, update interface{}, out interface{},
	timeout *time.Duration, opts ...*options.FindOneAndUpdateOptions) error {
	if update == nil {
		return errors.New("the parameter 'update' cannot be nil")
	}
	if filter == nil {
		filter = bson.D{}
	}
	ctx := m.GetTimeout(timeout)
	res := m.Collection.FindOneAndUpdate(ctx, filter, update, opts...)
	err := res.Err()
	if err != nil {
		return err
	}
	err = res.Decode(out)
	if err != nil {
		return err
	}
	return nil
}

//查找并替换一条记录
//	filter:查询条件
//	replace:替换的数据，必须包含_id字段，且为primitive.ObjectID类型
//	out:被替换的记录
func (m *MongoCollection) FindAndReplace(filter interface{}, replace interface{}, out interface{},
	timeout *time.Duration, opts ...*options.FindOneAndReplaceOptions) error {
	if replace == nil {
		return errors.New("the parameter 'replace' cannot be nil")
	}
	if filter == nil {
		filter = bson.D{}
	}
	ctx := m.GetTimeout(timeout)
	res := m.Collection.FindOneAndReplace(ctx, filter, replace, opts...)
	err := res.Err()
	if err != nil {
		return err
	}
	err = res.Decode(out)
	if err != nil {
		return err
	}
	return nil
}

//符合条件的记录总数
func (m *MongoCollection) Count(filter interface{}, timeout *time.Duration, opts ...*options.CountOptions) (int64, error) {
	if filter == nil {
		filter = bson.D{}
	}
	return m.Collection.CountDocuments(m.GetTimeout(timeout), filter, opts...)
}

//删除一条记录
func (m *MongoCollection) Delete(filter interface{}, timeout *time.Duration, opts ...*options.DeleteOptions) (int64, error) {
	if filter == nil {
		return 0, errors.New("the parameter 'filter' cannot be nil")
	}
	res, err := m.Collection.DeleteOne(m.GetTimeout(timeout), filter, opts...)
	if err != nil {
		return 0, err
	}
	return res.DeletedCount, err
}

//删除多条记录
func (m *MongoCollection) DeleteMany(filter interface{}, timeout *time.Duration, opts ...*options.DeleteOptions) (int64, error) {
	if filter == nil {
		return 0, errors.New("the parameter 'filter' cannot be nil")
	}
	res, err := m.Collection.DeleteMany(m.GetTimeout(timeout), filter, opts...)
	if err != nil {
		return 0, err
	}
	return res.DeletedCount, err
}

//创建索引
//	field: 索引字段
//	value: 1或-1
//	opt: 可选配置
func (m *MongoCollection) CreateIndex(field string, value int32, opt *options.IndexOptions) error {
	if field == "" {
		return errors.New("field cannot be empty")
	}
	if value != 1 && value != -1 {
		return errors.New("value can only be 1 or -1")
	}
	_, err := m.Collection.Indexes().CreateOne(
		context.Background(),
		mongo.IndexModel{
			Keys:    bsonx.Doc{{field, bsonx.Int32(value)}},
			Options: opt,
		},
	)
	if err != nil {
		return err
	}
	return nil
}

//回滚，将被删除、已插入、已更新、已替换的数据恢复
//	filter:
//		回滚插入，filter类型为[]string或[]primitive.ObjectID
//		回滚删除，filter为被删除的数据, interface{}必须包含_id
//		回滚更新，filter为被更新的数据，interface{}必须包含_id
//		回滚替换，filter为被替换的数据，interface{}必须包含_id
//	rollbackType: 回滚类型，如下
//		RollbackForInsert                 //回滚插入的数据
//		RollbackForDel                    //回滚删除的数据
//		RollbackForUpdateOrReplace        //回滚更新或替换的数据
func (m *MongoCollection) Rollback(rollbackData []interface{}, rollbackType int) error {
	if rollbackData == nil || len(rollbackData) == 0 {
		return errors.New("rollbackData is required")
	}
	if rollbackType == RollbackForInsert {
		var delFilter interface{}
		if ids, ok := rollbackData[0].([]string); ok {
			transIds := make([]primitive.ObjectID, len(ids))
			for i := 0; i < len(ids); i++ {
				id, _ := primitive.ObjectIDFromHex(ids[i])
				transIds = append(transIds, id)
			}
			delFilter = bson.D{{"_id", bson.D{{"$in", transIds}}}}
		} else if _, ok := rollbackData[0].(primitive.ObjectID); ok {
			delFilter = bson.D{{"_id", bson.D{{"$in", rollbackData}}}}
		} else {
			return errors.New("rollbackData must be either []string or []primitive.ObjectID")
		}
		m.rollbackForInsert(delFilter)
		return nil
	}
	m.rollbackForOther(rollbackData, rollbackType)
	return nil
}

//回滚插入的数据
func (m *MongoCollection) rollbackForInsert(filter interface{}) {
	var currInterval time.Duration
	go func() {
		var err error
		for {
			_, err = m.Collection.DeleteMany(context.Background(), filter)
			if err == nil {
				log.Logger.WithField("MongoRollback", "RollbackForInsert").
					Info("rollback success")
				return
			}
			log.Logger.WithField("MongoRollback", "RollbackForInsert").
				Warn("rollback failed, err: ", err)
			if currInterval >= m.MaxRbSleepInterval {
				currInterval = m.MaxRbSleepInterval
			} else {
				currInterval += m.RbSleepIncUnit
			}
			time.Sleep(currInterval)
		}
	}()
}

//回滚删除、更新、替换的数据
func (m *MongoCollection) rollbackForOther(resetData []interface{}, rollbackType int) {
	var currInterval time.Duration
	go func() {
		var fails []interface{}
		var logValue string
		for {
			var err error
			var currFails []interface{}
			if len(fails) != 0 {
				resetData = fails
			}
			if rollbackType == RollbackForDel {
				logValue = "RollbackForDel"
				for i, _ := range resetData {
					_, err = m.Collection.InsertOne(context.Background(), resetData[i])
					if err == nil {
						continue
					}
					currFails = append(currFails, resetData)
					log.Logger.WithField("MongoRollback", logValue).
						Warn("rollback failed, err: ", err)
				}
			} else {
				logValue = "RollbackForUpdateOrReplace"
				var temp map[string]interface{}
				for i, _ := range resetData {
					temp = make(map[string]interface{})
					bs, e := bson.Marshal(resetData[i])
					if e != nil {
						log.Logger.WithField("MongoRollback", logValue).Error(e)
						continue
					}
					e = bson.Unmarshal(bs, &temp)
					if e != nil {
						log.Logger.WithField("MongoRollback", logValue).Error(e)
						continue
					}
					if _, ok := temp["_id"]; !ok {
						log.Logger.WithField("MongoRollback", logValue).
							Error("_id does not exist, data: ", resetData[i])
						continue
					}
					switch rollbackType {
					case RollbackForUpdate:
						_, err = m.Collection.UpdateOne(context.Background(), bson.M{"_id": temp["_id"]},
							bson.M{"$set": temp})
					case RollbackForReplace:
						_, err = m.Collection.ReplaceOne(context.Background(), bson.M{"_id": temp["_id"]},
							temp)
					}
					if err == nil {
						continue
					}
					currFails = append(currFails, resetData)
					log.Logger.WithField("MongoRollback", logValue).
						Warn("rollback failed, err: ", err)
				}
			}
			if err == nil {
				log.Logger.WithField("MongoRollback", logValue).
					Info("rollback success ")
				return
			}
			fails = currFails
			if currInterval >= m.MaxRbSleepInterval {
				currInterval = m.MaxRbSleepInterval
			} else {
				currInterval += m.RbSleepIncUnit
			}
			time.Sleep(currInterval)
		}
	}()
}

func (m *MongoJson) ValidateObj() error {
	v := reflect.ValueOf(m.Obj)
	if m.Obj == nil || (v.Kind() != reflect.Struct && v.IsNil()) {
		return errors.New("MongoJson.Obj can't be nil")
	}
	return nil
}

func (m *MongoJson) MarshalJSON() ([]byte, error) {
	if err := m.ValidateObj(); err != nil {
		return nil, err
	}
	return json.Marshal(m.Obj)
}

func (m *MongoJson) UnmarshalJSON(b []byte) error {
	if err := m.ValidateObj(); err != nil {
		return err
	}
	return json.Unmarshal(b, m.Obj)
}

func (m *MongoJson) MarshalBSON() ([]byte, error) {
	if err := m.ValidateObj(); err != nil {
		return nil, err
	}
	return bson.Marshal(m.Obj)
}

func (m *MongoJson) UnmarshalBSON(data []byte) error {
	if err := m.ValidateObj(); err != nil {
		return err
	}
	return bson.Unmarshal(data, m.Obj)
}
