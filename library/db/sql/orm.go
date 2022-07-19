/**
 *描述：orm客户端
 *      实现以orm的形式连接mysql集群的客户端
 *      向其他文件提供原生orm的执行方法
 *作者：江洪
 *时间：2019-5-20
 */

package sql

import (
	"fmt"
	"github.com/go-sql-driver/mysql"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"library_Test/common"
	"library_Test/library/log"
	"library_Test/library/servicediscovery"
	"time"

	"errors"
	"github.com/jinzhu/gorm"
	"github.com/sirupsen/logrus"
)

type OrmSql struct {
	//Deprecated
	//请使用GetMaster()获取
	Master *OrmDb
	//Deprecated
	//请使用GetSlave()获取
	Slave            *OrmDb
	MasterConfig     *MysqlConfig
	SlaveConfig      *MysqlConfig
	master           *OrmDb
	slave            *OrmDb
	masterInstances  *sync.Map //key:{数据库地址}:{数据库端口}，value: *OrmDb
	slaveInstances   *sync.Map //key:{数据库地址}:{数据库端口}，value: *OrmDb
	discoverChanFlag bool      //是否已启动服务发现变更通道-消息处理流程
}

/**
生成masterInstances、slaveInstances map的key
*/
func InstanceMapKey(addr string, port int) string {
	return fmt.Sprintf("%s:%d", addr, port)
}

type OrmDb struct {
	*gorm.DB
	service servicediscovery.Service
}

func NewOrmSql(m, s *MysqlConfig) (*OrmSql, error) {
	ormDb := &OrmSql{MasterConfig: m, SlaveConfig: s, discoverChanFlag: false}
	err := ormDb.init("master", m)
	if err != nil {
		return nil, err
	}
	err = ormDb.init("slave", s)
	if err != nil {
		return nil, err
	}
	return ormDb, nil
}

func (o *OrmSql) init(t string, c *MysqlConfig) error {
	if !c.UseDiscovery {
		db, err := newOrmDb(c)
		if err != nil {
			return err
		}
		if t == "master" {
			o.master = db
			o.Master = o.master
		} else {
			o.slave = db
			o.Slave = o.slave
		}
		return nil
	}
	if c.Discovery == nil {
		return errors.New("when UseDiscovery = true, Discovery cannot be nil")
	}
	if c.ServiceName == "" {
		return errors.New("when UseDiscovery = true, ServiceName cannot be empty")
	}
	filterOpts := &servicediscovery.ServiceFilterOptions{
		Tags: c.Tags,
	}
	service, err := c.Discovery.ServiceDiscover(c.ServiceName, filterOpts)
	if err != nil {
		return err
	}
	if len(service) == 0 {
		return ErrNoServiceAlive
	}
	var instances *sync.Map
	if t == "master" {
		o.masterInstances = &sync.Map{}
		instances = o.masterInstances
	} else {
		o.slaveInstances = &sync.Map{}
		instances = o.slaveInstances
	}
	for _, s := range service {
		m := *c
		m.Addr = maybeIpv6(s.Address)
		m.Port = strconv.Itoa(s.Port)
		key := InstanceMapKey(m.Addr, s.Port)
		log.Logger.WithField("Orm", t).Debug("sql service ", key, " was found")
		db, err := newOrmDb(&m)
		if err != nil {
			continue
		}
		db.service = s
		instances.Store(key, db)
	}

	//启动变更通知消息处理流程
	if !o.discoverChanFlag {
		o.discoverChanFlag = true
		go func() {
			changeChan := c.Discovery.GetDiscoverChan(c.ServiceName, filterOpts)
			for services := range changeChan {
				if o.masterInstances != nil {
					o.UpdateInstances(o.masterInstances, services, o.MasterConfig)
				}
				if o.slaveInstances != nil {
					o.UpdateInstances(o.slaveInstances, services, o.SlaveConfig)
				}
			}
		}()
	}
	return nil
}

func newOrmDb(c *MysqlConfig) (*OrmDb, error) {
	err := check(c)
	if err != nil {
		return nil, err
	}
	cts := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8&parseTime=True&loc=Local&autocommit=true", c.UserName, c.Password, c.Addr, c.Port, c.Database)
	db, err := gorm.Open("mysql", cts)
	if err != nil {
		return nil, err
	}
	db.SingularTable(true)

	db.LogMode(!c.DisableLog)
	db.SetLogger(&OrmDbLogger{})
	//连接最大存活时间，该时间过期后，连接将被复用
	var connMaxLifetime time.Duration
	//connMaxLifetime := time.Minute
	if c.ConnMaxLifetime.Seconds() > 0 {
		connMaxLifetime = c.ConnMaxLifetime
	}
	//最大空闲连接数，默认是0
	maxIdleConns := 0
	if c.MaxIdleConns > 0 {
		maxIdleConns = c.MaxIdleConns
	}
	//最多可以打开的连接数
	maxOpenConns := 0
	if c.MaxOpenConns > 0 {
		maxOpenConns = c.MaxOpenConns
	}
	db.DB().SetConnMaxLifetime(connMaxLifetime)
	db.DB().SetMaxIdleConns(maxIdleConns)
	db.DB().SetMaxOpenConns(maxOpenConns)
	return &OrmDb{DB: db}, db.Error
}

//ORM日志
type OrmDbLogger struct {
}

func (l OrmDbLogger) Print(values ...interface{}) {
	old := log.GetLogrusLogger(log.Logger).ReportCaller
	log.GetLogrusLogger(log.Logger).ReportCaller = false
	level := logrus.InfoLevel
	if values[0].(string) == "log" {
		if _, ok := values[2].(*mysql.MySQLError); ok {
			level = logrus.ErrorLevel
		}
	}
	log.Logger.WithField("source", "orm").Log(level, gorm.LogFormatter(values...)...)
	log.GetLogrusLogger(log.Logger).ReportCaller = old
}

func (o *OrmSql) Close() error {
	if !o.MasterConfig.UseDiscovery {
		err := o.master.Close()
		if err != nil {
			return err
		}
	} else {
		o.masterInstances.Range(func(key, value interface{}) bool {
			_ = value.(*OrmDb).Close()
			return true
		})
	}
	if !o.SlaveConfig.UseDiscovery {
		err := o.master.Close()
		if err != nil {
			return err
		}
		return nil
	} else {
		o.slaveInstances.Range(func(key, value interface{}) bool {
			_ = value.(*OrmDb).Close()
			return true
		})
	}
	return nil
}

//如果启用服务发现，当找不到任何健康sql服务时，返回error
func (o *OrmSql) GetMaster(failDbs ...*OrmDb) (*OrmDb, error) {
	if !o.MasterConfig.UseDiscovery {
		return o.master, nil
	}
	return o.discoveryOne(o.masterInstances, o.MasterConfig, getExcludedServices(failDbs...)...)
}

//获取不再被选中的sql服务
func getExcludedServices(failDbs ...*OrmDb) []servicediscovery.Service {
	//不再选中已经失败的sql实例
	var excludedServices []servicediscovery.Service
	if len(failDbs) > 0 {
		for _, db := range failDbs {
			if db == nil {
				continue
			}
			if db.service.IsNull() {
				continue
			}
			excludedServices = append(excludedServices, db.service)
		}
	}
	return excludedServices
}

//如果启用服务发现，当找不到任何健康sql服务时，返回error
func (o *OrmSql) GetSlave(failDbs ...*OrmDb) (*OrmDb, error) {
	if !o.SlaveConfig.UseDiscovery {
		return o.slave, nil
	}
	return o.discoveryOne(o.slaveInstances, o.SlaveConfig, getExcludedServices(failDbs...)...)
}

func (o *OrmSql) discoveryOne(instances *sync.Map, config *MysqlConfig, excluded ...servicediscovery.Service) (*OrmDb, error) {
	filterOpts := &servicediscovery.ServiceFilterOptions{
		Tags:          config.Tags,
		UseSelectMode: true,
		SelectMode:    config.SelectMode,
	}
	if len(excluded) > 0 {
		filterOpts.ExcludedServices = excluded
	}
	services, fromConsulAPI, err := config.Discovery.ServiceDiscoverAll(config.ServiceName, filterOpts)
	if err != nil {
		return nil, err
	}
	if len(services) == 0 {
		//consul未发现可用数据库时则从现有instances中返回一个对象
		return o.fetchOneOrmDb(instances, nil)
	}

	if fromConsulAPI {
		//当从consul api而非local发现的服务时则更新instances
		o.UpdateInstances(instances, services, config)
	}

	//根据consul发现结果,从instances随机返回一个对象
	selectService := config.Discovery.SelectOneService(services, filterOpts)
	return o.fetchOneOrmDb(instances, selectService)
}

/**
从instances实例中获取一个OrmDB对象,当service非空时则返回该service对应的OrmDB对象
*/
func (o *OrmSql) fetchOneOrmDb(instances *sync.Map, service *servicediscovery.Service) (*OrmDb, error) {
	var db *OrmDb
	if service != nil {
		addr := maybeIpv6(service.Address)
		port := service.Port
		key := InstanceMapKey(addr, port)
		if value, ok := instances.Load(key); ok {
			db = value.(*OrmDb)
			log.Logger.WithField("Type", "Orm").Debug("select sql service:", key)
		} else {
			return nil, ErrNoServiceAlive
		}
		return db, nil
	}

	instances.Range(func(key, value interface{}) bool {
		db = value.(*OrmDb)
		log.Logger.WithField("Type", "Orm").Debug("select sql service:", key)
		return false
	})
	if db != nil {
		return db, nil
	}
	log.Logger.WithField("Type", "Orm").Debug("select sql service fail")
	return nil, ErrNoServiceAlive
}

/**
根据consul发现的服务更新数据库实例,consul发现的新服务则加入instances,consul未发现的服务则认为不健康踢出instances
*/
func (o *OrmSql) UpdateInstances(instances *sync.Map, services []servicediscovery.Service, config *MysqlConfig) {
	//services长度为0则不作更新,以防consul故障导致所有db对象被清空而可不用
	if len(services) == 0 {
		return
	}

	//先获取所有发现的数据库服务,然后将现有instances中已存在的服务剔出,剩余则是新服务
	newService := make(map[string]*servicediscovery.Service)
	for _, service := range services {
		addr := maybeIpv6(service.Address)
		port := service.Port
		discoverKey := InstanceMapKey(addr, port)
		newService[discoverKey] = &service
	}

	var unhealthyInstKey []string
	instances.Range(func(key, value interface{}) bool {
		isHealthy := false
		for discoverKey, _ := range newService {
			if key.(string) == discoverKey {
				//踢掉现有instances已存在的服务
				delete(newService, discoverKey)
				isHealthy = true
				break
			}
		}
		if !isHealthy {
			unhealthyInstKey = append(unhealthyInstKey, key.(string))
		}
		return true
	})

	//清理不健康的instances
	for _, unhealthyKey := range unhealthyInstKey {
		//再次判断unhealthyKey是否存在以应对高并发场景
		if db, ok := instances.Load(unhealthyKey); ok {
			_ = db.(*OrmDb).Close()
			instances.Delete(unhealthyKey)
			log.Logger.WithField("Type", "Orm").Debugf("Clear unhealthy sql service:%s", unhealthyKey)
		}
	}

	//添加新发现的实例
	for key, service := range newService {
		//再次判断key是否存在以应对高并发场景
		if _, ok := instances.Load(key); !ok {
			m := *config
			m.Addr = maybeIpv6(service.Address)
			m.Port = strconv.Itoa(service.Port)
			db, err := newOrmDb(&m)
			if err != nil {
				continue
			}
			db.service = *service
			instances.Store(key, db)
			log.Logger.WithField("Type", "Orm").Debugf("Add new sql service:%s", key)
		}
	}
}

func (o *OrmSql) Request(getDb GetOrmDbFunc, do OrmDo, retryTimes int, failDbs ...*OrmDb) error {
	return Orm(getDb, do, retryTimes, failDbs...)
}

func (o *OrmSql) RequestMater(do OrmDo, retryTimes int, failDbs ...*OrmDb) error {
	return Orm(o.GetMaster, do, retryTimes, failDbs...)
}

func (o *OrmSql) RequestSlave(do OrmDo, retryTimes int, failDbs ...*OrmDb) error {
	return Orm(o.GetSlave, do, retryTimes, failDbs...)
}

//插入一条数据
func (o *OrmSql) Insert(entity interface{}) error {
	return o.Request(o.GetMaster, func(db *OrmDb) error {
		return Transaction(db.DB, func(tx *gorm.DB, entity interface{}) error {
			return tx.Create(entity).Error
		}, entity)
	}, common.SqlRetryTimes)
}

//InsertMutil 一次插入多条数据
func (o *OrmSql) InsertMutil(list []interface{}) error {
	return o.Request(o.GetMaster, func(db *OrmDb) error {
		return Transaction(db.DB, func(tx *gorm.DB, list []interface{}) error {
			for _, entity := range list {
				if err := tx.Create(entity).Error; err != nil {
					return err
				}
			}
			return nil
		}, list)
	}, common.SqlRetryTimes)
}

//更新数据，所有字段都会更新,如果Id值为空或者表没有这条记录，那么将插入一条数据，此时如果主键非自增字段，则报错
//建议先查询后使用这个方法更新
func (o *OrmSql) UpdateAllFields(entity interface{}) error {
	return o.Request(o.GetMaster, func(db *OrmDb) error {
		return Transaction(db.DB, func(tx *gorm.DB, entity interface{}) error {
			return checkNotRecordError(tx.Save(entity))
		}, entity)
	}, common.SqlRetryTimes)
}

//更新数据,更新map中指定的字段
// model: 用于指定表与主键，建议设置Id值
// updateData:
// 		类型 common | map[string]interface{}
// 		如果updateData为struct，model与updateData可以相同；model与updateData必须有一个指定Id值， 否则将更新全部记录
//		如果updateData为map[string]interface{}，不允许存在为主键字段Id的key
func (o *OrmSql) Updates(model interface{}, updateData interface{}) error {
	return o.Request(o.GetMaster, func(db *OrmDb) error {
		return Transaction(db.DB, func(tx *gorm.DB, entity interface{}) error {
			return checkNotRecordError(tx.Model(entity).Updates(updateData))
		}, model)
	}, common.SqlRetryTimes)
}

//更新数据,使用组合条件更新map中单个属性
//entity 更新实体
//query 组合条件
//args 组合条件属性
//updateMap 更新属性
func (o *OrmSql) UpdatesWhere(entity interface{}, updateMap map[string]interface{}, query interface{}, args ...interface{}) error {
	return o.Request(o.GetMaster, func(db *OrmDb) error {
		return Transaction(db.DB, func(tx *gorm.DB, entity interface{}) error {
			return checkNotRecordError(tx.Model(entity).Where(query, args...).Updates(updateMap))
		}, entity)
	}, common.SqlRetryTimes)
}

//真删除一条数据
// 删除记录时，需要确保其主键字段具有值，GORM将使用主键删除记录，如果主键字段为空，GORM将删除模型的所有记录
func (o *OrmSql) Delete(entity interface{}) error {
	return o.Request(o.GetMaster, func(db *OrmDb) error {
		return Transaction(db.DB, func(tx *gorm.DB, entity interface{}) error {
			return tx.Delete(entity).Error
		}, entity)
	}, common.SqlRetryTimes)
}

//条件删除一条数据
//condition - 类型为 Struct | Map | string
//args - 当condition为string时，为condition所需参数
func (o *OrmSql) DeleteWithCondition(entity interface{}, condition interface{}, args ...interface{}) error {
	return o.Request(o.GetMaster, func(db *OrmDb) error {
		return Transaction(db.DB, func(tx *gorm.DB, entity interface{}) error {
			db := tx
			if _, ok := condition.(string); ok {
				db = db.Where(condition, args...)
			} else {
				db = db.Where(condition)
			}
			return db.Delete(entity).Error
		}, entity)
	}, common.SqlRetryTimes)
}

//假删
func (o *OrmSql) FakeDelete(entity interface{}) error {
	return o.Request(o.GetMaster, func(db *OrmDb) error {
		return Transaction(db.DB, func(tx *gorm.DB, entity interface{}) error {
			return checkNotRecordError(tx.Model(entity).Updates(map[string]interface{}{
				"update_date": time.Now(), "del_flag": 1,
			}))
		}, entity)
	}, common.SqlRetryTimes)
}

//真删除关联的数据，只支持两张表的关联删除
//entity: 要删除的记录
//entityCasade: 要关联删除的结构体
//where: entityCasade的删除条件,如: 表字段PId为entity的外键, 那么where 为 "PId = ?"
//args：条件参数值
func (o *OrmSql) DeleteCascade(entity interface{}, entityCasade interface{}, where string, args ...interface{}) error {
	return o.Request(o.GetMaster, func(db *OrmDb) error {
		return Transaction(db.DB, func(tx *gorm.DB, entity interface{}) error {
			err := tx.Delete(entity).Error
			if err != nil {
				panic(err)
			}
			return tx.Where(where, args...).Delete(entityCasade).Error
		}, entity)
	}, common.SqlRetryTimes)
}

//多对多关联删除
//entity:
// 	必须有TableName方法，many2many标签必须包含association_jointable_foreignkey与jointable_foreignkey
//	主键字段必须含有标签column
func (o *OrmSql) DeleteForMany2Many(entity interface{}, columns ...string) error {
	return o.Request(o.GetMaster, func(db *OrmDb) error {
		return Transaction(db.DB, deleteMany2Many, []interface{}{entity, columns}...)
	}, common.SqlRetryTimes)
}

//获取一条记录
//根据主键查询建议使用TryGetone或GetOneWithCondition或TryGetOneWithCondition
//如果entity主键为类型默认值，则查找第一条记录
//未找到记录，不会返回RecordNotFound的错误
func (o *OrmSql) GetOne(entity interface{}) error {
	err := o.TryGetOne(entity)
	if gorm.IsRecordNotFoundError(err) {
		return nil
	}
	return err
}

//查询一条记录
//如果未找到，则返回RecordNotFound的错误
func (o *OrmSql) TryGetOne(entity interface{}) error {
	return o.Request(o.GetSlave, func(db *OrmDb) error {
		return db.First(entity).Error
	}, common.SqlRetryTimes)
}

//查询一条记录
//如果未找到，则返回RecordNotFound的错误
func (o *OrmSql) TryGetOneWithSelect(entity interface{}, columns interface{}) error {
	return o.Request(o.GetSlave, func(db *OrmDb) error {
		return db.Select(columns).First(entity).Error
	}, common.SqlRetryTimes)
}

//级联查询一条记录
//未找到记录，不会返回RecordNotFound的错误
func (o *OrmSql) GetOnePreload(entity interface{}, columns ...string) error {
	err := o.TryGetOnePreload(entity, columns...)
	if gorm.IsRecordNotFoundError(err) {
		return nil
	}
	return err
}

//级联查询一条记录
//如果未找到，则返回RecordNotFound的错误
func (o *OrmSql) TryGetOnePreload(entity interface{}, columns ...string) error {
	return o.Request(o.GetSlave, func(db *OrmDb) error {
		s := db.DB
		if len(columns) > 0 {
			for i := range columns {
				s = s.Preload(columns[i])
			}
		}
		return s.First(entity).Error
	}, common.SqlRetryTimes)
}

//级联查询一条记录
//如果未找到，则返回RecordNotFound的错误
func (o *OrmSql) TryGetOnePreloadSelect(entity interface{}, columns []string, sel interface{}) error {
	return o.Request(o.GetSlave, func(db *OrmDb) error {
		s := db.DB
		if len(columns) > 0 {
			for i := range columns {
				s = s.Preload(columns[i])
			}
		}
		if sel != nil {
			s = s.Select(sel)
		}
		return s.First(entity).Error
	}, common.SqlRetryTimes)
}

//条件查询一条记录
//condition - 类型为 Struct | Map | string
//args - 当condition为string时，为condition所需参数
//未找到记录，不会返回RecordNotFound的错误
func (o *OrmSql) GetOneWithCondition(out interface{}, condition interface{}, args ...interface{}) error {
	return o.GetOneWithConditionPreload(out, nil, condition, args...)
}

//条件查询一条记录
//condition - 类型为 Struct | Map | string
//args - 当condition为string时，为condition所需参数
//如果未找到，则返回RecordNotFound的错误
func (o *OrmSql) TryGetOneWithCondition(out interface{}, condition interface{}, args ...interface{}) error {
	return o.TryGetOneWithConditionPreload(out, nil, condition, args...)
}

//条件查询一条记录,并关联查询
//fields - 预加载字段
//condition - 类型为 Struct | Map | string
//args - 当condition为string时，为condition所需参数
//未找到记录，不会返回RecordNotFound的错误
func (o *OrmSql) GetOneWithConditionPreload(out interface{}, fields []string, condition interface{}, args ...interface{}) error {
	err := o.TryGetOneWithConditionPreload(out, fields, condition, args...)
	if gorm.IsRecordNotFoundError(err) {
		return nil
	}
	return err
}

//条件查询一条记录,并关联查询
//fields - 预加载字段
//condition - 类型为 Struct | Map | string
//args - 当condition为string时，为condition所需参数
//如果未找到，则返回RecordNotFound的错误
func (o *OrmSql) TryGetOneWithConditionPreload(out interface{}, fields []string, condition interface{}, args ...interface{}) error {
	if out == nil {
		return errors.New("parameter out is nil")
	}
	return o.Request(o.GetSlave, func(db *OrmDb) error {
		s := db.DB
		if len(fields) != 0 {
			for i := range fields {
				s = s.Preload(fields[i])
			}
		}
		if condition != nil {
			if _, ok := condition.(string); ok {
				s = s.Where(condition, args...)
			} else {
				s = s.Where(condition)
			}
		}
		return s.First(out).Error
	}, common.SqlRetryTimes)
}

//条件查询一条记录,并关联查询
//fields - 预加载字段
//sel - 指定查询表字段
//condition - 类型为 Struct | Map | string
//args - 当condition为string时，为condition所需参数
//如果未找到，则返回RecordNotFound的错误
func (o *OrmSql) TryGetOneWithSelectConditionPreload(out interface{}, sel interface{}, fields []string, condition interface{}, args ...interface{}) error {
	if out == nil {
		return errors.New("parameter out is nil")
	}
	return o.Request(o.GetSlave, func(db *OrmDb) error {
		s := db.DB
		if len(fields) != 0 {
			for i := range fields {
				s = s.Preload(fields[i])
			}
		}
		if sel != nil {
			s = s.Select(sel)
		}
		if condition != nil {
			if _, ok := condition.(string); ok {
				s = s.Where(condition, args...)
			} else {
				s = s.Where(condition)
			}
		}
		return s.First(out).Error
	}, common.SqlRetryTimes)
}

//获取所有记录
func (o *OrmSql) GetAll(entity interface{}) error {
	return o.Request(o.GetSlave, func(db *OrmDb) error {
		return checkNotRecordError(db.Find(entity))
	}, common.SqlRetryTimes)
}

//获取所有记录
func (o *OrmSql) GetAllWithSelect(entity interface{}, sel string) error {
	return o.GetAllWithSelectAndOrder(entity, sel, "")
}

//获取所有记录
func (o *OrmSql) GetAllWithSelectAndOrder(entity interface{}, sel string, order string) error {
	return o.Request(o.GetSlave, func(db *OrmDb) error {
		s := db.DB
		if sel != "" {
			s = s.Select(sel)
		}
		if order != "" {
			s = s.Order(order)
		}
		return checkNotRecordError(s.Find(entity))
	}, common.SqlRetryTimes)
}

//关联联查询所有记录
func (o *OrmSql) GetAllPreload(entity interface{}, columns ...string) error {
	return o.Request(o.GetSlave, func(db *OrmDb) error {
		s := db.DB
		if len(columns) > 0 {
			for i := range columns {
				s = s.Preload(columns[i])
			}
		}
		return checkNotRecordError(s.Find(entity))
	}, common.SqlRetryTimes)
}

//条件查询所有记录
//condition类型为 Struct | Map | string
//args - 当condition为string时，为condition所需参数
func (o *OrmSql) GetAllWithCondition(out interface{}, condition interface{}, args ...interface{}) error {
	return o.GetAllWithConditionPreload(out, nil, condition, args...)
}

//条件查询所有记录
//condition类型为 Struct | Map | string
//args - 当condition为string时，为condition所需参数
func (o *OrmSql) TryGetAllWithCondition(out interface{}, condition interface{}, args ...interface{}) error {
	return o.TryGetAllWithConditionPreload(out, nil, condition, args...)
}

//条件关联查询所有记录
//out - 查询结果
//fields - 预加载字段
//condition - 查询条件 Struct & Map & string
//args - 当condition为string时，使用Plain SQL方式查询，args为condition所需参数
func (o *OrmSql) GetAllWithConditionPreload(out interface{}, fields []string, condition interface{}, args ...interface{}) error {
	if out == nil {
		return errors.New("parameter out is nil")
	}
	return o.Request(o.GetSlave, func(db *OrmDb) error {
		s := db.DB
		if len(fields) != 0 {
			for i := range fields {
				s = s.Preload(fields[i])
			}
		}
		if condition != nil {
			if _, ok := condition.(string); ok {
				s = s.Where(condition, args...)
			} else {
				s = s.Where(condition)
			}
		}
		return checkNotRecordError(s.Find(out))
	}, common.SqlRetryTimes)
}

func (o *OrmSql) TryGetAllWithConditionPreload(out interface{}, fields []string, condition interface{}, args ...interface{}) error {
	if out == nil {
		return errors.New("parameter out is nil")
	}
	return o.Request(o.GetSlave, func(db *OrmDb) error {
		s := db.DB
		if len(fields) != 0 {
			for i := range fields {
				s = s.Preload(fields[i])
			}
		}
		if condition != nil {
			if _, ok := condition.(string); ok {
				s = s.Where(condition, args...)
			} else {
				s = s.Where(condition)
			}
		}
		return s.Find(out).Error
	}, common.SqlRetryTimes)
}

//分页查询
//page - 分页实例，建议在此之前先给page.Rows指定值
//out - 结果列表
//fields - 查询字段，字段间用逗号隔开
//condition - 查询条件 common | map
func (o *OrmSql) GetAllByPage(page *MysqlOrmPage, fields string, condition interface{}) error {
	return o.GetAllWithOrderByPage(page, fields, condition, nil)
}

//分页查询条件为 condition and  plainCond
//page - 分页实例，建议在此之前先给page.Rows指定值
//out - 结果列表
//fields - 查询字段，字段间用逗号隔开
//order - 排序，格式："name DESC"
//condition - 查询条件 struct | map
//plainCond - 直接查询条件 处理>, <, >=, <=等范围查询，与condition为and关系
//args - plainCond的参数
func (o *OrmSql) GetAllWithOrderByPageAndMutiCond(page *MysqlOrmPage, fields string, condition interface{}, order interface{}, plainCond string, args ...interface{}) error {
	offset := (page.GetPageNum() - 1) * page.GetPageSize()
	limit := page.GetPageSize()
	return o.Request(o.GetSlave, func(db *OrmDb) error {
		s := db.DB
		if fields != "" {
			s = s.Select(fields)
		}
		if condition != nil {
			s = s.Where(condition)
		}
		if plainCond != "" {
			s = s.Where(plainCond, args...)
		}
		if order != nil {
			s = s.Order(order)
		}
		return checkNotRecordError(s.Offset(offset).Limit(limit).Find(page.Rows).
			Limit(-1).Offset(-1).Count(&page.TotalCount))
	}, common.SqlRetryTimes)
}

//分页查询
//page - 分页实例，建议在此之前先给page.Rows指定值
//out - 结果列表
//fields - 查询字段，字段间用逗号隔开,*或为空表示查询全部
//condition - 查询条件 common | map
//order - 排序，格式："name DESC"
func (o *OrmSql) GetAllWithOrderByPage(page *MysqlOrmPage, fields string, condition interface{}, order interface{}) error {
	return o.GetAllWithOrderByPageAndMutiCond(page, fields, condition, order, "", nil)
}

/*
func (o *OrmSqlDb) GetAllWithOrderByPage(page *MysqlOrmPage, fields string, condition interface{}, order interface{}) error {
	offset := (page.GetPageNum() - 1) * page.GetPageSize()
	limit := page.GetPageSize()
	db := o.Slave
	if fields != "" {
		db = db.Select(fields)
	}
	if condition != nil {
		db = db.Where(condition)
	}
	if order != nil {
		db = db.Order(order)
	}
	return checkNotRecordError(db.Offset(offset).Limit(limit).Find(page.Rows).
		Limit(-1).Offset(-1).Count(&page.TotalCount))
}
*/

//查询
//参数：
//	getOne: true获取第一个查询到的记录
//	out：查询结果, 指针类型
//	preload: 需要预加载数据的字段
//	selectColumn: 指定要获取的表字段, string或[]string类型
//	orderBy: 排序
//	condition: where条件, string|common|map类型
//	conditionArgs: where条件中占位符?所对应的参数
func (o *OrmSql) FindWithPreloadSelectConditionOrderBy(getOne bool, out interface{}, preload []string, selectColumn interface{},
	orderBy string, condition interface{}, conditionArgs ...interface{}) error {
	return o.Request(o.GetSlave, func(db *OrmDb) error {
		s := db.DB
		if len(preload) != 0 {
			for i := range preload {
				s = s.Preload(preload[i])
			}
		}
		if selectColumn != nil {
			s = s.Select(selectColumn)
		}
		if condition != nil {
			s = s.Where(condition, conditionArgs...)
		}
		if orderBy != "" {
			s = s.Order(orderBy)
		}
		if getOne {
			return s.First(out).Error
		}
		return s.Find(out).Error
	}, common.SqlRetryTimes)
}

//检查空记录错误
func checkNotRecordError(res *gorm.DB) error {
	if gorm.IsRecordNotFoundError(res.Error) {
		return nil
	}
	return res.Error
}

func deleteMany2Many(tx *gorm.DB, entity interface{}, fields []string) error {
	t := reflect.TypeOf(entity)
	v := reflect.ValueOf(entity)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		v = v.Elem()
	}

	if t.Kind() != reflect.Struct {
		return errors.New("entity must be type common")
	}

	info, err := getTableInfo(t, v, true)
	if err != nil {
		return err
	}
	//删除记录
	sql := fmt.Sprintf("delete from %s where %s=?", info["table"], info["column"])
	err = tx.Exec(sql, info["value"]).Error
	if err != nil {
		return err
	}

	if len(fields) == 0 {
		return nil
	}

	//获取属性字段columns主键值
	for i := range fields {
		ct, ok := t.FieldByName(fields[i])
		if !ok {
			//字段不存在，报错
			return errors.New(fmt.Sprintf("field %s does not exist", fields[i]))
		}
		cv := v.FieldByName(fields[i])
		if !cv.IsValid() {
			//值为空，忽略
			continue
		}
		if cv.Kind() != reflect.Slice {
			//不是slice类型，报错
			return errors.New(fmt.Sprintf("field %s must be type slice", fields[i]))
		}

		//获取中间表名
		middleTableName := getTagItem(ct, "MANY2MANY", "")
		if middleTableName == "" {
			return errors.New("many2many tag does not exist")
		}
		ajf := getTagItem(ct, "ASSOCIATION_JOINTABLE_FOREIGNKEY", "")
		if ajf == "" {
			return errors.New("association_jointable_foreignkey tag does not exist")
		}

		jf := getTagItem(ct, "JOINTABLE_FOREIGNKEY", "")
		if jf == "" {
			return errors.New("jointable_foreignkey tag does not exist")
		}

		//获取关联表主键字段值列表
		var assInfo map[string]interface{}
		var fIds []interface{}
		for j := 0; j < cv.Len(); j++ {
			if j != 0 {
				fIds = append(fIds, (cv.Index(j).FieldByName(assInfo["field"].(string))).String())
				continue
			}
			assInfo, err = getTableInfo(reflect.TypeOf(cv.Index(j).Interface()), cv.Index(j), true)
			if err != nil {
				return err
			}
			fIds = append(fIds, assInfo["value"])
		}

		//删除中间表数据
		sql = fmt.Sprintf("delete from %s where %s=? and %s in (?)",
			middleTableName, jf, ajf)
		err = tx.Exec(sql, info["value"], fIds).Error
		if err != nil {
			return err
		}
		//删除关联表数据
		sql = fmt.Sprintf("delete from %s where %s in (?)",
			assInfo["table"], assInfo["column"])
		err = tx.Exec(sql, fIds).Error
		if err != nil {
			return err
		}
	}

	return nil
}

//获取主键字段值
func getTableInfo(t reflect.Type, v reflect.Value, searchTableName bool) (map[string]interface{}, error) {
	var info map[string]interface{}
	var pK reflect.StructField
	var value reflect.Value
	var otherPK reflect.StructField
	var otherValue reflect.Value
	fs := t.NumField()
	for i := 0; i < fs; i++ {
		if v.Field(i).Kind() == reflect.Struct {
			//内嵌结构体取主键值
			info, err := getTableInfo(t.Field(i).Type, v.Field(i), false)
			if err != nil {
				return nil, err
			}
			r, err := getTableName(t, v)
			if err != nil {
				return nil, err
			}
			info["table"] = r
			return info, nil
		}

		//tag中包含PRIMARY_KEY或者字段名为ID的字段就是主键字段
		tag := t.Field(i).Tag.Get("gorm")
		if strings.Contains(strings.ToUpper(tag), "PRIMARY_KEY") {
			info = make(map[string]interface{})
			pK = t.Field(i)
			value = v.Field(i)
			break
		}
		if strings.ToUpper(t.Field(i).Name) == "ID" {
			otherPK = t.Field(i)
			otherValue = v.Field(i)
		}
	}
	if info != nil {
		info["value"] = value.Interface()
		info["column"] = getTagItem(pK, "column", "id")
		info["field"] = pK.Name

	} else if otherPK.Name != "" {
		info = map[string]interface{}{
			"value":  otherValue.Interface(),
			"column": getTagItem(otherPK, "column", "id"),
			"field":  otherPK.Name,
		}
	}
	if info == nil {
		return nil, errors.New(fmt.Sprintf("the %s primary key does not exist", t.Name()))
	}
	if searchTableName {
		r, err := getTableName(t, v)
		if err != nil {
			return nil, err
		}
		info["table"] = r
	}
	return info, nil
}

func getTagItem(sf reflect.StructField, key string, def string) string {
	rT := def
	tag := sf.Tag.Get("gorm")
	if tag == "" {
		return rT
	}
	if !strings.Contains(strings.ToUpper(tag), key) {
		return rT
	}
	tags := strings.Split(tag, ";")
	for _, st := range tags {
		k := strings.Split(st, ":")
		if strings.ToUpper(k[0]) == key {
			rT = k[1]
			break
		}
	}
	return rT
}

func getTableName(t reflect.Type, v reflect.Value) (string, error) {
	method := v.MethodByName("TableName")
	if method.IsValid() {
		return method.Call([]reflect.Value{})[0].String(), nil
	}
	return "", errors.New(fmt.Sprintf("%s's method TableName() doesn't exist", t.Name()))
}
