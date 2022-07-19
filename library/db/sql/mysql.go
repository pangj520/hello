/**
 *描述：mysql客户端
 *      实现以mysql driver的形式连接mysql集群的客户端
 *      向其他文件提供原生sql的执行方法
 *作者：江洪
 *时间：2019-5-20
 */

package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"library_Test/common"
	"library_Test/library/log"
	"library_Test/library/servicediscovery"
	"strconv"
	"strings"
	"sync"
	"time"
)

type StmtSql struct {
	//Deprecated
	//请使用GetMaster()获取
	Master *StmtDb
	//Deprecated
	//请使用GetSlave()获取
	Slave           *StmtDb
	MasterConfig    *MysqlConfig
	SlaveConfig     *MysqlConfig
	master          *StmtDb
	slave           *StmtDb
	masterInstances *sync.Map //key:{数据库地址}:{数据库端口}，value: *StmtDb
	slaveInstances  *sync.Map //key:{数据库地址}:{数据库端口}，value: *StmtDb
}

type StmtDb struct {
	*sql.DB
	service servicediscovery.Service
}

/**
 *新建数据库连接，为sql执行获得连接实例
 * 输入：
 *       c - 数据库连接配置
 * 返回： 数据库连接对象
 */
func NewStmtSql(m, s *MysqlConfig) (*StmtSql, error) {
	stmtDb := &StmtSql{MasterConfig: m, SlaveConfig: s}
	err := stmtDb.init("master", m)
	if err != nil {
		return nil, err
	}
	err = stmtDb.init("slave", s)
	if err != nil {
		return nil, err
	}
	return stmtDb, nil
}

func newStmtDb(c *MysqlConfig) (*StmtDb, error) {
	err := check(c)
	if err != nil {
		return nil, err
	}
	Dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?multiStatements=true&autocommit=true", c.UserName, c.Password, c.Addr, c.Port, c.Database)
	db, err := sql.Open("mysql", Dsn)
	if err != nil {
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		return nil, err
	}
	if !c.DisableLog {
		err = mysql.SetLogger(log.Logger.WithField("source", "mysql"))
		if err != nil {
			return nil, err
		}
	}
	//连接最大存活时间，该时间过期后，连接将被复用
	var connMaxLifetime time.Duration
	if c.ConnMaxLifetime.Seconds() != 0 {
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
	db.SetConnMaxLifetime(connMaxLifetime)
	db.SetMaxIdleConns(maxIdleConns)
	db.SetMaxOpenConns(maxOpenConns)
	return &StmtDb{DB: db}, nil
}

func (s *StmtSql) init(t string, c *MysqlConfig) error {
	if !c.UseDiscovery {
		db, err := newStmtDb(c)
		if err != nil {
			return err
		}
		if t == "master" {
			s.master = db
			s.Master = s.master
		} else {
			s.slave = db
			s.Slave = s.slave
		}
		return nil
	}
	if c.Discovery == nil {
		return errors.New("when UseDiscovery = true, Discovery cannot be nil")
	}
	if c.ServiceName == "" {
		return errors.New("when UseDiscovery = true, ServiceName cannot be empty")
	}
	service, err := c.Discovery.ServiceDiscover(c.ServiceName, &servicediscovery.ServiceFilterOptions{
		Tags: c.Tags,
	})
	if err != nil {
		return err
	}
	if len(service) == 0 {
		return ErrNoServiceAlive
	}
	var instances *sync.Map
	if t == "master" {
		s.masterInstances = &sync.Map{}
		instances = s.masterInstances
	} else {
		s.slaveInstances = &sync.Map{}
		instances = s.slaveInstances
	}
	for _, s := range service {
		m := *c
		m.Addr = maybeIpv6(s.Address)
		m.Port = strconv.Itoa(s.Port)
		key := fmt.Sprintf("%s:%s", m.Addr, m.Port)
		log.Logger.WithField("Stmt", t).Debug("sql service ", key, " was found")
		db, err := newStmtDb(&m)
		if err != nil {
			return err
		}
		db.service = s
		instances.Store(key, db)
	}
	return nil
}

func (s *StmtSql) Close() error {
	if !s.MasterConfig.UseDiscovery {
		err := s.master.Close()
		if err != nil {
			return err
		}
	} else {
		s.masterInstances.Range(func(key, value interface{}) bool {
			_ = value.(*StmtDb).Close()
			return true
		})
	}
	if !s.SlaveConfig.UseDiscovery {
		err := s.master.Close()
		if err != nil {
			return err
		}
		return nil
	} else {
		s.slaveInstances.Range(func(key, value interface{}) bool {
			_ = value.(*StmtDb).Close()
			return true
		})
	}
	return nil
}

func (s *StmtSql) Request(getDb GetStmtDbFunc, do StmtDo, retryTimes int, failDbs ...*StmtDb) error {
	return Stmt(getDb, do, retryTimes, failDbs...)
}

func (s *StmtSql) RequestMaster(do StmtDo, retryTimes int, failDbs ...*StmtDb) error {
	return Stmt(s.GetMaster, do, retryTimes, failDbs...)
}

func (s *StmtSql) RequestSlave(do StmtDo, retryTimes int, failDbs ...*StmtDb) error {
	return Stmt(s.GetSlave, do, retryTimes, failDbs...)
}

//如果启用服务发现，当找不到任何健康sql服务时，返回error
func (s *StmtSql) GetMaster(failDbs ...*StmtDb) (*StmtDb, error) {
	if !s.MasterConfig.UseDiscovery {
		return s.master, nil
	}
	return s.discoveryOne(s.masterInstances, s.MasterConfig, getExcludedServices2(failDbs...)...)
}

//获取不再被选中的sql服务
func getExcludedServices2(failDbs ...*StmtDb) []servicediscovery.Service {
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
func (s *StmtSql) GetSlave(failDbs ...*StmtDb) (*StmtDb, error) {
	if !s.SlaveConfig.UseDiscovery {
		return s.slave, nil
	}
	return s.discoveryOne(s.slaveInstances, s.SlaveConfig, getExcludedServices2(failDbs...)...)
}

func (s *StmtSql) discoveryOne(instances *sync.Map, config *MysqlConfig, excluded ...servicediscovery.Service) (*StmtDb, error) {
	filterOpts := &servicediscovery.ServiceFilterOptions{
		Tags:          config.Tags,
		UseSelectMode: true,
		SelectMode:    config.SelectMode,
	}
	if len(excluded) > 0 {
		filterOpts.ExcludedServices = excluded
	}
	service, err := config.Discovery.ServiceDiscover(config.ServiceName, filterOpts)
	if err != nil {
		return nil, err
	}
	if len(service) == 0 {
		return nil, ErrNoServiceAlive
	}
	addr := service[0].Address
	addr = maybeIpv6(addr)
	port := service[0].Port
	key := fmt.Sprintf("%s:%d", addr, port)
	log.Logger.WithField("Type", "Stmt").Debug("select sql service:", key)
	v, ok := instances.Load(key)
	if ok {
		db := v.(*StmtDb)
		db.service = service[0]
		return db, nil
	}
	m := *config
	m.Addr = addr
	m.Port = strconv.Itoa(port)
	db, err := newStmtDb(&m)
	if err != nil {
		return nil, err
	}
	db.service = service[0]
	instances.Store(key, db)
	return db, nil
}

//增改删一条数据
//参数：
//	query - sql语句
//	timeout - 查询超时时间，默认1秒
//	values - 预编译参数值
//返回：
//	error - 错误
func (s *StmtSql) CUDStatement(query string, timeout time.Duration, values ...interface{}) error {
	return s.Request(s.GetMaster, func(db *StmtDb) error {
		return Transaction(db.DB, func(tx *sql.DB) error {
			if query == "" {
				return errors.New("query cannot be empty")
			}
			if timeout == time.Second*0 {
				timeout = time.Second
			}
			ctx, _ := context.WithTimeout(context.Background(), timeout)
			stm, err := tx.PrepareContext(ctx, query)
			if err != nil {
				return err
			}
			_, err = stm.ExecContext(ctx, values...)
			return err
		})
	}, common.SqlRetryTimes)
}

//查询
//参数：
//	query - sql语句
//	it - 查询结果迭代函数，对查询结果中的每一条记录执行该函数
//	timeout - 查询超时时间，默认3秒
//	args - 预编译参数值
//返回：
//	[]interface{} - 函数it执行结果集
//	error - 错误
func (s *StmtSql) QueryStatement(query string, it func(rows *sql.Rows) (interface{}, error),
	timeout time.Duration, args ...interface{}) ([]interface{}, error) {
	if query == "" {
		return nil, errors.New("query cannot be empty")
	}
	if timeout == time.Second*0 {
		timeout = time.Second * 3
	}
	var records []interface{}
	err := s.Request(s.GetMaster, func(db *StmtDb) error {
		ctx, _ := context.WithTimeout(context.Background(), timeout)
		stmt, err := db.DB.PrepareContext(ctx, query)
		if err != nil {
			return err
		}
		var rows *sql.Rows
		var temp []interface{}
		rows, err = stmt.QueryContext(ctx, args...)
		if err != nil {
			return err
		}
		for rows.Next() {
			var record interface{}
			record, err = it(rows)
			if err != nil {
				return err
			}
			temp = append(temp, record)
		}
		records = temp
		return nil
	}, common.SqlRetryTimes)
	return records, err
}

//分页查询
//参数：
//	query - sql语句
//	page - 分页参数
//	timeout - 查询超时时间，默认3秒
//	args - 预编译参数值
//返回：
//	error - 错误
func (s *StmtSql) QueryStatementByPage(query string, page *MysqlStatementPage,
	timeout time.Duration, args ...interface{}) error {
	if query == "" {
		return errors.New("query cannot be empty")
	}
	if timeout == time.Second*0 {
		timeout = time.Second * 3
	}
	return s.Request(s.GetMaster, func(db *StmtDb) error {
		ctx, _ := context.WithTimeout(context.Background(), timeout)
		i := strings.Index(query, "from")
		countQuery := "select count(*) " + query[i:]
		stmt, err := db.DB.PrepareContext(ctx, countQuery)
		if err != nil {
			return err
		}
		var rows *sql.Rows
		rows, err = stmt.QueryContext(ctx, args...)
		if err != nil {
			return err
		}
		var count int64
		for rows.Next() {
			err = rows.Scan(&count)
			if err != nil {
				return nil
			}
			break
		}

		if count == 0 {
			return nil
		}
		page.Rows = []interface{}{}
		page.TotalCount = count
		start := (page.PageNum - 1) * page.PageSize
		size := page.PageSize
		limitQuery := fmt.Sprintf("%s limit %d,%d", query, start, size)
		stmt, err = db.DB.PrepareContext(ctx, limitQuery)
		if err != nil {
			return err
		}

		rows, err = stmt.QueryContext(ctx, args...)
		if err != nil {
			return err
		}
		for rows.Next() {
			var record interface{}
			record, err = page.It(rows)
			if err != nil {
				return err
			}
			page.Rows = append(page.Rows, record)
		}
		return nil
	}, common.SqlRetryTimes)
}

//Count统计
//参数：
//	query - sql语句
//	timeout - 查询超时时间，默认3秒
//	args - 预编译参数值
//返回：
//	int64 - 统计结果
//	error - 错误
func (s *StmtSql) CountStatement(query string, timeout time.Duration, args ...interface{}) (int64, error) {
	if query == "" {
		return 0, errors.New("query cannot be empty")
	}
	if timeout == time.Second*0 {
		timeout = time.Second * 3
	}
	var count int64
	err := s.Request(s.GetMaster, func(db *StmtDb) error {
		ctx, _ := context.WithTimeout(context.Background(), timeout)
		stmt, err := db.DB.PrepareContext(ctx, query)
		if err != nil {
			return err
		}
		var rows *sql.Rows
		rows, err = stmt.QueryContext(ctx, args...)
		if err != nil {
			return err
		}
		for rows.Next() {
			err = rows.Scan(&count)
			if err != nil {
				return nil
			}
			break
		}
		return err
	}, common.SqlRetryTimes)
	return count, err
}
