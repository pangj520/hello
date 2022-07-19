package sql

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/jinzhu/gorm"
	"reflect"
	"strings"
	"time"
	"library_Test/library/log"
	"library_Test/library/servicediscovery"
	"library_Test/library/utils"
)

type SqlDb struct {
	//Deprecated
	Stmt *StmtSql
	Orm  *OrmSql
}

func (db *SqlDb) Close() error {
	//err := db.Stmt.Close()
	//if err != nil {
	//	return err
	//}
	err := db.Orm.Close()
	if err != nil {
		return err
	}
	return nil
}

// Config mysql config.
type MysqlConfig struct {
	Addr            string `check:"required"`
	UserName        string `check:"required"`
	Password        string `check:"required"`
	Port            string `check:"required"`
	Database        string `check:"required"`
	ConnMaxLifetime time.Duration
	MaxIdleConns    int
	MaxOpenConns    int
	DisableLog      bool

	UseDiscovery bool
	Discovery    servicediscovery.ServiceDiscovery
	ServiceName  string
	Tags         []string
	SelectMode   servicediscovery.SelectMode
}

type MysqlOrmPage struct {
	utils.Page
	Rows interface{} //数据记录
}

type MysqlStatementPage struct {
	utils.Page
	Rows []interface{}                             //数据记录
	It   func(rows *sql.Rows) (interface{}, error) //操作每一条查询结果
}

type GetOrmDbFunc func(failDbs ...*OrmDb) (*OrmDb, error)
type GetStmtDbFunc func(failDbs ...*StmtDb) (*StmtDb, error)

type OrmDo func(db *OrmDb) error
type StmtDo func(db *StmtDb) error

/**
 *事务控制，一旦出现错误则回滚，会检查操作函数f的返回值，如果error不为空也回滚
 *参数：
 *	db: 数据库对象
 *	f: 操作数据库的函数, 函数的第一个参数类型必须为*gorm.DB或*sql.Tx
 *	args: 函数f的参数
 *返回：
 *  error: 参数类型有误或事务异常
 */
func Transaction(db interface{}, f interface{}, args ...interface{}) (err error) {
	t := reflect.ValueOf(f)
	switch t.Kind() {
	case reflect.Func:
	default:
		return errors.New("f must be of type Func")
	}

	var tx interface{}
	var isOrm bool
	tx, isOrm, err = begin(db)
	if err != nil {
		return
	}

	params := make([]reflect.Value, len(args)+1)
	params[0] = reflect.ValueOf(tx)
	for i := range args {
		params[i+1] = reflect.ValueOf(args[i])
	}

	//捕获异常与处理
	defer func() {
		pError := recover()
		if pError != nil {
			err = pError.(error)
			cErr := rollback(tx, isOrm)
			if cErr != nil {
				log.Logger.Error("rollback failed, err: ", cErr)
			}
			return
		}
		cErr := commit(tx, isOrm)
		if cErr != nil {
			log.Logger.Error("commit failed, err: ", cErr)
			return
		}
	}()

	v := t.Call(params)
	if len(v) == 0 {
		return
	}
	for i := range v {
		if _, ok := v[i].Interface().(error); !ok {
			continue
		}
		if !v[i].IsNil() {
			panic(v[i].Interface())
		}
	}
	return
}

//开启事务
func begin(db interface{}) (interface{}, bool, error) {
	switch db.(type) {
	case *OrmDb:
		tx := db.(*OrmDb).Begin()
		return tx, true, tx.Error
	case *gorm.DB:
		tx := db.(*gorm.DB).Begin()
		return tx, true, tx.Error
	case *StmtDb:
		tx, err := db.(*StmtDb).Begin()
		return tx, false, err
	case *sql.DB:
		tx, err := db.(*sql.DB).Begin()
		return tx, false, err
	default:
		return nil, false, errors.New("db must be of type *gorm.DB or *sql.DB")
	}
}

//提交事务
func commit(tx interface{}, isOrm bool) error {
	if isOrm {
		return tx.(*gorm.DB).Commit().Error
	}
	return tx.(*sql.Tx).Commit()
}

//事务回滚
func rollback(tx interface{}, isOrm bool) error {
	if isOrm {
		return tx.(*gorm.DB).Rollback().Error
	}
	return tx.(*sql.Tx).Rollback()
}

func check(conf *MysqlConfig) error {
	err := utils.CheckRequiredStringField("MysqlConfig", conf)
	if err != nil {
		return err
	}
	return nil
}

//使用ORM操作数据库，操作失败会进行重试
//	输入:
//		ctx: 上下文
//		getDb: 获取ORM DB实例的方法
//		do: 如何对数据库进行操作的方法，方法第一个参数为*OrmDb
//		failDbs: 操作失败的*OrmDb，这些*OrmDb在重试过程中不会被再次使用
//	输出:
//		异常
func Orm(getDb GetOrmDbFunc, do OrmDo, retryTimes int, failDbs ...*OrmDb) error {
	db, err := getDb(failDbs...)
	if err != nil {
		return err
	}
	err = do(db)
	if err == nil {
		return nil
	}
	//判断是否需要重试
	if !maybeRetry(err) {
		return err
	}
	if retryTimes < 1 {
		return err
	}
	retryTimes--
	fmt.Println("retry times:", len(failDbs)+1)
	//此次失败的db不再被选中
	failDbs = append(failDbs, db)
	nErr := Orm(getDb, do, retryTimes, failDbs...)
	if nErr != nil && err == ErrNoServiceAlive {
		//返回导致需要重试的错误
		return err
	}
	return nErr
}

//使用Stmt操作数据库，操作失败会进行重试
//	输入:
//		getDb: 获取Stmt DB实例的方法
//		do: 如何对数据库进行操作的方法，方法第一个参数为*StmtDb
//		retryTimes: 重试次数，默认为0，不进行重试
//		failDbs: 操作失败的*StmtDb，这些*StmtDb在重试过程中不会被再次使用
//	输出:
//		异常
func Stmt(getDb GetStmtDbFunc, do StmtDo, retryTimes int, failDbs ...*StmtDb) error {
	//span := log.StartSpanWithParentContext("Stmt", ctx)
	//defer log.FinishJaegerSpan(span)
	db, err := getDb(failDbs...)
	if err != nil {
		return err
	}
	err = do(db)
	if err == nil {
		return nil
	}
	//判断是否需要重试
	if !maybeRetry(err) {
		return err
	}
	if retryTimes < 1 {
		return err
	}
	retryTimes--
	fmt.Println("retry times:", len(failDbs)+1)
	//此次失败的db不再被选中
	failDbs = append(failDbs, db)
	nErr := Stmt(getDb, do, retryTimes, failDbs...)
	if nErr != nil && err == ErrNoServiceAlive {
		//返回导致需要重试的错误
		return err
	}
	return nErr
}

func maybeIpv6(addr string) string {
	if strings.Contains(addr, ":") {
		if strings.HasPrefix(addr, "]") {
			return addr
		}
		return "[" + addr + "]"
	}
	return addr
}