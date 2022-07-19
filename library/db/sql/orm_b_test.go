package sql

import (
	"github.com/jinzhu/gorm"
	"gotest.tools/assert"
	"library_Test/library/log"
	"runtime"
	"sync"
	"testing"
)

type Test struct {
	Name string `gorm:"column:name"`
}

func BenchmarkTransaction(b *testing.B) {
	_ = log.InitLog(log.CreateDefaultLoggerConfig())
	config := &MysqlConfig{
		Addr:            "192.168.37.57",
		UserName:        "nef",
		Password:        "SQnfNlLyqteJLvg7",
		Port:            "6033",
		Database:        "NEFDB",
		MaxIdleConns:    runtime.NumCPU() * 10,
		ConnMaxLifetime: -1,
	}
	db, err := NewOrmSql(config, config)
	assert.NilError(b, err)
	b.N = 10000

	b.ResetTimer()
	for i := 0; i < b.N; i++ { // b.N，测试循环次数
		err = Transaction(db, func(tx *gorm.DB) error {
			//return nil
			return tx.Create(&Test{Name: "a"}).Error
		})
		assert.NilError(b, err)
		//tx := db.Begin()
		//f := func() {}
		//f()
		//tx.Commit()
	}

	//wg := sync.WaitGroup{}
	//sig := make(chan bool)
	//fmt.Println(b.N)
	//for i := 0; i < b.N; i++ { // b.N，测试循环次数
	//	wg.Add(1)
	//	go tx(b, db, sig, &wg)
	//	//go get(b, rc, sig, &wg)
	//}
	//s := time.Now()
	//b.ResetTimer()
	//close(sig)
	//wg.Wait()
	//w := time.Now().Sub(s)
	//fmt.Println(w.Seconds())
}

func tx(b *testing.B, db *gorm.DB, sig chan bool, wg *sync.WaitGroup) {
	defer wg.Done()
	<-sig
	_ = Transaction(db, func(tx *gorm.DB) error {
		return nil
		//return tx.Create(&Test{Name:"a"}).Error
	})
}
