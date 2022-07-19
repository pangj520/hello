package sql

import (
	"gotest.tools/assert"
	"testing"
	"library_Test/library/log"
	"library_Test/library/servicediscovery"
	"library_Test/library/servicediscovery/consul"
)

var (
	consulAddr = "http://172.20.101.15:35926"
	datacenter = "guangzhou_dc1"
	token      = "db02cbec-90d7-d525-626e-c70c6e5542cf"
)

func TestNewOrmSqlDb(t *testing.T) {
	err := log.InitLog(log.CreateDefaultLoggerConfig())
	assert.NilError(t, err)
	discovery, err := consul.NewConsulClient(consulAddr, consul.DataCenterOption(datacenter), consul.TokenOption(token))
	assert.NilError(t, err)
	type args struct {
		m *MysqlConfig
		s *MysqlConfig
	}
	config := &MysqlConfig{
		UserName:     "nef",
		Password:     "SQnfNlLyqteJLvg7",
		Database:     "NEFDB",
		UseDiscovery: true,
		Discovery:    discovery,
		ServiceName:  "proxysql",
		SelectMode:   servicediscovery.RandomSelect,
	}
	tests := []struct {
		name    string
		args    args
		want    *OrmSql
		wantErr bool
	}{
		{"测试通过服务发现", args{
			m: config,
			s: config,
		}, nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewOrmSql(tt.args.m, tt.args.s)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewOrmSql() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got == nil {
				t.Error("got = nil")
				return
			}
			for i := 0; i < 10; i++ {
				_, err := got.GetSlave()
				assert.NilError(t, err)
				_, err = got.GetMaster()
				assert.NilError(t, err)
			}
		})
	}
}