package nosql

import (
	"encoding/json"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gotest.tools/assert"
	"testing"
	"time"
	"library_Test/library/log"
)

func TestMongoCollection_FindOneForMap(t *testing.T) {
	_ = log.InitLog(log.CreateDefaultLoggerConfig())
	db, err := Connect(&MongoConfig{
		Type:           ReplicaSet,
		Brokers:        "192.168.37.60:27017,192.168.37.61:27017,192.168.37.62:27017",
		UserName:       "nef",
		Password:       "dianxin",
		DbName:         "nef",
		Auth:           "nef",
		ReplicaSetName: "replSet-1",
	})
	assert.NilError(t, err)
	timeout := time.Second * 3
	id, _ := primitive.ObjectIDFromHex("5ed9e6e14076729b7d77bcb4")
	type fields struct {
		Collection         *mongo.Collection
		DefaultTimeout     time.Duration
		RbSleepIncUnit     time.Duration
		MaxRbSleepInterval time.Duration
	}
	type args struct {
		filter  interface{}
		timeout *time.Duration
		opts    []*options.FindOneOptions
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantOut map[string]interface{}
		wantErr bool
	}{
		{"测试FindOneForMap", fields{
			Collection: db.GetCollection("test").Collection,
		}, args{
			//filter:  map[string]interface{}{
			//	"_id": id,
			//},
			filter:  bson.D{{Key: "_id", Value: id}},
			timeout: &timeout,
			opts:    []*options.FindOneOptions{{Projection: bson.M{"_id": 0}}},
		}, nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &MongoCollection{
				Collection: tt.fields.Collection,
			}
			gotOut, err := m.FindOneForMap(tt.args.filter, tt.args.timeout, tt.args.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("FindOneForMap() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			bts, err := json.Marshal(gotOut)
			fmt.Println(err)
			fmt.Println(string(bts))
		})
	}
}

func TestMongoCollection_UpdateMany(t *testing.T) {
	_ = log.InitLog(log.CreateDefaultLoggerConfig())
	db, err := Connect(&MongoConfig{
		Type:           ReplicaSet,
		Brokers:        "192.168.37.60:27017,192.168.37.61:27017,192.168.37.62:27017",
		UserName:       "nef",
		Password:       "dianxin",
		DbName:         "nef",
		Auth:           "nef",
		ReplicaSetName: "replSet-1",
	})
	assert.NilError(t, err)
	timeout := time.Second * 5
	type fields struct {
		Collection         *mongo.Collection
		DefaultTimeout     time.Duration
		RbSleepIncUnit     time.Duration
		MaxRbSleepInterval time.Duration
	}
	type args struct {
		filter  interface{}
		update  interface{}
		timeout *time.Duration
		opts    []*options.UpdateOptions
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{"测试UpdateMany", fields{
			Collection: db.GetCollection("test").Collection,
		}, args{
			filter: map[string]interface{}{
				"a": "c",
			},
			update: map[string]interface{}{
				"$set": map[string]interface{}{
					"b": "baa",
				},
			},
			timeout: &timeout}, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &MongoCollection{
				Collection: tt.fields.Collection,
			}
			if err := m.UpdateMany(tt.args.filter, tt.args.update, tt.args.timeout, tt.args.opts...); (err != nil) != tt.wantErr {
				t.Errorf("UpdateMany() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}