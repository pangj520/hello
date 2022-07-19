package servicediscovery

import (
	"context"
	"errors"
	"fmt"
	"gotest.tools/assert"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

func Test_consistentHashSelector_Select(t *testing.T) {
	type args struct {
		ctx      context.Context
		services []Service
	}
	tests := []struct {
		name   string
		args   args
		count int
		want   Service
	}{
		{"测试一致性哈希", args{
			ctx:      context.Background(),
			services: []Service{{ID: "s1", Address: "127.0.0.1"},{ID: "s3", Address: "127.0.0.3"},{ID: "s2", Address: "127.0.0.2"}},
		}, 1000, Service{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var s1, s2, s3 int
			for i:=0;i<tt.count;i++{
				s := newConsistentHashSelector(tt.args.services)
				got := s.Select(tt.args.ctx)
				if got.IsNull(){
					assert.Error(t, errors.New(""), "没有选择任何服务")
				}
				//bts, _ := json.Marshal(&got)
				//fmt.Println(string(bts))
				switch got.ID {
				case "s1":
					s1++
				case "s2":
					s2++
				case "s3":
					s3++
				}
				time.Sleep(time.Millisecond)
			}
			fmt.Println(s1, s2, s3)
		})
	}
}

func Test_consistentHashSelector_UpdateServer(t *testing.T) {
	type args struct {
		services []Service
	}
	tests := []struct {
		name   string
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

		})
	}
}

func Test_createGeoServer(t *testing.T) {
	type args struct {
		services []Service
	}
	tests := []struct {
		name string
		args args
		want []*geoServer
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := createGeoServer(tt.args.services); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("createGeoServer() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_createICMPWeighted(t *testing.T) {
	type args struct {
		services []Service
	}
	tests := []struct {
		name string
		args args
		want []*Weighted
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := createICMPWeighted(tt.args.services); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("createICMPWeighted() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_createWeighted(t *testing.T) {
	type args struct {
		services []Service
	}
	tests := []struct {
		name string
		args args
		want []*Weighted
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := createWeighted(tt.args.services); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("createWeighted() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_geoSelector_Select(t *testing.T) {
	type fields struct {
		services  []*geoServer
		Latitude  float64
		Longitude float64
		r         *rand.Rand
	}
	type args struct {
		ctx      context.Context
		services []Service
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   Service
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := geoSelector{
				services:  tt.fields.services,
				Latitude:  tt.fields.Latitude,
				Longitude: tt.fields.Longitude,
				r:         tt.fields.r,
			}
			if got := s.Select(tt.args.ctx); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Select() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_geoSelector_UpdateServer(t *testing.T) {
	type fields struct {
		services  []*geoServer
		Latitude  float64
		Longitude float64
		r         *rand.Rand
	}
	type args struct {
		services []Service
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

		})
	}
}

func Test_newGeoSelector(t *testing.T) {
	type args struct {
		services  []Service
		latitude  float64
		longitude float64
	}
	tests := []struct {
		name string
		args args
		want Selector
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newGeoSelector(tt.args.services, tt.args.latitude, tt.args.longitude); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newGeoSelector() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_newRandomSelector(t *testing.T) {
	type args struct {
		services []Service
	}
	tests := []struct {
		name string
		args args
		want Selector
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newRandomSelector(tt.args.services); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newRandomSelector() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_newRoundRobinSelector(t *testing.T) {
	type args struct {
		services []Service
	}
	tests := []struct {
		name string
		args args
		want Selector
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newRoundRobinSelector(tt.args.services); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newRoundRobinSelector() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_newSelector(t *testing.T) {
	type args struct {
		selectMode SelectMode
		services   []Service
	}
	tests := []struct {
		name string
		args args
		want Selector
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewSelector(tt.args.selectMode, tt.args.services); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewSelector() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_newWeightedICMPSelector(t *testing.T) {
	type args struct {
		services []Service
	}
	tests := []struct {
		name string
		args args
		want Selector
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newWeightedICMPSelector(tt.args.services); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newWeightedICMPSelector() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_newWeightedRoundRobinSelector(t *testing.T) {
	type args struct {
		services []Service
	}
	tests := []struct {
		name string
		args args
		want Selector
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newWeightedRoundRobinSelector(tt.args.services); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newWeightedRoundRobinSelector() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_nextWeighted(t *testing.T) {
	type args struct {
		servers []*Weighted
	}
	tests := []struct {
		name     string
		args     args
		wantBest *Weighted
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotBest := nextWeighted(tt.args.servers); !reflect.DeepEqual(gotBest, tt.wantBest) {
				t.Errorf("nextWeighted() = %v, want %v", gotBest, tt.wantBest)
			}
		})
	}
}

func Test_randomSelector_Select(t *testing.T) {
	type fields struct {
		services []Service
	}
	type args struct {
		ctx      context.Context
		services []Service
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   Service
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := randomSelector{
				services: tt.fields.services,
			}
			if got := s.Select(tt.args.ctx); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Select() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_randomSelector_UpdateServer(t *testing.T) {
	type fields struct {
		services []Service
	}
	type args struct {
		services []Service
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

		})
	}
}

func Test_roundRobinSelector_Select(t *testing.T) {
	type fields struct {
		services []Service
		i        int
	}
	type args struct {
		ctx      context.Context
		services []Service
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   Service
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &roundRobinSelector{
				services: tt.fields.services,
				i:        tt.fields.i,
			}
			if got := s.Select(tt.args.ctx); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Select() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_roundRobinSelector_UpdateServer(t *testing.T) {
	type fields struct {
		services []Service
		i        int
	}
	type args struct {
		services []Service
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

		})
	}
}

func Test_weightedICMPSelector_Select(t *testing.T) {
	type fields struct {
		services []*Weighted
	}
	type args struct {
		ctx      context.Context
		services []Service
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   Service
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := weightedICMPSelector{
				services: tt.fields.services,
			}
			if got := s.Select(tt.args.ctx); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Select() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_weightedICMPSelector_UpdateServer(t *testing.T) {
	type fields struct {
		services []*Weighted
	}
	type args struct {
		services []Service
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

		})
	}
}

func Test_weightedRoundRobinSelector_Select(t *testing.T) {
	type fields struct {
		services []*Weighted
	}
	type args struct {
		ctx      context.Context
		services []Service
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   Service
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &weightedRoundRobinSelector{
				services: tt.fields.services,
			}
			if got := s.Select(tt.args.ctx); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Select() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_weightedRoundRobinSelector_UpdateServer(t *testing.T) {
	type fields struct {
		services []*Weighted
	}
	type args struct {
		services []Service
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

		})
	}
}