package irislib

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestIrisResponse_MarshalJSON(t *testing.T) {
	type fields struct {
		OptResultAndMsg OptResultAndMsg
		Obj             interface{}
	}
	type TestStruct struct {
		Field1 string
	}
	nullStruct := func() *TestStruct { return nil}
	nullInterface := func() interface{} { return nil}
	nullMap := func() map[string]interface{} { return nil}
	m := map[string]interface{}{"a": "a"}
	bs, _ := json.Marshal(m)

	s := "haha"
	i := 10
	f := 10.19
	b := true
	ss := []string{"a"}
	ibts := []byte{1, 1}
	bt := byte(1)
	ibtsp := []*byte{&bt}
	tests := []struct {
		name    string
		fields  fields
		want    []byte
		wantErr bool
	}{
		{"什么都不传", fields{}, nil, false},
		{"只传OptResultAndMsg", fields{OptResultAndMsg: OptResultAndMsg{
			OptResult: 1,
			Msg:       "",
		}}, nil, false},
		{"传Obj-nil", fields{Obj: nil}, nil, false},
		{"传Obj-nilInterface", fields{Obj: nullInterface()}, nil, false},
		{"传Obj-map", fields{Obj: map[string]interface{}{"m": "m"}}, nil, false},
		{"传Obj-nilMap", fields{Obj: nullMap()}, nil, false},
		{"传Obj-map pointer", fields{Obj: &map[string]interface{}{"m": "m-p"}}, nil, false},
		{"传Obj-struct", fields{Obj: TestStruct{Field1: "s"}}, nil, false},
		{"传Obj-struct-nil", fields{Obj: nullStruct()}, nil, false},
		{"传Obj-struct pointer", fields{Obj: &TestStruct{Field1: "s-p"}}, nil, false},
		{"传Obj-[]byte", fields{Obj: bs}, nil, false},
		{"传Obj-[]byte pointer", fields{Obj: &bs}, nil, false},

		{"传Obj-string", fields{Obj: s}, nil, true},
		{"传Obj-string pointer", fields{Obj: &s}, nil, true},
		{"传Obj-int", fields{Obj: i}, nil, true},
		{"传Obj-int pointer", fields{Obj: &i}, nil, true},
		{"传Obj-float", fields{Obj: f}, nil, true},
		{"传Obj-float pointer", fields{Obj: &f}, nil, true},
		{"传Obj-bool", fields{Obj: b}, nil, true},
		{"传Obj-bool pointer", fields{Obj: &b}, nil, true},
		{"传Obj-string slice", fields{Obj: ss}, nil, true},
		{"传Obj-string slice pointer", fields{Obj: &ss}, nil, true},
		{"传Obj-invalid []byte", fields{Obj: ibts}, nil, true},
		{"传Obj-invalid []byte pointer", fields{Obj: &ibts}, nil, true},
		{"传Obj-[]*byte", fields{Obj: &ibtsp}, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := IrisResponse{
				OptResultAndMsg: tt.fields.OptResultAndMsg,
				Obj:             tt.fields.Obj,
			}
			got, err := r.MarshalJSON()
			if err != nil {
				fmt.Println(err)
			}
			if (err != nil) != tt.wantErr {
				t.Errorf("MarshalJSON() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			fmt.Println(string(got))
		})
	}
}
