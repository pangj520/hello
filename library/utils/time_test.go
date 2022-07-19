package utils

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type TimeStruct struct {
	Time MyTime `json:"time"`
}

func TestMyTime_UnmarshalJSON(t *testing.T) {
	s1 := TimeStruct{Time: MyTime{time.Now()}}
	b, _ := json.Marshal(s1)
	fmt.Println(string(b))
	s := new(TimeStruct)
	err := json.Unmarshal(b, s)
	assert.NoError(t, err)
	fmt.Println(s.Time)
}

func TestMyTime_UnmarshalJSON2(t *testing.T) {
	var ti time.Time
	s1 := TimeStruct{Time: MyTime{Time: ti}}
	b, _ := json.Marshal(s1)
	fmt.Println(string(b))
	s := new(TimeStruct)
	err := json.Unmarshal(b, s)
	assert.NoError(t, err)
	fmt.Println(s.Time)
}
