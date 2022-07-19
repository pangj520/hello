/*
*创建者：pangj
*创建时间：2019/8/20
*描述：
 */
package utils

import (
	"database/sql/driver"
	"fmt"
	"time"
)

const (
	timeFormat = "2006-01-02 15:04:05"
)

type MyTime struct {
	time.Time
}

func (t MyTime) String() string {
	return t.Time.Format(timeFormat)
}

// MarshalJSON on JSONTime format Time field with %Y-%m-%d %H:%M:%S
func (t MyTime) MarshalJSON() ([]byte, error) {
	if t.IsZero(){
		return []byte(`""`), nil
	}
	b := make([]byte, 0, len(timeFormat)+2)
	b = append(b, '"')
	b = t.Time.AppendFormat(b, timeFormat)
	b = append(b, '"')
	return b, nil
}

func (t *MyTime) UnmarshalJSON(data []byte) error {
	// Ignore null, like in the main JSON package.
	if string(data) == "null" || string(data) == `""` {
		return nil
	}
	var err error
	var tmp time.Time
	tmp, err = time.Parse(`"`+timeFormat+`"`, string(data))
	if err != nil {
		return err
	}
	(*t).Time = tmp
	return err
}

// Value insert timestamp into mysql need this function.
func (t MyTime) Value() (driver.Value, error) {
	if t.IsNil(){
		return nil, nil
	}
	return t.Time, nil
}

// Scan valueof time.Time
func (t *MyTime) Scan(v interface{}) error {
	value, ok := v.(time.Time)
	if ok {
		*t = MyTime{Time: value}
		return nil
	}
	return fmt.Errorf("can not convert %v to timestamp", v)
}

func (t MyTime) IsNil() bool {
	var zeroTime time.Time
	if t.Time.UnixNano() == zeroTime.UnixNano() {
		return true
	}
	return false
}
