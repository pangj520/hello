package thread

import (
	"fmt"
	"time"
	"library_Test/common"

	uuid "github.com/satori/go.uuid"
)

var timerPool = NewTimerPool(common.TimerPoolSize)

type TimerPool struct {
	poolSize int
	pool     chan *Timer
}

func NewTimerPool(size int) *TimerPool {
	pool := make(chan *Timer, size)
	for i := 0; i < size; i++ {
		pool <- &Timer{}
	}
	return &TimerPool{poolSize: size, pool: pool}
}

func (p *TimerPool) Get() (t *Timer) {
	t = <-p.pool
	t.ID = uuid.NewV4().String()
	t.stop = make(chan bool, 1)
	return
}

func (p *TimerPool) Recycle(t *Timer) {
	p.pool <- t
}

type Timer struct {
	ID           string
	timeout      time.Duration
	callback     func(args ...interface{})
	callbackArgs []interface{}
	stop         chan bool
}

func RegisterTimer(t time.Duration, f func(args ...interface{}), args ...interface{}) *Timer {
	timer := timerPool.Get()
	timer.timeout = t
	timer.callback = f
	timer.callbackArgs = args
	return timer
}

func (t *Timer) Start() {
	fmt.Printf("timer[%s] start.\n", t.ID)
	go func() {
		defer t.close()
		select {
		case <-time.After(t.timeout):
			if t.callback != nil {
				t.callback(t.callbackArgs...)
				fmt.Printf("timer[%s] call back done.\n", t.ID)
			}
			fmt.Printf("timer[%s] done.\n", t.ID)
		case <-t.stop:
			fmt.Printf("stop timer[%s].\n", t.ID)
		}
	}()
}

//复用
func (t *Timer) close() {
	t.timeout = time.Second * 0
	t.callback = nil
	t.callbackArgs = nil
	timerPool.Recycle(t)
}

func (t *Timer) Stop() {
	close(t.stop)
}
