package taskpool

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/atomic"
)

type execResp struct {
	errMsg       string
	errCode      int
	detailedInfo interface{}
}

func (e *execResp) Error() string {
	return "errMsg:" + e.errMsg + ", errCode:" + strconv.Itoa(e.errCode)
}

func TestDemoRunWithRetry(t *testing.T) {
	retryTime := new(atomic.Int32)
	Convey("Test TaskPool while occasionally encounter some error", t, func() {
		tp := New(500, WithRetryTime(2), WithWorkNum(10), WithInitialBackoff(1*time.Second))
		defer tp.Close()
		for i := 0; i < 100; i++ {
			i := i
			logicFunc := func() error {
				err := mockHttpRequest()
				if err != "" {
					retryTime.Inc()
					_errMsg := fmt.Sprintf("i:%d, encounter err:%v, Need retry\n", i, err)
					t.Log(_errMsg)
					return &execResp{
						errMsg:       _errMsg,
						errCode:      500,
						detailedInfo: "XXXX",
					}
				}
				return nil
			}
			tp.Run(logicFunc)
		}

		errs := tp.Wait()
		t.Logf("There's %d task retried, after retry, got:%d error\n", retryTime.Load(), len(errs))
	})
}

func TestDemoRunAsync(t *testing.T) {
	execTime := new(atomic.Int32)
	Convey("Test taskPool without wait ", t, func() {
		tp := New(500, WithRetryTime(2), WithWorkNum(10))
		defer tp.Close()
		for i := 0; i < 10; i++ {
			task := func() error {
				execTime.Inc()
				return nil
			}

			tp.Run(task)
		}

		// The reason for waiting here is to demonstrate the accuracy of the results.
		// However, if the user does not wish to wait for the execution to complete, they can directly return.
		time.Sleep(time.Second)
		So(execTime.Load(), ShouldEqual, 10)
	})
}

func TestDemoExtractInfoFromError(t *testing.T) {
	Convey("Test extract informations from returned error", t, func() {
		// Use default settings
		tp := New(500)
		defer tp.Close()
		retErr := &execResp{
			errMsg:       "Encountered some error",
			errCode:      500,
			detailedInfo: "XXXX",
		}

		logicFunc := func() error {
			return retErr
		}

		tp.Run(logicFunc)

		errs := tp.Wait()
		for _, err := range errs {
			errInfo := err.(*execResp)
			// Deal with the informations extracted from error
			So(errInfo, ShouldEqual, retErr)
		}
		So(len(errs), ShouldEqual, 1)
	})
}

func TestReuseTaskpool(t *testing.T) {
	retryTime := new(atomic.Int32)
	tp := New(500, WithRetryTime(2), WithWorkNum(10), WithInitialBackoff(1*time.Second))
	defer tp.Close()
	Convey("Test Run() and Wait()", t, func() {
		for i := 0; i < 20; i++ {
			i := i
			logicFunc := func() error {
				err := mockHttpRequest()
				if err != "" {
					retryTime.Inc()
					_errMsg := fmt.Sprintf("i:%d, encounter err:%v, Need retry\n", i, err)
					t.Log(_errMsg)
					return &execResp{
						errMsg:       _errMsg,
						errCode:      500,
						detailedInfo: "XXXX",
					}
				}
				return nil
			}
			tp.Run(logicFunc)
		}
		errs := tp.Wait()
		t.Logf("There's %d task retried, after retry, got:%d error\n", retryTime.Load(), len(errs))
	})

	Convey("Test Run() and Wait() again", t, func() {
		for i := 0; i < 20; i++ {
			i := i
			logicFunc := func() error {
				err := mockHttpRequest()
				if err != "" {
					retryTime.Inc()
					_errMsg := fmt.Sprintf("i:%d, encounter err:%v, Need retry\n", i, err)
					t.Log(_errMsg)
					return &execResp{
						errMsg:       _errMsg,
						errCode:      500,
						detailedInfo: "XXXX",
					}
				}
				return nil
			}
			tp.Run(logicFunc)
		}
		errs := tp.Wait()
		t.Logf("There's %d task retried, after retry, got:%d error\n", retryTime.Load(), len(errs))
	})
}

func mockHttpRequest() string {
	num := rand.Int31n(10)
	if num == 0 {
		return "Oops! Encountered http request failed"
	}
	return ""
}

func TestBackoff(t *testing.T) {
	tp := &taskPool{
		conf: &taskPoolConfig{
			multiplier:     defaultMultiplier,
			jitter:         defaultJitter,
			retryTime:      0,
			initialBackoff: defaultInitialBackoff,
			maxBackoff:     defaultMaxBackoff,
		},

		ec: &errCollector{
			errorChan: make(chan error, defaultTaskChanCapacity),
			Mutex:     new(sync.Mutex),
		},
		workerChan: make(chan struct{}, defaultWorkerNum),
		wg:         new(sync.WaitGroup),
	}

	t.Log("\nWhen retry time set to 1, randomly generated backoff time as following:\n")
	for i := 0; i < 10; i++ {
		t.Log(tp.backoff(1))
	}

	t.Log("\n\nretry time set to 2:")
	for i := 0; i < 10; i++ {
		t.Log(tp.backoff(2))
	}

	t.Log("\n\nretry time set to 3:")
	for i := 0; i < 10; i++ {
		t.Log(tp.backoff(3))
	}
}
