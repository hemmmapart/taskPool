package taskpool

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"log/slog"

	"go.uber.org/atomic"
)

type TaskPool interface {
	Run(task func() error)
	Wait() []error
	Close()
}

type taskPool struct {
	conf *taskPoolConfig
	ec   *errCollector
	*poolStat

	wg         *sync.WaitGroup
	workerChan chan struct{}
	taskChan   chan func() error
	closed     atomic.Bool
}

type taskPoolConfig struct {
	multiplier     int32   // The multiplier is the factor used to increase the retry time.
	jitter         float64 // The 'jitter' is a factor used to randomly generate retry time intervals.
	retryTime      int     // The 'retryTime' represents how much time will the error be retried.
	capacity       int
	workerNum      int
	initialBackoff time.Duration
	maxBackoff     time.Duration
}

type errCollector struct {
	errorChan chan error
	*sync.Mutex
}

type poolStat struct {
	totalTask    *atomic.Int32
	consumedTask *atomic.Int32
	succTask     *atomic.Int32
	failedTask   *atomic.Int32
}

const (
	defaultWorkerNum        int           = 10
	defaultMultiplier       int32         = 2
	defaultJitter           float64       = 0.1
	defaultInitialBackoff   time.Duration = 1 * time.Second
	defaultMaxBackoff       time.Duration = 30 * time.Second
	defaultTaskChanCapacity int           = 200
)

// By default, the request won't retry, the workerNum is 10, default initial backoff is 1 second.
// 'capacity' represents the maximum number of tasks. If more tasks are added than the capacity allows,
// the execution of 'Run' might become blocked for moments. If the input capacity is lower than 0, it will be set to 200.
func New(capacity int, opts ...Option) TaskPool {
	if capacity < 0 {
		capacity = defaultTaskChanCapacity
	}

	tp := &taskPool{
		conf: &taskPoolConfig{
			multiplier:     defaultMultiplier,
			jitter:         defaultJitter,
			retryTime:      0,
			capacity:       capacity,
			workerNum:      defaultWorkerNum,
			initialBackoff: defaultInitialBackoff,
		},

		ec: &errCollector{
			errorChan: make(chan error, capacity),
			Mutex:     new(sync.Mutex),
		},

		poolStat: &poolStat{
			totalTask:    atomic.NewInt32(0),
			consumedTask: atomic.NewInt32(0),
			succTask:     atomic.NewInt32(0),
			failedTask:   atomic.NewInt32(0),
		},

		wg: new(sync.WaitGroup),
	}

	for _, o := range opts {
		o(tp.conf)
	}

	tp.workerChan = make(chan struct{}, tp.conf.workerNum)
	tp.taskChan = make(chan func() error, tp.conf.capacity)

	go tp.consumeTasks()
	return tp
}

func (t *taskPool) Run(task func() error) {
	t.wg.Add(1)
	t.totalTask.Inc()
	t.taskChan <- task
}

func (t *taskPool) consumeTasks() {
	for {
		task, ok := <-t.taskChan
		if !ok {
			return
		}

		t.consumedTask.Inc()
		t.workerChan <- struct{}{}
		go t.runWithRetry(task)
	}
}

func (t *taskPool) Close() {
	slog.Info(fmt.Sprintf("totalTask:%d, consumedTask:%d, succTask:%d, failedTask:%d", t.totalTask.Load(),
		t.consumedTask.Load(), t.succTask.Load(), t.failedTask.Load()))
	if t.closed.CompareAndSwap(false, true) {
		close(t.taskChan)
	}
}

func (t *taskPool) Wait() []error {
	t.wg.Wait()
	res := make([]error, 0)

	t.ec.Lock()
	defer t.ec.Unlock()

	close(t.ec.errorChan)
	for {
		err, ok := <-t.ec.errorChan
		if !ok {
			break
		}
		res = append(res, err)
	}
	t.ec.errorChan = make(chan error, t.conf.capacity)

	return res
}

func (t *taskPool) runWithRetry(task func() error) {
	defer t.wg.Done()
	var err error
	for i := 0; i <= t.conf.retryTime; i++ {
		err = task()
		if err != nil {
			time.Sleep(t.backoff(i))
			continue
		}
		break
	}

	if err != nil {
		t.failedTask.Inc()
		t.ec.errorChan <- err
	} else {
		t.succTask.Inc()
	}
	<-t.workerChan
}

func (t *taskPool) backoff(retries int) time.Duration {
	if retries == 0 {
		return t.conf.initialBackoff
	}

	backoff, max := float64(t.conf.initialBackoff), float64(t.conf.maxBackoff)

	// calculate backoff time
	for backoff < max && retries > 0 {
		backoff *= float64(t.conf.multiplier)
		retries--
	}

	if backoff > max {
		backoff = max
	}

	// Randomize backoff delays
	backoff *= 1 + t.conf.jitter*(rand.Float64()*2-1)
	if backoff < 0 {
		return 0
	}
	return time.Duration(backoff)
}
