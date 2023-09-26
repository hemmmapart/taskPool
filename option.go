package taskpool

import (
	"time"
)

type Option func(tp *taskPool)

func WithRetryTime(retryTime int) Option {
	if retryTime < 0 {
		retryTime = 0
	}
	return func(t *taskPool) {
		t.conf.retryTime = retryTime
	}
}

func WithWorkNum(workerNum int) Option {
	if workerNum < 0 {
		workerNum = defaultWorkerNum
	}
	return func(t *taskPool) {
		t.workerChan = make(chan struct{}, workerNum)
	}
}

func WithInitialBackoff(initialBackoff time.Duration) Option {
	return func(t *taskPool) {
		t.conf.initialBackoff = initialBackoff
	}
}

func WithMaxWaitTime(maxBackoff time.Duration) Option {
	if maxBackoff > time.Duration(3*time.Hour) {
		maxBackoff = time.Duration(3 * time.Hour)
	}
	return func(t *taskPool) {
		t.conf.maxBackoff = maxBackoff
	}
}

func WithJitter(jitter float64) Option {
	if jitter > 1 || jitter < -1 {
		jitter = defaultJitter
	}
	return func(t *taskPool) {
		t.conf.jitter = jitter
	}
}

func WithTaskCapacity(capacity int) Option {
	if capacity < 0 {
		capacity = defaultTaskChanCapacity
	}
	return func(t *taskPool) {
		if capacity > 0 {
			t.taskChan = make(chan func() error, capacity)
		}
	}
}
