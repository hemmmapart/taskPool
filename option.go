package taskpool

import (
	"time"
)

type Option func(tpConfig *taskPoolConfig)

func WithRetryTime(retryTime int) Option {
	if retryTime < 0 {
		retryTime = 0
	}

	return func(tpConfig *taskPoolConfig) {
		tpConfig.retryTime = retryTime
	}
}

func WithWorkNum(workerNum int) Option {
	if workerNum < 0 {
		workerNum = defaultWorkerNum
	}

	return func(tpConfig *taskPoolConfig) {
		tpConfig.workerNum = workerNum
	}
}

func WithInitialBackoff(initialBackoff time.Duration) Option {
	return func(tpConfig *taskPoolConfig) {
		tpConfig.initialBackoff = initialBackoff
	}
}

func WithMaxWaitTime(maxBackoff time.Duration) Option {
	if maxBackoff > time.Duration(3*time.Hour) {
		maxBackoff = time.Duration(3 * time.Hour)
	}

	return func(tpConfig *taskPoolConfig) {
		tpConfig.maxBackoff = maxBackoff
	}
}

func WithJitter(jitter float64) Option {
	if jitter > 1 || jitter < -1 {
		jitter = defaultJitter
	}

	return func(tpConfig *taskPoolConfig) {
		tpConfig.jitter = jitter
	}
}
