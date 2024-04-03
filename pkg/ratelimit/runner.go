// Copyright 2022 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ratelimit

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// Runner is the interface for running tasks.
type Runner interface {
	RunTask(ctx context.Context, opt TaskOpts, f func(context.Context)) error
}

type Task func(context.Context)

type TaskWrap struct {
	ctx         context.Context
	Opts        TaskOpts
	Task        Task
	submittedAt time.Time
}

// ErrMaxWaitingTasksExceeded is returned when the number of waiting tasks exceeds the maximum.
var ErrMaxWaitingTasksExceeded = errors.New("max waiting tasks exceeded")

// AsyncRunner is a simple task runner that limits the number of concurrent tasks.
type AsyncRunner struct {
	numTasks           int64
	maxPendingTasks    uint64
	name               string
	maxPendingDuration time.Duration
	taskChan           chan *TaskWrap
	pendingTasks       []*TaskWrap
	pendingMu          sync.Mutex
	stopChan           chan struct{}
	wg                 sync.WaitGroup
}

// NewAsyncRunner creates a new AsyncRunner.
func NewAsyncRunner(name string, maxPendingTasks uint64) *AsyncRunner {
	s := &AsyncRunner{
		name:               name,
		maxPendingTasks:    maxPendingTasks,
		maxPendingDuration: 3 * time.Minute,
		taskChan:           make(chan *TaskWrap),
		pendingTasks:       make([]*TaskWrap, 0),
		stopChan:           make(chan struct{}),
	}
	s.Start()
	return s
}

// TaskOpts is the options for RunTask.
type TaskOpts struct {
	// TaskName is a human-readable name for the operation. TODO: metrics by name.
	TaskName string
	Limit    *ConcurrencyLimiter
}

func (s *AsyncRunner) Start() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			select {
			case task := <-s.taskChan:
				if task.Opts.Limit != nil {
					token, err := task.Opts.Limit.Acquire(context.Background())
					if err != nil {
						log.Error("failed to acquire semaphore", zap.String("task-name", task.Opts.TaskName), zap.Error(err))
						continue
					}
					go s.runTask(task.ctx, task.Task, token)
				} else {

				}
			case <-s.stopChan:
				return
			}
		}
	}()
}

func (s *AsyncRunner) runTask(ctx context.Context, task Task, token *TaskToken) {
	task(ctx)
	if token != nil {
		token.Release()
		s.processPendingTasks()
	}
}

func (s *AsyncRunner) processPendingTasks() {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	for len(s.pendingTasks) > 0 {
		task := s.pendingTasks[0]
		select {
		case s.taskChan <- task:
			s.pendingTasks = s.pendingTasks[1:]
			return
		default:
			return
		}
	}
}

// Stop stops the runner.
func (s *AsyncRunner) Stop() {
	close(s.stopChan)
	s.wg.Wait()
}

// RunTask runs the task asynchronously.
func (s *AsyncRunner) RunTask(ctx context.Context, opt TaskOpts, f func(context.Context)) error {
	taskWrap := &TaskWrap{
		ctx:  ctx,
		Opts: opt,
		Task: f,
	}
	select {
	case s.taskChan <- taskWrap:
	default:
		s.pendingMu.Lock()
		defer s.pendingMu.Unlock()
		if len(s.pendingTasks) > 0 {
			maxWait := time.Since(s.pendingTasks[0].submittedAt)
			if maxWait > s.maxPendingDuration {
				return errors.New("max pending duration exceeded")
			}
		}
		taskWrap.submittedAt = time.Now()
		s.pendingTasks = append(s.pendingTasks, taskWrap)
	}
	return nil
}

// RunTask except the callback f is run in a goroutine.
// The call doesn't block for the callback to finish execution.
func (s *AsyncRunner) RunTask2(ctx context.Context, opt TaskOpts, f func(context.Context)) error {
	if opt.Limit != nil && atomic.LoadInt64(&s.numTasks) >= int64(s.maxPendingTasks) {
		log.Error("max waiting tasks exceeded", zap.String("task-name", opt.TaskName))
		return ErrMaxWaitingTasksExceeded
	}
	s.addTask(1)
	go func(ctx context.Context, opt TaskOpts) {
		var token *TaskToken
		if opt.Limit != nil {
			// Wait for permission to run from the semaphore.
			var err error
			if opt.Limit != nil {
				token, err = opt.Limit.Acquire(ctx)
			}
			if err != nil {
				log.Error("failed to acquire semaphore", zap.String("task-name", opt.TaskName), zap.Error(err))
				return
			}

			// Check for canceled context: it's possible to get the semaphore even
			// if the context is canceled.
			if ctx.Err() != nil {
				log.Error("context is canceled", zap.String("task-name", opt.TaskName))
				return
			}
		}
		defer s.addTask(-1)
		defer s.recover()
		if token != nil {
			defer token.Release()
		}
		f(ctx)
	}(ctx, opt)

	return nil
}

func (s *AsyncRunner) recover() {
	if r := recover(); r != nil {
		log.Error("panic in runner", zap.Any("error", r))
		return
	}
}

func (s *AsyncRunner) addTask(delta int64) {
	atomic.AddInt64(&s.numTasks, delta)
}

// SyncRunner is a simple task runner that limits the number of concurrent tasks.
type SyncRunner struct{}

// NewSyncRunner creates a new SyncRunner.
func NewSyncRunner() *SyncRunner {
	return &SyncRunner{}
}

// RunTask runs the task synchronously.
func (s *SyncRunner) RunTask(ctx context.Context, opt TaskOpts, f func(context.Context)) error {
	f(ctx)
	return nil
}
