// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package deadline

import (
	"sync"
	"time"
)

// PipeDeadline is an abstraction for handling timeouts.
type PipeDeadline struct {
	mu          sync.Mutex // Guards timer, its active state, and cancel
	timer       *time.Timer
	timerActive bool
	stopped     bool
	cancel      chan struct{} // Must be non-nil
}

func MakePipeDeadline() PipeDeadline {
	return PipeDeadline{cancel: make(chan struct{})}
}

// Set sets the point in time when the deadline will time out.
// A timeout event is signaled by closing the channel returned by waiter.
// Once a timeout has occurred, the deadline can be refreshed by specifying a
// t value in the future.
//
// A zero value for t prevents timeout.
func (d *PipeDeadline) Set(t time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.stopped {
		return
	}
	d.stopTimer()

	// Time is zero, then there is no deadline.
	closed := isClosedChan(d.cancel)
	if t.IsZero() {
		if closed {
			d.cancel = make(chan struct{})
		}
		return
	}

	// Time in the future, setup a timer to cancel in the future.
	if dur := time.Until(t); dur > 0 {
		if closed {
			d.cancel = make(chan struct{})
		}
		if d.timer == nil {
			d.timer = time.AfterFunc(dur, func() { close(d.cancel) })
		} else {
			d.timer.Reset(dur)
		}
		d.timerActive = true
		return
	}

	// Time in the past, so close immediately.
	if !closed {
		close(d.cancel)
	}
}

// stopTimer runs with mu held. A previously cleared deadline has an inactive
// timer and an open cancel channel; waiting on that channel would deadlock.
func (d *PipeDeadline) stopTimer() {
	if d.timerActive && !d.timer.Stop() {
		<-d.cancel // The callback finishes before Set can replace cancel.
	}
	d.timerActive = false
}

// stop prevents both existing and concurrently refreshed timers from retaining
// a closed connection. The connection itself wakes its blocked readers.
func (d *PipeDeadline) stop() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stopped = true
	d.stopTimer()
	d.timer = nil
}

// Wait returns a channel that is closed when the deadline is exceeded.
func (d *PipeDeadline) Wait() chan struct{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.cancel
}

func isClosedChan(c <-chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}

func makeFilledChan() chan struct{} {
	ch := make(chan struct{}, 1)
	ch <- struct{}{}
	return ch
}
