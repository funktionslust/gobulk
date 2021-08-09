package gobulk

import (
	"sync/atomic"
)

// newWorkersPool creates a new workerPool instance.
func newWorkersPool(
	read func(container *Container) (map[string][]byte, error),
	requests chan *workerRequest,
	results chan *workerResponse,
	workersCount int,
) *workersPool {
	p := &workersPool{
		read:     read,
		requests: requests,
		results:  results,
		stop:     make(chan struct{}),
	}
	for i := 0; i < workersCount; i++ {
		p.AddWorker()
	}
	return p
}

// workersPool is a simple workers pool with ability to vary the workers number.
type workersPool struct {
	read         func(container *Container) (map[string][]byte, error)
	requests     chan *workerRequest
	results      chan *workerResponse
	stop         chan struct{}
	workersCount int64
}

// AddWorker runs a new worker instance.
func (p *workersPool) AddWorker() {
	w := worker{
		read:     p.read,
		requests: p.requests,
		results:  p.results,
		stop:     p.stop,
	}
	atomic.AddInt64(&p.workersCount, 1)
	go w.run()
}

// StopWorker stops the first free worker instance.
func (p *workersPool) StopWorker() {
	if atomic.LoadInt64(&p.workersCount) > 0 {
		atomic.AddInt64(&p.workersCount, -1)
		p.stop <- struct{}{}
	}
}

// WorkersCount returns the number of currently running workers.
func (p *workersPool) WorkersCount() int64 {
	return atomic.LoadInt64(&p.workersCount)
}

// Stop stops the pool and all its workers.
func (p *workersPool) Stop() {
	atomic.StoreInt64(&p.workersCount, 0)
	close(p.stop)
}
