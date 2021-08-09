package gobulk

import (
	"runtime"
	"sync"
	"time"

	"github.com/paulbellamy/ratecounter"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const defaultWorkersLimit int64 = 15

// NewLoader returns a new instance of Loader.
func NewLoader(read func(container *Container) (map[string][]byte, error), maxWorkers int64) *Loader {
	requests := make(chan *workerRequest)
	responses := make(chan *workerResponse)
	if maxWorkers == 0 {
		maxWorkers = defaultWorkersLimit
	}
	l := &Loader{
		read:                read,
		requests:            requests,
		responses:           responses,
		workersPool:         newWorkersPool(read, requests, responses, 1),
		mu:                  &sync.Mutex{},
		tasks:               newTasksList(),
		responsesCache:      make(map[string]chan *readResponse, 100),
		responsesSizes:      make(map[string]int, 100),
		reqRate:             ratecounter.NewRateCounter(time.Minute),
		readRate:            ratecounter.NewRateCounter(time.Minute),
		onFeetResponsesRate: ratecounter.NewRateCounter(time.Minute),
		cachedResponsesRate: ratecounter.NewRateCounter(time.Minute),
		maxWorkers:          maxWorkers,
		stop:                make(chan struct{}),
	}
	return l
}

// Loader is a helper for input. It uses the read func to read the container data and then
// store it in memory to be quick obtainable.
type Loader struct {
	read        func(container *Container) (map[string][]byte, error)
	requests    chan *workerRequest
	responses   chan *workerResponse
	workersPool *workersPool

	mu             *sync.Mutex
	tasks          *tasksList
	responsesCache map[string]chan *readResponse
	responsesSizes map[string]int

	reqRate             *ratecounter.RateCounter
	readRate            *ratecounter.RateCounter
	onFeetResponsesRate *ratecounter.RateCounter
	cachedResponsesRate *ratecounter.RateCounter
	maxWorkers          int64
	cacheSize           int64
	stop                chan struct{}
}

// Put puts the passed container to the containers queue.
func (l *Loader) Put(containers []*Container) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.tasks.Add(containers)
}

// Get instantly returns the container data it if has been downloaded, otherwise
// it just gets the data on the run.
func (l *Loader) Get(container *Container) (map[string][]byte, error) {
	l.reqRate.Incr(1)
	key := buildContainerKey(container)
	l.mu.Lock()
	start := time.Now()
	ch := l.responsesCache[key]
	if ch == nil {
		l.tasks.Delete(key)
		l.responsesCache[key] = make(chan *readResponse, 1)
		l.mu.Unlock()
		l.onFeetResponsesRate.Incr(1)
		resp, err := l.read(container)
		dur := time.Since(start)
		onFeetResponsesDuration.Set(float64(dur.Microseconds()))
		l.deleteFromCache(key)
		return resp, err
	}
	l.mu.Unlock()
	l.cachedResponsesRate.Incr(1)
	resp := <-ch
	dur := time.Since(start)
	cachedResponsesDuration.Set(float64(dur.Microseconds()))
	l.deleteFromCache(key)
	return resp.data, resp.err
}

// Stop stops the loader.
func (l *Loader) Stop() {
	close(l.stop)
	l.workersPool.Stop()
}

// Run runs the loader listening.
func (l *Loader) Run() {
	go l.listenRequests()
	go l.listenResponses()
	go l.listenRates()
	go l.runMetricsUpdater()
	go l.runCacheSizeCalculator()
}

// listenRequests listens for new containers passed to the loader by Put method
// and sends corresponding download jobs to workers.
func (l *Loader) listenRequests() {
	for {
		select {
		case <-l.stop:
			return
		default:
			l.mu.Lock()
			c := l.tasks.Next()
			if c == nil {
				l.mu.Unlock()
				runtime.Gosched()
				continue
			}
			key := buildContainerKey(c)
			l.responsesCache[key] = make(chan *readResponse, 1)
			l.mu.Unlock()
			select {
			case <-l.stop:
				return
			case l.requests <- &workerRequest{
				container: c,
				id:        key,
			}:
			}
		}
	}
}

// listenResponses listens for fulfilled requests responses and stores the result
// data in the memory cache.
func (l *Loader) listenResponses() {
	for {
		select {
		case <-l.stop:
			return
		case resp, ok := <-l.responses:
			if !ok {
				return
			}
			l.readRate.Incr(1)
			size := resp.response.Size()
			l.mu.Lock()
			ch := l.responsesCache[resp.id]
			l.responsesSizes[resp.id] = size
			l.mu.Unlock()
			select {
			case <-l.stop:
				return
			case ch <- resp.response:
			}
		}
	}
}

// listenRates listens for changing request and response rates and in accordance
// to that changes the running workers number.
func (l *Loader) listenRates() {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-t.C:
			switch l.calculatePoolAction() {
			case poolActionAdd:
				l.workersPool.AddWorker()
			case poolActionStop:
				l.workersPool.StopWorker()
			case poolActionKeep:
			}
		}
	}
}

// runMetricsUpdater runs a process which periodically collects the loader metrics
// and sends them to the prometheus.
func (l *Loader) runMetricsUpdater() {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-t.C:
			workersCount.Set(float64(l.workersPool.WorkersCount()))
			readRequestsRate.Set(float64(l.reqRate.Rate()))
			readRate.Set(float64(l.readRate.Rate()))
			onFeetResponsesRate.Set(float64(l.onFeetResponsesRate.Rate()))
			cachedResponsesRate.Set(float64(l.cachedResponsesRate.Rate()))
			containersQueueSize.Set(float64(l.tasks.Len()))
			responseCacheLength.Set(float64(len(l.responsesCache)))
			responseCacheSize.Set(float64(l.cacheSize))
		}
	}
}

// runCacheSizeCalculator periodically calculates the total size of the loader cache.
func (l *Loader) runCacheSizeCalculator() {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-t.C:
			l.cacheSize = l.calculateCacheSize()
		}
	}
}

// deleteFromCache closes a channel created for a container with the passed containerKey
// and deletes it from the responsesCache.
func (l *Loader) deleteFromCache(containerKey string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ex := l.responsesCache[containerKey]; ex {
		close(l.responsesCache[containerKey])
		l.responsesCache[containerKey] = nil
		delete(l.responsesCache, containerKey)
		delete(l.responsesSizes, containerKey)
	}
}

// calculatePoolAction in dependence of current read and requst rates decides changes or not
// the workers number.
func (l *Loader) calculatePoolAction() poolAction {
	reqRate := l.reqRate.Rate()
	readRate := l.readRate.Rate()
	workersCount := l.workersPool.WorkersCount()
	switch {
	case l.readRateIsTooLow(reqRate, readRate) && !l.tooManyWorkers(workersCount) && !l.cacheIsTooBig():
		return poolActionAdd
	case l.cacheIsTooBig() || (l.readRateIsTooHigh(reqRate, readRate) || reqRate == 0) && l.workersPool.WorkersCount() > 1:
		return poolActionStop
	default:
		return poolActionKeep
	}
}

// readRateIsTooLow returns true if the read rate is lower than the request rate+20%.
func (Loader) readRateIsTooLow(reqRate, readRate int64) bool {
	return float64(readRate) < float64(reqRate)*1.2
}

// tooManyWorkers returns whether the workers count is more than the l.MaxWorkers value.
func (l *Loader) tooManyWorkers(workersCount int64) bool {
	return workersCount >= l.maxWorkers
}

// readRateIsTooHigh returns true if the read rate is higher that the request rate.
func (Loader) readRateIsTooHigh(reqRate, readRate int64) bool {
	return readRate > reqRate
}

// cacheIsTooBig returns true if the cache size is more that 100MiB.
func (l *Loader) cacheIsTooBig() bool {
	return l.cacheSize > 100*1024*1024
}

// calculateCacheSize returns the current repsonses cache size in bytes.
func (l *Loader) calculateCacheSize() int64 {
	var cacheSize int64
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, size := range l.responsesSizes {
		cacheSize += int64(size)
	}
	return cacheSize
}

// readResponse contains the read call result.
type readResponse struct {
	data map[string][]byte
	err  error
}

// Size returns the response size in bytes.
func (r *readResponse) Size() int {
	var size int
	for _, f := range r.data {
		size += len(f)
	}
	return size
}

type poolAction int

const (
	poolActionAdd = iota
	poolActionStop
	poolActionKeep
)

var (
	readRequestsRate = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "input_loader_read_requests_rate",
		Help: "Read requests per minute rate",
	})
	readRate = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "input_loader_read_rate",
		Help: "Workers responses per minute rate",
	})
	onFeetResponsesRate = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "input_loader_on_feet_responses_rate",
		Help: "Rate of responses per minute which have been processed by the loader on feet",
	})
	onFeetResponsesDuration = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "input_loader_on_feet_responses_duration",
		Help: "Time taken to get a single request response on feet in microseconds",
	})
	cachedResponsesRate = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "input_loader_cached_responses_rate",
		Help: "Rate of responses per minute which have been processed by workers",
	})
	cachedResponsesDuration = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "input_loader_cached_responses_duration",
		Help: "Time taken to get a single request response from cache in microseconds",
	})
	workersCount = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "input_loader_workers_count",
		Help: "Number of currently running workers",
	})
	containersQueueSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "input_loader_containers_queue_size",
		Help: "Number of containers currently stored in the queue",
	})
	responseCacheLength = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "input_loader_response_cache_length",
		Help: "Number of cached container read responses ready to be gotten",
	})
	responseCacheSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "input_loader_response_cache_size",
		Help: "Size in bytes of cached container read responses ready to be gotten",
	})
)
