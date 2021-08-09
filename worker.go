package gobulk

import (
	"context"
	"log"
	"time"
)

// worker is an entity to fulfill data download tasks.
type worker struct {
	read     func(container *Container) (map[string][]byte, error)
	requests chan *workerRequest
	results  chan *workerResponse
	stop     chan struct{}
}

// run runs the worker.
func (w *worker) run() {
	for {
		select {
		case <-w.stop:
			return
		case req, ok := <-w.requests:
			if !ok {
				return
			}
			ctx, cancel := context.WithCancel(context.Background())
			go func() {
				t := time.NewTicker(5 * time.Minute)
				for {
					select {
					case <-ctx.Done():
						return
					case <-t.C:
						log.Printf("worker download is in progress: %s %s", req.container.InputRepository, req.container.InputIdentifier)
					}
				}
			}()
			d, err := w.read(req.container)
			w.results <- &workerResponse{
				id: req.id,
				response: &readResponse{
					data: d,
					err:  err,
				},
			}
			cancel()
		}
	}
}

// workerRequest contains data needed to build a request and distinguish the
// corresponding container identity.
type workerRequest struct {
	container *Container
	id        string
}

// workerResponse contains data regarding a single workerRequest result.
type workerResponse struct {
	response *readResponse
	id       string
}
