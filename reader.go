package gobulk

import (
	"fmt"
	"path"
	"time"

	"go.uber.org/zap"
)

// TODO: adapt metrics names to new plural containers pipeline.
const (
	readerTrackerNextContainersMetricName   = "reader_tracker_next_containers"
	readerInputReadContainersBulkMetricName = "reader_input_read_containers_bulk"
)

// Reader constantly checks the Tracker for new containers to be processed.
// Once a container has been found, the raw data of the container will be
// retrieved from the input to be forwarded to the Parser.
type Reader struct {
	tracker      Tracker
	input        Input
	readBulkSize int
	readStrategy Strategy
	metrics      MetricsTracker
	logger       *zap.Logger
}

// NewReader returns a preconfigured reader struct.
func NewReader(
	tracker Tracker,
	input Input,
	readBulkSize int,
	readStrategy Strategy,
	logger *zap.Logger,
	metricsTracker MetricsTracker,
) *Reader {
	metricsTracker.Add(readerTrackerNextContainersMetricName, "Time taken to get next containers from the reader")
	metricsTracker.Add(readerInputReadContainersBulkMetricName, "Time taken to get containers bulk data from the input")
	return &Reader{
		tracker:      tracker,
		input:        input,
		readBulkSize: readBulkSize,
		readStrategy: readStrategy,
		metrics:      metricsTracker,
		logger:       logger,
	}
}

// NextContainersBulk tries to get the next containers bulk. Returns successfully read containers,
// issues mapped by failed container IDs and an error.
func (r *Reader) NextContainersBulk() (*ProcessContainersResult, error) {
	r.logger.Info("reader start", zap.Int("bulk_size", r.readBulkSize))
	nextStart := time.Now()
	containers, err := r.tracker.NextContainers(r.readStrategy, r.readBulkSize)
	if err != nil {
		return nil, err
	}
	if len(containers) == 0 {
		r.logger.Info("reader has been stopped: no containers have been received from the tracker")
		return NewProcessContainersResult(nil, nil), nil
	}
	r.logger.Info("reader has received the next containers bulk", zap.Int("amount", len(containers)))
	r.metrics.Set(readerTrackerNextContainersMetricName, fmt.Sprintf("%d", time.Since(nextStart).Microseconds()))
	failed := NewContainerIssues()
	read := make([]*Container, 0, len(containers))
	readStart := time.Now()
	for _, container := range containers {
		location := path.Join(container.InputRepository, container.InputIdentifier)
		r.logger.Info("read container start", zap.String("location", location))
		container.Data, err = r.input.Read(container)
		if err != nil {
			if issue, ok := err.(*Issue); ok {
				r.logger.Info("read container result in an issue", zap.String("error", err.Error()))
				failed.Append(container, issue)
				continue
			}
			return nil, fmt.Errorf("read container error: %v", err)
		}
		read = append(read, container)
		r.logger.Info("read container end", zap.String("location", location))
	}
	r.metrics.Set(readerInputReadContainersBulkMetricName, fmt.Sprintf("%d", time.Since(readStart).Microseconds()))
	result := NewProcessContainersResult(read, failed)
	logStepResults(r.logger, "read", result)
	return result, nil
}
