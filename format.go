package gobulk

import (
	"time"

	"go.uber.org/zap"
)

// Format contains methods that describe specific data handling pipeline and provide storages and
// other configuration engaged in import process.
type Format interface {
	// Name returns the name of the format.
	Name() string
	// Setup will be called once when creating a new runner, it can be used to preconfigure or
	// initialise the format for forthcoming import process.
	Setup() error
	// SetIteration is used to later give access to the current iteration using the format.
	SetIteration(iteration *Iteration)
	// NewIterationOnRestart defines whether after every restart a complete re-import should be
	// performed.
	NewIterationOnRestart() bool
	// Tracker returns the format specific tracker.
	Tracker() Tracker
	// Input returns the format specific input.
	Input() Input
	// Output returns the format specific output.
	Output() Output
	// Parse processes the input element RawData and populates the element ParsedData with the result.
	Parse(container *Container, input Element) (Element, error)
	// Plan creates executable operations based on the element.
	Plan(container *Container, inputElements []Element) ([]*Operation, error)
	// ReadStrategy defines the order of containers read from the input. I.e. whether the first or the
	// last tracked container should be the starting import point.
	ReadStrategy() Strategy
	// ContainerBulkSize defines how many containers should be read at a time and then grouped into
	// one bulk. This can be handy if the data to be imported is split across many small containers,
	// e.g. one file for each document to import instead of one file containing multiple documents.
	// To boost performance you can increase the container bulk size.
	ContainerBulkSize() int
	// ExecutorBulkSize returns the number of operations to perform by executor at a time. This
	// value as well as the ContainerBulkSize can be increased in case of small numerous containers.
	// It decreases the amount of distinct calls for the Output operations execution by grouping
	// operations to bulks.
	ExecutorBulkSize() int
	// StopOnError returns whether the format processing should be stopped once an error occurred
	// with a single container at any step. The true value could mean e.g. that each container
	// depends on previous ones. Otherwise, it's possible to track errors as Issues in the Tracker.
	StopOnError() bool
	// BeforeIssue gets called whenever an issue gets tracked, it can be used for general format
	// specific modifications or for sending notifications or automated infrastructure tasks.
	BeforeIssue(issue *Issue) *Issue
	// MetricsTracker returns a gobulk.MetricsTracker instance to be used alongside the format.
	MetricsTracker() MetricsTracker
	// Logger returns the logger to be used alongside the format.
	Logger() *zap.Logger
	// ExecutionShouldBeIntermitted gets executed before importing, if there's a need to wait for
	// something it'll return a time in the future, otherwise nil.
	ExecutionShouldBeIntermitted() (*time.Time, error)
	// ExecutionIsIntermitted returns whether the format execution is still intermitted.
	ExecutionIsIntermitted() bool
	// SetIntermitUntil is used by the runner to set the ExecutionShouldBeIntermitted result to
	// IntermitUntil.
	SetIntermitUntil(t *time.Time)
}

const (
	// defaultNewIterationOnRestart is the default result value for the BaseFormat NewIterationOnRestart
	// method which means not to create a new iteration on restarts.
	defaultNewIterationOnRestart bool = false
	// defaultReadStrategy is the default result value for the BaseFormat ReadStrategy method which
	// means to read containers from the input from the first to the last.
	defaultReadStrategy Strategy = StrategyFIFO
	// defaultContainerBulkSize is the default result value for the BaseFormat ContainerBulkSize
	// method which means to read containers from the input one by one.
	defaultContainerBulkSize int = 1
	// defaultExecutorBulkSize is the default result value for the BaseFormat ExecutorBulkSize
	// method which means to execute operations one by one.
	defaultExecutorBulkSize int = 1
	// defaultStopOnError is the default result value for the BaseFormat StopOnError method which
	// means to stop the import process on the first occurred error.
	defaultStopOnError bool = true
)

var (
	// defaultMetricsTracker is the default value for the BaseFormat MetricsTracker method which
	// means to skip any metrics tracking.
	defaultMetricsTracker MetricsTracker = emptyMetricsTracker{}
)

// buildDefaultLogger creates a default value for the BaseFormat Logger method which commits the
// debug and higher level logs supplemented with the format name as the "context" field value.
func buildDefaultLogger(context string) *zap.Logger {
	logger, _ := zap.NewDevelopment()
	logger = logger.With(zap.String("context", context))
	return logger
}

// FormatOpt is a type that modifies the default BaseFormat behaviour.
type FormatOpt func(f *BaseFormat)

// FormatWithNewIterationOnRestart makes the import process create a new iteration instance each time
// the format is initialised.
var FormatWithNewIterationOnRestart = func() func(f *BaseFormat) {
	return func(f *BaseFormat) {
		f.newIterationOnRestart = true
	}
}

// FormatWithBackwardImport makes the import process read containers from the input from the last
// one to the first one.
var FormatWithBackwardImport = func() func(f *BaseFormat) {
	return func(f *BaseFormat) {
		f.readStrategy = StrategyLIFO
	}
}

// FormatWithContainerBulkSize makes the import process read, parse and plan containers from the
// input by bulks of the specified size.
var FormatWithContainerBulkSize = func(size int) func(f *BaseFormat) {
	return func(f *BaseFormat) {
		f.containerBulkSize = size
	}
}

// FormatWithExecutorBulkSize makes the import execute operations by bulks of the specified size.
var FormatWithExecutorBulkSize = func(size int) func(f *BaseFormat) {
	return func(f *BaseFormat) {
		f.executorBulkSize = size
	}
}

// FormatWithIssuesTracking prevents the import from being stopped when issues occur. Instead, issues
// are saved in the Tracker and the import process remains running with the issued containers skipped.
var FormatWithIssuesTracking = func() func(f *BaseFormat) {
	return func(f *BaseFormat) {
		f.stopOnError = false
	}
}

// FormatWithMetricsTracker makes the import track metrics using the specified MetricsTracker.
var FormatWithMetricsTracker = func(tracker MetricsTracker) func(f *BaseFormat) {
	return func(f *BaseFormat) {
		f.metricsTracker = tracker
	}
}

// FormatWithLogger enhances the format with the passed logger which will be used in import logging.
var FormatWithLogger = func(logger *zap.Logger) func(f *BaseFormat) {
	return func(f *BaseFormat) {
		f.logger = logger
	}
}

// NewBaseFormat creates a new instance of BaseFormat. By default, a BaseFormat with the default
// settings is created and returned, but it's possible to modify the further import process behaviour
// by config optional parameters usage. This func must be a part of all format constructors.
func NewBaseFormat(name string, tracker Tracker, input Input, output Output, opts ...FormatOpt) BaseFormat {
	f := BaseFormat{
		name:                  name,
		tracker:               tracker,
		input:                 input,
		output:                output,
		newIterationOnRestart: defaultNewIterationOnRestart,
		readStrategy:          defaultReadStrategy,
		containerBulkSize:     defaultContainerBulkSize,
		executorBulkSize:      defaultExecutorBulkSize,
		stopOnError:           defaultStopOnError,
		metricsTracker:        defaultMetricsTracker,
		logger:                buildDefaultLogger(name),
	}
	for _, opt := range opts {
		opt(&f)
	}
	return f
}

// BaseFormat must be embedded to all formats.
type BaseFormat struct {
	Iteration             *Iteration
	tracker               Tracker
	input                 Input
	output                Output
	name                  string
	newIterationOnRestart bool
	intermitUntil         *time.Time
	readStrategy          Strategy
	containerBulkSize     int
	executorBulkSize      int
	stopOnError           bool
	metricsTracker        MetricsTracker
	logger                *zap.Logger
}

// Name returns the name of the format.
func (f *BaseFormat) Name() string {
	return f.name
}

// SetIteration is used to later give access to the current iteration using the format.
func (f *BaseFormat) SetIteration(iteration *Iteration) {
	f.Iteration = iteration
}

// NewIterationOnRestart defines whether after every restart a complete re-import should be
// performed.
func (f *BaseFormat) NewIterationOnRestart() bool {
	return f.newIterationOnRestart
}

// Tracker returns the format specific tracker.
func (f *BaseFormat) Tracker() Tracker {
	return f.tracker
}

// Input returns the format specific input.
func (f *BaseFormat) Input() Input {
	return f.input
}

// Output returns the format specific output.
func (f *BaseFormat) Output() Output {
	return f.output
}

// ReadStrategy defines the order of containers read from the input. I.e. whether the first or the
// last tracked container should be the starting import point.
func (f *BaseFormat) ReadStrategy() Strategy {
	return f.readStrategy
}

// ContainerBulkSize defines how many containers should be read at a time and then grouped into
// one bulk. This can be handy if the data to be imported is split across many small containers,
// e.g. one file for each document to import instead of one file containing multiple documents.
// To boost performance you can increase the container bulk size.
func (f *BaseFormat) ContainerBulkSize() int {
	return f.containerBulkSize
}

// ExecutorBulkSize returns the number of operations to perform by executor at a time. This value
// as well as the ContainerBulkSize can be increased in case of small numerous containers. It
// decreases the amount of distinct calls for the Output operations execution by grouping
// operations to bulks.
func (f *BaseFormat) ExecutorBulkSize() int {
	return f.executorBulkSize
}

// StopOnError returns whether the format processing should be stopped once an error occurred
// with a single container at any step. The true value could mean e.g. that each container
// depends on previous ones. Otherwise, it's possible to track errors as Issues in the Tracker.
func (f *BaseFormat) StopOnError() bool {
	return f.stopOnError
}

// BeforeIssue simply returns the issue without modifying it.
func (f *BaseFormat) BeforeIssue(issue *Issue) *Issue {
	return issue
}

// MetricsTracker returns a gobulk.MetricsTracker instance to be used alongside the format.
func (f *BaseFormat) MetricsTracker() MetricsTracker {
	return f.metricsTracker
}

// Logger returns the logger to be used alongside the format.
func (f *BaseFormat) Logger() *zap.Logger {
	return f.logger
}

// ExecutionShouldBeIntermitted gets executed before importing, if there's a need to wait for
// something it'll return a time in the future, otherwise nil.
func (f *BaseFormat) ExecutionShouldBeIntermitted() (*time.Time, error) {
	return nil, nil
}

// ExecutionIsIntermitted returns whether the format execution is still intermitted.
func (f *BaseFormat) ExecutionIsIntermitted() bool {
	if f.intermitUntil == nil {
		return false
	}
	if f.intermitUntil.Before(time.Now()) {
		f.SetIntermitUntil(nil)
		return false
	}
	return true
}

// SetIntermitUntil is used by the runner to set the ExecutionShouldBeIntermitted result to
// IntermitUntil.
func (f *BaseFormat) SetIntermitUntil(t *time.Time) {
	f.intermitUntil = t
}
