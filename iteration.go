package gobulk

import (
	"time"

	"go.uber.org/zap"
)

// Iteration is the task to import a format from the first to the last container
type Iteration struct {
	// ID should be used as identifier of the iteration in tracker
	ID uint64
	// Number should be used to identify the iteration (re-importing a format should result in increasing the number)
	Number uint
	// Format referrs to the format the iteration is about
	Format Format
	// When the iteration / import task has been created. This can be used to track how long a "full" import took
	Created time.Time
	// When the iteration / import task has been changed the last time. This can be used to find out how long the iteration took
	Modified *time.Time
	// The last container that has been tracked. This should be used by the listener to not start from scratch on every execution
	LastTrackedContainer *Container
	// The last container that has been sucessfully processed.
	LastProcessedContainer *Container
	// Input defines where to get the containers from e.g. objects from AWS S3 or files from a filesystem
	Input Input
	// Output defines where to store the data e.g. to Elasticsearch or a filesystem
	Output Output
	// Tracker is used to store the listening protocol and track errors
	Tracker Tracker
}

// GetIteration returns a new or the format latest iteration based on the format NewIterationOnRestart
// configuration. The result iteration is preset with the format, tracker, input and output before
// being returned.
func GetIteration(format Format, tracker Tracker) (*Iteration, error) {
	iteration, err := tracker.CurrentIteration(format)
	if err != nil {
		return nil, err
	}
	if format.NewIterationOnRestart() || iteration == nil {
		var number uint = 1
		if iteration != nil {
			number = iteration.Number + 1
		}
		iteration, err = tracker.NewIteration(format, number)
		if err != nil {
			return nil, err
		}
	}
	format.SetIteration(iteration)
	format.Logger().Info("iteration has been set for the format",
		zap.Int("iteration_number", int(iteration.Number)),
		zap.Int("iteration_id", int(iteration.ID)),
	)
	return iteration, nil
}
