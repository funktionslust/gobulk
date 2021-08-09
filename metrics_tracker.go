package gobulk

// MetricsTracker registers and measures pipeline steps duration and instanc metrics.
type MetricsTracker interface {
	// Add registers the measurement in the metrics tracker with the following description.
	Add(measurement, description string)
	// Start launches the measurement duration timer.
	Start(measurement string)
	// Stop stops the measurement timer and registers the time diff in the metrics tracker.
	Stop(measurement string)
	// Set registers the measurement value in the metrics tracker. Should be used to register
	// instant metrics.
	Set(measurement, value string)
}

// emptyMetricsTracker is used when no metrics tracker is needed. It just does nothing on every call.
type emptyMetricsTracker struct{}

func (emptyMetricsTracker) Add(measurement, description string) {}
func (emptyMetricsTracker) Start(measurement string)            {}
func (emptyMetricsTracker) Stop(measurement string)             {}
func (emptyMetricsTracker) Set(measurement, value string)       {}
