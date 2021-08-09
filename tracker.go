package gobulk

// TrackerNextContainersOpt represents optional paratemers which could be used to modify the tracker
// behaviour in the NextContainers method.
type TrackerNextContainersOpt int

const (
	// TrackerNextContainersOptNoLock prevents containers from being locked.
	TrackerNextContainersOptNoLock TrackerNextContainersOpt = iota
	// TrackerNextContainersOptOnlyNew enhances the query to get only containers which got to the
	// tracker after the last processed one.
	TrackerNextContainersOptOnlyNew
)

// Tracker represents a storage that acts as a registry for import iterations. It tracks iteration
// state and details and state of containers, issues and operations. The Tracker interface is used
// to track the import progress.
type Tracker interface {
	Storage
	// CurrentIteration retrieves the current iteration state of the passed format. A completely
	// populated iteration is expected as the result with all the fields set.
	CurrentIteration(format Format) (*Iteration, error)
	// NewIteration creates a new iteration based on the format definitions and saves it in the tracker.
	// A completely populated iteration is expected as the result with all the fields set.
	NewIteration(format Format, number uint) (*Iteration, error)
	// GetUnfinishedContainers returns a list of containers which have already been tracked but haven't
	// yet been finished.
	GetUnfinishedContainers() ([]*Container, error)
	// TrackContainers tracks the containers in the slice and updates corresponding Iteration last
	// tracked container with the last one in the slice.
	TrackContainers(containers []*Container) (*TrackContainersResponse, error)
	// NextContainers by default, searches for and returns new processable containers and locks them
	// (marks as started). However, it's possible to modify the method behaviour by the opts parameter.
	NextContainers(readStrategy Strategy, number int, opts ...TrackerNextContainersOpt) ([]*Container, error)
	// TrackContainerOperations persists the containers operations and their error/success status.
	TrackContainerOperations(container []*Container) (*ProcessContainersResult, error)
	// FinishContainers sets the containers state as successfully and completely imported.
	FinishContainers(container []*Container) (*ProcessContainersResult, error)
	// TrackIssue tracks the issue.
	TrackIssue(issue *Issue) error
}

// TrackContainersResponse represents a successful result of a TrackContainers call.
type TrackContainersResponse struct {
	// Tracked is a subslice of containers to track which are new and have been tracked.
	Tracked []*Container
	// Conflicted is a subslice of containers to track which already exist in the tracker.
	Conflicted []*Container
}
