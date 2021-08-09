package gobulk

import (
	"time"
)

// Container represents a data segment like a file, it contains raw data that will be parsed into elements
type Container struct {
	// TODO add comments to the fields
	ID              uint64
	IterationID     uint64
	Operations      []*Operation
	Elements        []Element
	Created         time.Time
	Started         *time.Time
	Finished        *time.Time
	InputRepository string
	InputIdentifier string
	Size            uint64
	LastModified    time.Time
	ContentHash     string
	Data            map[string][]byte
	Note            string
	// ProcessID has to be used if multiple instances of the import process use the same iteration in the same tracker
	ProcessID string
}

// AllOperationsSucceeded returns true if c operation and all its sub operations have completed successfully.
func (c *Container) AllOperationsSucceeded() bool {
	for _, operation := range c.Operations {
		if !operation.Success || operation.HasFailedSubOperations() {
			return false
		}
	}
	return true
}

// GetAllOperations returns all operations and suboperations of the container.
func (c *Container) GetAllOperations() []*Operation {
	ops := make([]*Operation, 0, len(c.Operations))
	for _, op := range c.Operations {
		ops = append(ops, op)
		ops = append(ops, op.GetSubOperations()...)
	}
	return ops
}

// NewProcessContainersResult returns a new instance of *ProcessContainersResult.
func NewProcessContainersResult(succeeded []*Container, failed ContainerIssues) *ProcessContainersResult {
	if failed == nil {
		failed = NewContainerIssues()
	}
	return &ProcessContainersResult{Succeeded: succeeded, Failed: failed}
}

// ProcessContainersResult represents the result of an operation performed using a slice of containers
// which outcome may include successfully processed containers as well as ones which failed.
type ProcessContainersResult struct {
	Succeeded []*Container
	Failed    ContainerIssues
}

// FailContainers creates issues for the passed slice of containers and adds them to the result Failed
// list.
func (r *ProcessContainersResult) FailContainers(cs []*Container, err error, note, payload string, iType IssueType) {
	for _, c := range cs {
		r.Failed.Append(c, NewIssue(err, note, iType, payload))
	}
}

// splitContainers splits the given list of containers to chunks.
func splitContainers(containers []*Container, chunkSize int) [][]*Container {
	chunks := make([][]*Container, 0, len(containers)/chunkSize+1)
	for len(containers) >= chunkSize {
		chunk := containers[:chunkSize]
		containers = containers[chunkSize:]
		chunks = append(chunks, chunk)
	}
	if len(containers) > 0 {
		chunks = append(chunks, containers)
	}
	return chunks
}
