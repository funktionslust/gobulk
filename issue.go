package gobulk

import (
	"encoding/json"
	"fmt"
	"time"
)

// NewIssue returns a new *Issue populated with the passed parameters. In spite of the fact that
// the result issue lacks several fields, the issue is ready to be returned to the gobulk pipeline
// cause the missing fields will be then populated by gobulk itself.
func NewIssue(
	err error,
	note string,
	issueType IssueType,
	payload string,
) *Issue {
	return &Issue{
		Type:    issueType,
		Payload: payload,
		Note:    note,
		Created: time.Now(),
		Err:     err,
	}
}

// NewExecutionIssue returns a new executor *Issue populated with the passed parameters. In spite of
// the fact that the result issue lacks several fields, the issue is ready to be returned to the
// gobulk pipeline cause the missing fields will be then populated by gobulk itself.
func NewExecutionIssue(
	err error,
	note string,
	container *Container,
	operation *Operation,
	issueType IssueType,
	payload string,
) *Issue {
	return &Issue{
		Container: container,
		Operation: operation,
		Step:      StepExecutor,
		Type:      issueType,
		Payload:   payload,
		Note:      note,
		Created:   time.Now(),
		Err:       err,
	}
}

// Issue represents an error or problem that's happened during processing. It's used
// by tracker to mark containers which failed at one of the steps.
type Issue struct {
	ID        uint64     `json:"id"`
	Iteration *Iteration `json:"-"`
	Container *Container `json:"-"`
	Operation *Operation `json:"-"`
	Step      Step       `json:"step"`
	Type      IssueType  `json:"type"`
	Payload   string     `json:"payload,omitempty"`
	Note      string     `json:"note,omitempty"`
	Handled   *time.Time `json:"handled"`
	Created   time.Time  `json:"created"`
	Err       error      `json:"err"`
}

// Error makes the Issue type implement Error interface.
func (i *Issue) Error() string {
	if d, err := json.Marshal(i); err == nil {
		return string(d)
	}
	return fmt.Sprintf("%+v", *i)
}

// MarshalJSON overrides the default MarshalJSON method in order to make it possible to represent
// the issue iteration, container and operation IDs instead of the structures.
func (i *Issue) MarshalJSON() ([]byte, error) {
	var iterationID, containerID, operationID int64
	if i.Iteration != nil {
		iterationID = int64(i.Iteration.ID)
	}
	if i.Container != nil {
		containerID = int64(i.Container.ID)
	}
	if i.Operation != nil {
		operationID = int64(i.Operation.ID)
	}
	type Alias Issue
	return json.Marshal(&struct {
		IterationID int64 `json:"iteration_id"`
		ContainerID int64 `json:"container_id,omitempty"`
		OperationID int64 `json:"operation_id,omitempty"`
		*Alias
	}{
		IterationID: iterationID,
		ContainerID: containerID,
		OperationID: operationID,
		Alias:       (*Alias)(i),
	})
}

// complete finishes the issue definition by setting its last fields left unknown.
func (i *Issue) complete(Iteration *Iteration, container *Container, step Step) {
	i.Iteration = Iteration
	i.Container = container
	i.Step = step
}

// NewContainerIssues initialises a new instance of ContainerIssues.
func NewContainerIssues() ContainerIssues {
	return make(ContainerIssues)
}

// ContainerIssues is a list of issues mapped by corresponding containers.
type ContainerIssues map[*Container][]*Issue

// Append registers the passed issues for the given container.
func (i ContainerIssues) Append(container *Container, issues ...*Issue) {
	i[container] = append(i[container], issues...)
}

// mergeContainerIssues merges two issue maps.
func mergeContainerIssues(dst ContainerIssues, sources ...ContainerIssues) {
	if dst == nil {
		dst = NewContainerIssues()
	}
	for _, src := range sources {
		for container, issues := range src {
			dst[container] = append(dst[container], issues...)
		}
	}
}

// IssueType defines the kind of an issue within the gobulk process. It can be used to logically
// group issues.
type IssueType string

const (
	// IssueTypeInfrastructure issues that have been caused by infrastructure malfunction.
	IssueTypeInfrastructure IssueType = "infrastructure"
	// IssueTypeDataIntegrity describes issues that have been caused by broken data.
	IssueTypeDataIntegrity IssueType = "data_integrity"
	// IssueTypePersistance describes issues that have been caused by data not being saved or deleted
	IssueTypePersistance IssueType = "data_persistance"
	// IssueTypeParsing describes issues that have been caused by data not being parsed
	IssueTypeParsing IssueType = "data_parsing"
)

// String converts a IssueType to string.
func (i IssueType) String() string {
	return string(i)
}
