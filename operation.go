package gobulk

import (
	"errors"
	"time"
)

// Operation represents a data segment like a file, it contains raw data that will be parsed into elements.
type Operation struct {
	ID               uint64
	Iteration        *Iteration
	Container        *Container
	Type             OperationType
	OutputRepository string
	OutputIdentifier string
	Success          bool
	Created          time.Time
	Data             interface{}
	SubOperations    []*Operation
}

// GetSubOperations returns all suboperations recursively.
func (o *Operation) GetSubOperations() []*Operation {
	subops := make([]*Operation, 0, len(o.SubOperations))
	for _, subop := range o.SubOperations {
		subops = append(subops, subop)
		subops = append(subops, subop.GetSubOperations()...)
	}
	return subops
}

// HasFailedSubOperations checks whether at least one of o sub operations resulted with failure.
func (o *Operation) HasFailedSubOperations() bool {
	for _, subOp := range o.SubOperations {
		if !subOp.Success || subOp.HasFailedSubOperations() {
			return true
		}
	}
	return false
}

// OperationType defines what a operation can do.
type OperationType string

const (
	// OperationTypeCreate whether to add something to the output.
	OperationTypeCreate OperationType = "create"
	// OperationTypeUpdate whether to update something of the output.
	OperationTypeUpdate OperationType = "update"
	// OperationTypeDelete whether to delete something from the output.
	OperationTypeDelete OperationType = "delete"
	// OperationTypeOmit whether to keep everything as it is. use it if data are already saved correctly in output.
	OperationTypeOmit OperationType = "omit"
)

// Valid checks whether the assigned operation type value is valid.
func (t OperationType) Valid() error {
	switch t {
	case OperationTypeCreate, OperationTypeUpdate, OperationTypeDelete, OperationTypeOmit:
		return nil
	}
	return errors.New("invalid operation type")
}
