package gobulk

import (
	"fmt"

	"go.uber.org/zap"
)

const (
	executorImportMetricName             = "executor_import"
	executorOperationsBulkSizeMetricName = "executor_operations_bulk_size"
)

// Executor is responsible for actually saving the data
// to the Output by performing Operations.
type Executor struct {
	tracker         Tracker
	output          Output
	executeBulkSize int
	metrics         MetricsTracker
	logger          *zap.Logger
}

// NewExecutor returns a preconfigured Executor struct.
func NewExecutor(
	tracker Tracker,
	output Output,
	executeBulkSize int,
	logger *zap.Logger,
	metricsTracker MetricsTracker,
) *Executor {
	e := &Executor{
		tracker:         tracker,
		output:          output,
		executeBulkSize: executeBulkSize,
		logger:          logger,
	}
	metricsTracker.Add(executorImportMetricName, "Time taken to import all containers of a single containers bulk")
	metricsTracker.Add(e.buildOperationsBulkMetricName(OperationTypeCreate), "Size of operations bulk of create type")
	metricsTracker.Add(e.buildOperationsBulkMetricName(OperationTypeDelete), "Size of operations bulk of delete type")
	metricsTracker.Add(e.buildOperationsBulkMetricName(OperationTypeOmit), "Size of operations bulk of omit type")
	metricsTracker.Add(e.buildOperationsBulkMetricName(OperationTypeUpdate), "Size of operations bulk of update type")
	e.metrics = metricsTracker
	return e
}

// Import executes Operations that are the final result of the bulk Containers. It saves
// all Operations without their actual payload in the Tracker. The order of operations
// execution is the following: Deletes, Updates, Creates, Omits.
// The returned values are occurred issues mapped by corresponding container IDs and an error.
func (e *Executor) Import(containers []*Container) (ContainerIssues, error) {
	e.logger.Info("executor start", zap.Int("bulk_size", len(containers)))
	e.metrics.Start(executorImportMetricName)
	defer e.metrics.Stop(executorImportMetricName)
	operations := make([]*Operation, 0, len(containers))
	for _, c := range containers {
		operations = append(operations, c.Operations...)
	}
	e.logger.Info("executing operations", zap.Int("operations", len(operations)))
	issues, err := e.executeOperations(operations)
	if err != nil {
		return nil, err
	}
	e.logger.Info("tracking containers operations", zap.Int("containers", len(containers)))
	trackResult, err := e.tracker.TrackContainerOperations(containers)
	if err != nil {
		return nil, fmt.Errorf("track container operations error: %v", err)
	}
	mergeContainerIssues(issues, trackResult.Failed)
	completedContainers := make([]*Container, 0, len(trackResult.Succeeded))
	for _, container := range trackResult.Succeeded {
		if container.AllOperationsSucceeded() {
			completedContainers = append(completedContainers, container)
		}
	}
	if len(completedContainers) != 0 {
		e.logger.Info("finishing completed containers", zap.Int("containers", len(completedContainers)))
		finishResult, err := e.tracker.FinishContainers(completedContainers)
		if err != nil {
			return nil, fmt.Errorf("finish containers error: %v", err)
		}
		mergeContainerIssues(issues, finishResult.Failed)
	}
	e.logger.Info("executor end")
	return issues, nil
}

// executeOperations executes the given list of operations. It handles sub operations in recursion
// if they are present. The returned values are occurred issues mapped by corresponding container IDs
// and an error.
func (e *Executor) executeOperations(operations []*Operation) (ContainerIssues, error) {
	if len(operations) == 0 {
		return nil, nil
	}
	failed := NewContainerIssues()
	operationsByType := e.getOperationsByType(operations)
	deleteIssues, err := e.executeAllOperationsByType(operationsByType[OperationTypeDelete], OperationTypeDelete)
	if err != nil {
		return nil, err
	}
	updateIssues, err := e.executeAllOperationsByType(operationsByType[OperationTypeUpdate], OperationTypeUpdate)
	if err != nil {
		return nil, err
	}
	createIssues, err := e.executeAllOperationsByType(operationsByType[OperationTypeCreate], OperationTypeCreate)
	if err != nil {
		return nil, err
	}
	omitIssues, err := e.executeAllOperationsByType(operationsByType[OperationTypeOmit], OperationTypeOmit)
	if err != nil {
		return nil, err
	}
	mergeContainerIssues(failed, deleteIssues, updateIssues, createIssues, omitIssues)
	return failed, nil
}

// getOperationsByType returns the operations mapped by types.
func (e *Executor) getOperationsByType(operations []*Operation) map[OperationType][]*Operation {
	result := make(map[OperationType][]*Operation)
	for _, operation := range operations {
		if operation.Type.Valid() == nil {
			result[operation.Type] = append(result[operation.Type], operation)
		}
	}
	return result
}

// executeAllOperationsByType executes the given list of operations via bulks by the given type.
// It also executes all the inlying suboperations for successfully executed operations. The returned
// values are occurred issues mapped by corresponding container IDs and an error.
func (e *Executor) executeAllOperationsByType(operations []*Operation, opType OperationType) (ContainerIssues, error) {
	failed := NewContainerIssues()
	for _, ops := range e.splitOperations(operations) {
		response, err := e.executeOperationsByType(opType, ops)
		if err != nil {
			return nil, err
		}
		for _, issue := range response.Issues {
			for _, op := range operations {
				if issue.Operation == op {
					failed.Append(op.Container, issue)
				}
			}
		}
		subOps := e.handleSucceededOperations(response.Succeeded)
		subIssues, err := e.executeOperations(subOps)
		if err != nil {
			return nil, err
		}
		mergeContainerIssues(failed, subIssues)
	}
	return failed, nil
}

// splitOperations splits the given list of operations to chunks.
func (e *Executor) splitOperations(operations []*Operation) [][]*Operation {
	chunks := make([][]*Operation, 0, len(operations)/e.executeBulkSize+1)
	for len(operations) >= e.executeBulkSize {
		chunk := operations[:e.executeBulkSize]
		operations = operations[e.executeBulkSize:]
		chunks = append(chunks, chunk)
	}
	if len(operations) > 0 {
		chunks = append(chunks, operations)
	}
	return chunks
}

// executeOperationsByType executes the given list of operations by type.
func (e *Executor) executeOperationsByType(operationType OperationType, operations []*Operation) (*OutputResponse, error) {
	e.metrics.Set(e.buildOperationsBulkMetricName(operationType), fmt.Sprintf("%d", len(operations)))
	switch operationType {
	case OperationTypeCreate:
		e.logger.Info("executor create", zap.Int("operation count", len(operations)))
		return e.output.Create(operations...)
	case OperationTypeUpdate:
		e.logger.Info("executor update", zap.Int("operation count", len(operations)))
		return e.output.Update(operations...)
	case OperationTypeDelete:
		e.logger.Info("executor delete", zap.Int("operation count", len(operations)))
		return e.output.Delete(operations...)
	case OperationTypeOmit:
		e.logger.Info("executor omit", zap.Int("operation count", len(operations)))
		return &OutputResponse{Succeeded: operations}, nil
	}
	return nil, fmt.Errorf("invalid type of operations: %s", operationType)
}

// handleSucceededOperations handles the given list of operations.
func (e *Executor) handleSucceededOperations(operations []*Operation) []*Operation {
	var subOperations []*Operation
	for _, operation := range operations {
		operation.Success = true
		subOperations = append(subOperations, operation.SubOperations...)
	}
	return subOperations
}

// buildOperationsBulkMetricName concatenates executorOperationsBulkSizeMetricName with
// the passed opType using underscore.
func (Executor) buildOperationsBulkMetricName(opType OperationType) string {
	return fmt.Sprintf("%s_%s", executorOperationsBulkSizeMetricName, opType)
}
