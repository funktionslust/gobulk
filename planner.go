package gobulk

import (
	"fmt"
	"path"
	"sort"
	"sync"

	"go.uber.org/zap"
)

const (
	plannerPlanBulkOperationsMetricName = "planner_plan_bulk_operations"
)

// Planner is responsible for using the Format's definitions to convert containers
// elements into executable Operations (Create Update Delete Omit).
type Planner struct {
	plan           func(container *Container, inputElements []Element) ([]*Operation, error)
	chunkSize      int
	metricsTracker MetricsTracker
	logger         *zap.Logger
}

// NewPlanner returns a preconfigured Planner struct.
func NewPlanner(
	plan func(container *Container, inputElements []Element) ([]*Operation, error),
	chunkSize int,
	metricsTracker MetricsTracker,
	logger *zap.Logger,
) *Planner {
	metricsTracker.Add(plannerPlanBulkOperationsMetricName, "Time taken to plan all operations of a single containers bulk")
	return &Planner{
		plan:           plan,
		chunkSize:      chunkSize,
		metricsTracker: metricsTracker,
		logger:         logger,
	}
}

// PlanBulkOperations converts the bulk containers elements into Operations based on the Format's
// planning logic and assigns the result operations to the corresponding containers. It returns
// successfully planned containers bulk, issues mapped by failed container IDs and an error.
func (p *Planner) PlanBulkOperations(containers []*Container) (*ProcessContainersResult, error) {
	p.logger.Info("planner start", zap.Int("bulk_size", len(containers)))
	p.metricsTracker.Start(plannerPlanBulkOperationsMetricName)
	defer p.metricsTracker.Stop(plannerPlanBulkOperationsMetricName)
	failed := NewContainerIssues()
	planned, errors := p.planContainers(containers)
	if len(errors) != 0 {
		for container, err := range errors {
			if issue, ok := err.(*Issue); ok {
				failed.Append(container, issue)
			} else {
				return nil, fmt.Errorf("plan container error: %v", err)
			}
		}
	}
	result := NewProcessContainersResult(planned, failed)
	logStepResults(p.logger, "plan", result)
	return result, nil
}

// planContainers plans the containers operations and returns a successfully planned containers
// slice sorted by container.ID and issues mapped by corresponding containers.
func (p *Planner) planContainers(containers []*Container) ([]*Container, map[*Container]error) {
	plannedMu := &sync.Mutex{}
	planned := make([]*Container, 0, len(containers))
	failedMu := &sync.Mutex{}
	failed := make(map[*Container]error)
	for _, chunk := range splitContainers(containers, p.chunkSize) {
		wg := &sync.WaitGroup{}
		for _, container := range chunk {
			wg.Add(1)
			go func(container *Container) {
				defer wg.Done()
				if err := p.planContainer(container); err != nil {
					failedMu.Lock()
					defer failedMu.Unlock()
					failed[container] = err
				} else {
					plannedMu.Lock()
					defer plannedMu.Unlock()
					planned = append(planned, container)
				}
			}(container)
		}
		wg.Wait()
	}
	sort.SliceStable(planned, func(i, j int) bool {
		return planned[i].ID < planned[j].ID
	})
	return planned, failed
}

// planContainer plans the container operations and populates its Operations field with the result.
func (p *Planner) planContainer(container *Container) error {
	location := path.Join(container.InputRepository, container.InputIdentifier)
	p.logger.Info("container plan start",
		zap.String("location", location),
		zap.Int("element count", len(container.Elements)),
	)
	defer p.logger.Info("container plan end", zap.String("location", location))
	containerOperations, err := p.plan(container, container.Elements)
	if err != nil {
		return err
	}
	container.Operations = containerOperations
	container.Elements = nil
	return nil
}
