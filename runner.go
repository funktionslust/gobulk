package gobulk

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

const (
	runnerBulkProcessingMetricName = "runner_bulk_processing"
	runnerReadBulkMetricName       = "runner_read_bulk"
)

var (
	errFormatIsIntermitted = errors.New("format is intermitted")
)

// RunnerConfig represents a structure for the Runner config.
type RunnerConfig struct {
	Format             Format
	ProcessIDPrefix    string
	ListenerState      SwitcherState
	ScanFromScratch    bool
	ProcessState       SwitcherState
	ParseChunkSize     int
	PlanChunkSize      int
	LoaderWorkersLimit int
}

// Validate validates the RunnerConfig fields.
func (c *RunnerConfig) Validate() error {
	if c.ParseChunkSize <= 0 {
		return fmt.Errorf("invalid ParseChunkSize value %d: should be more than 0", c.ParseChunkSize)
	}
	if c.PlanChunkSize <= 0 {
		return fmt.Errorf("invalid PlanChunkSize value %d: should be more than 0", c.PlanChunkSize)
	}
	return nil
}

// NewRunner returns a preconfigured runner struct.
func NewRunner(ctx context.Context, cfg RunnerConfig) (*Runner, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("the passed RunnerConfig is invalid: %v", err)
	}
	logger := cfg.Format.Logger()
	processID := buildProcessID(cfg.ProcessIDPrefix, cfg.Format.Name())
	tracker, input, output, err := initFormatStorages(cfg.Format, ctx, processID, logger)
	if err != nil {
		return nil, err
	}
	iteration, err := GetIteration(cfg.Format, tracker)
	if err != nil {
		return nil, err
	}
	var marker *Container
	if !cfg.ScanFromScratch {
		marker = iteration.LastTrackedContainer
	}
	listener, err := NewListener(iteration, tracker, input, marker, logger, cfg.ListenerState, cfg.LoaderWorkersLimit)
	if err != nil {
		return nil, err
	}
	metricsTracker := cfg.Format.MetricsTracker()
	metricsTracker.Add(runnerBulkProcessingMetricName, "Time taken to process a single containers bulk")
	metricsTracker.Add(runnerReadBulkMetricName, "Time taken to read a single containers bulk")
	reader := NewReader(tracker, input, iteration.Format.ContainerBulkSize(), iteration.Format.ReadStrategy(), logger, metricsTracker)
	parser := NewParser(iteration.Format.Parse, cfg.ParseChunkSize, metricsTracker, logger)
	planner := NewPlanner(iteration.Format.Plan, cfg.PlanChunkSize, metricsTracker, logger)
	executor := NewExecutor(tracker, output, iteration.Format.ExecutorBulkSize(), logger, metricsTracker)
	return &Runner{
		Iteration: iteration,
		Listener:  listener,
		Reader:    reader,
		Parser:    parser,
		Planner:   planner,
		Executor:  executor,
		switcher:  newSwitcher(cfg.ProcessState),
		metrics:   metricsTracker,
		logger:    logger,
	}, nil
}

// Run executes one or multiple runners. Once one runner has processed everything and is idle, the next runner starts.
// The interval defines how long to wait between walk throughs.
func Run(ctx context.Context, interval time.Duration, runners ...*Runner) error {
	controller := newRunnersController(runners)
	defer controller.ShutdownRunners()
	for {
		start := time.Now()
		if err := controller.ExecRunners(ctx); err != nil {
			return err
		}
		if interval == 0 {
			return nil
		}
		if time.Since(start) < interval {
			time.Sleep(interval)
		}
	}
}

// Runner bundles the logic to retrieve a Container from Tracker, parse, plan and finally import it.
type Runner struct {
	Iteration *Iteration
	Listener  *Listener
	Reader    *Reader
	Parser    *Parser
	Planner   *Planner
	Executor  *Executor
	*switcher
	metrics MetricsTracker
	logger  *zap.Logger
}

// run in accordance with configuration runs or idles listen and import processes
// and keeps them switchable.
func (r *Runner) run(outCtx context.Context) error {
	r.logger.Info(fmt.Sprintf("runner is running"))
	var runErr error
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(outCtx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := r.Listener.Listen(ctx); err != nil {
			r.logger.Error(fmt.Sprintf("listener failed with an error: %v", err))
			cancel()
			runErr = err
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := r.runImport(ctx); err != nil && err != errFormatIsIntermitted {
			r.logger.Error(fmt.Sprintf("import failed with an error: %v", err))
			runErr = err
		}
		cancel()
	}()
	wg.Wait()
	return runErr
}

// runImport listens for the Reader to get new containers and processes all the gotten containers.
func (r *Runner) runImport(ctx context.Context) error {
	if r.Listener.IsOn() && !r.Listener.Ready() {
		if canceled := !r.waitForListenerTurnOn(ctx); canceled {
			return nil
		}
	}
	if r.IsOff() {
		if canceled := !r.waitForTurnOn(ctx); canceled {
			return nil
		}
	}
	if r.Iteration.Format.ExecutionIsIntermitted() {
		r.logger.Info(fmt.Sprintf("format %s is intermitted", r.Iteration.Format.Name()))
		return errFormatIsIntermitted
	} else {
		if intermittence, err := r.Iteration.Format.ExecutionShouldBeIntermitted(); err != nil {
			return err
		} else if intermittence != nil {
			r.Iteration.Format.SetIntermitUntil(intermittence)
			r.logger.Info(fmt.Sprintf("format %s has been set to intermitted until %s", r.Iteration.Format.Name(), intermittence.Format(time.RFC1123Z)))
			return errFormatIsIntermitted
		}
	}
	for {
		if r.IsOff() {
			if canceled := !r.waitForTurnOn(ctx); canceled {
				return nil
			}
		} else {
			select {
			case <-r.Off():
				r.logger.Info("import has been turned off")
			case <-ctx.Done():
				r.logger.Info("import has been stopped by context")
				return nil
			default:
				readStart := time.Now()
				readResult, err := r.Reader.NextContainersBulk()
				if err := r.handleProcessContainersResult(StepReader, readResult, err); err != nil {
					return err
				}
				if readResult == nil || len(readResult.Succeeded) == 0 {
					if err == nil {
						r.logger.Info("import has been stopped: all existing containers have been imported")
						return nil
					}
					continue
				}
				r.metrics.Set(runnerReadBulkMetricName, fmt.Sprintf("%d", time.Since(readStart).Microseconds()))
				bulkProcessStart := time.Now()
				parseResult, err := r.Parser.ParseBulkElements(readResult.Succeeded)
				if err := r.handleProcessContainersResult(StepParser, parseResult, err); err != nil {
					return err
				}
				if parseResult == nil || len(parseResult.Succeeded) == 0 {
					continue
				}
				planResult, err := r.Planner.PlanBulkOperations(parseResult.Succeeded)
				if err := r.handleProcessContainersResult(StepPlanner, planResult, err); err != nil {
					return err
				}
				if planResult == nil || len(planResult.Succeeded) == 0 {
					continue
				}
				r.completeContainerOperations(planResult.Succeeded)
				failed, err := r.Executor.Import(planResult.Succeeded)
				if err != nil {
					return err
				}
				if err := r.handleImportIssues(StepExecutor, failed); err != nil {
					return err
				}
				r.metrics.Set(runnerBulkProcessingMetricName, fmt.Sprintf("%d", time.Since(bulkProcessStart).Microseconds()))
			}
		}
	}
}

// waitForTurnOn waits for a turn on switch for the runner and returns true, if it happened, and
// false, if the ctx has been stopped during the wait.
func (r *Runner) waitForTurnOn(ctx context.Context) bool {
	r.logger.Info("import is off and waiting to be launched")
	select {
	case <-r.On():
		r.logger.Info("starting import")
		return true
	case <-ctx.Done():
		r.logger.Info("import has been stopped by context")
		return false
	}
}

// waitForListenerTurnOn waits for a turn on switch for the runner's listener and returns true, if
// it happened, and false, if the ctx has been stopped during the wait.
func (r *Runner) waitForListenerTurnOn(ctx context.Context) bool {
	r.logger.Info("import is waiting for the listener to be ready")
	select {
	case <-r.Listener.ReadyChan():
		r.logger.Info("import is no more blocked by listener")
		return true
	case <-ctx.Done():
		r.logger.Info("import has been stopped by context")
		return false
	}
}

// setup setups the runner. It's called only once before executing the runner.
func (r *Runner) setup() error {
	r.logger.Info("setting up the runner")
	if err := r.Iteration.Format.Setup(); err != nil {
		return err
	}
	r.logger.Info("runner has been set up")
	return nil
}

// beforeRun runs BeforeRun for the runner storages.
func (r *Runner) beforeRun() error {
	r.logger.Info("preparing the runner for a run")
	if err := r.Iteration.Input.BeforeRun(); err != nil {
		return err
	}
	if err := r.Iteration.Tracker.BeforeRun(); err != nil {
		return err
	}
	if err := r.Iteration.Output.BeforeRun(); err != nil {
		return err
	}
	r.Listener.Prepare()
	r.logger.Info("runner has been prepared for a run")
	return nil
}

// afterRun runs AfterRun for the runner storages and resets the runner listener.
func (r *Runner) afterRun() error {
	r.logger.Info("finishing the runner after a run")
	if err := r.Iteration.Input.AfterRun(); err != nil {
		return err
	}
	if err := r.Iteration.Tracker.AfterRun(); err != nil {
		return err
	}
	if err := r.Iteration.Output.AfterRun(); err != nil {
		return err
	}
	r.Listener.Reset()
	r.logger.Info("runner has been finished after a run")
	return nil
}

// shutdown runs Shutdown for the runner storages.
func (r *Runner) shutdown() {
	r.logger.Info("shutting down the runner")
	r.Iteration.Input.Shutdown()
	r.Iteration.Output.Shutdown()
	r.Iteration.Tracker.Shutdown()
	r.logger.Info("runner has been shut down")
}

// handleProcessContainersResult checks the process result and the corresponding error. If there is
// an error or an error occurred later during tracking the result issues, it's returned.
func (r *Runner) handleProcessContainersResult(step Step, result *ProcessContainersResult, err error) error {
	if err != nil {
		issue, ok := err.(*Issue)
		if !ok {
			return err
		}
		issue.complete(r.Iteration, nil, step)
		if r.Iteration.Format.StopOnError() {
			return issue
		}
		if err := r.Iteration.Tracker.TrackIssue(issue); err != nil {
			return fmt.Errorf("failed to track issue: %v", err)
		}
		return nil
	}
	if result != nil && len(result.Failed) != 0 {
		if err := r.handleImportIssues(step, result.Failed); err != nil {
			return err
		}
	}
	return nil
}

// completeOperations follows up containers operations fields population with the runner iteration
// and the corresponding container.
func (r *Runner) completeContainerOperations(containers []*Container) {
	for _, container := range containers {
		for _, operation := range container.Operations {
			operation.Iteration = r.Iteration
			operation.Container = container
		}
	}
}

// handleImportIssues populates the issues container and iteration id fields and tracks the result
// issues using the runner tracker.
func (r *Runner) handleImportIssues(step Step, issues ContainerIssues) error {
	for container, containerIssues := range issues {
		for _, issue := range containerIssues {
			issue.complete(r.Iteration, container, step)
			if r.Iteration.Format.StopOnError() {
				return issue
			}
			if err := r.Iteration.Tracker.TrackIssue(issue); err != nil {
				return fmt.Errorf("failed to track %d container issue: %v", container.ID, err)
			}
		}
	}
	return nil
}

// initFormatStorages returns the initialised format tracker, input and output.
func initFormatStorages(format Format, ctx context.Context, processID string, logger *zap.Logger) (Tracker, Input, Output, error) {
	tracker := format.Tracker()
	if err := initStorage(tracker, ctx, processID, logger); err != nil {
		return nil, nil, nil, err
	}
	input := format.Input()
	if err := initStorage(input, ctx, processID, logger); err != nil {
		return nil, nil, nil, err
	}
	output := format.Output()
	if err := initStorage(output, ctx, processID, logger); err != nil {
		return nil, nil, nil, err
	}
	return tracker, input, output, nil
}

// buildProcessID concatenates the configured process id prefix with the current format name
// as the result process id.
func buildProcessID(prefix, formatName string) string {
	if prefix != "" {
		return prefix + "_" + formatName
	}
	return formatName
}

// newRunnersController creates a new instance of the runnersController.
func newRunnersController(runners []*Runner) *runnersController {
	c := &runnersController{runners: make([]*runnerState, 0, len(runners))}
	for _, r := range runners {
		c.runners = append(c.runners, newRunnerState(r))
	}
	return c
}

// runnersController wraps interaction with runners.
type runnersController struct {
	runners []*runnerState
}

// ExecRunner runs all the runners along with all the runners auxiliary functions.
func (c *runnersController) ExecRunners(ctx context.Context) error {
	for _, runner := range c.runners {
		if !runner.IsSetup() {
			if err := runner.setup(); err != nil {
				return err
			}
			runner.MarkAsSetup()
		}
		if err := runner.beforeRun(); err != nil {
			return err
		}
		err := runner.run(ctx)
		if err != nil {
			return err
		}
		if err := runner.afterRun(); err != nil {
			return err
		}
	}
	return nil
}

// ShutdownRunners calls shutdown func for the runners which have been setup.
func (c *runnersController) ShutdownRunners() {
	for _, runner := range c.runners {
		runner.shutdown()
	}
}

// newRunnerState wraps the runner with a state.
func newRunnerState(runner *Runner) *runnerState {
	return &runnerState{runner, false}
}

// runnerState enhances a Runner with state properties.
type runnerState struct {
	*Runner
	isSetup bool
}

// IsSetup returns whether the runner has been setup.
func (s *runnerState) IsSetup() bool {
	return s.isSetup
}

// MarkAsSetup marks the runner as setup.
func (s *runnerState) MarkAsSetup() {
	s.isSetup = true
}
