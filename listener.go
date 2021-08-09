package gobulk

import (
	"fmt"

	"go.uber.org/zap"
	"golang.org/x/net/context"
)

// Listener checks the input of a iteration with it's format for (new) containers (e.g. files),
// creates records in the tracker (tracks then) and delegates tracked containers download jobs
// to the loader.
type Listener struct {
	iteration  *Iteration
	tracker    Tracker
	input      Input
	marker     *Container
	loader     *Loader
	initLoader func() *Loader
	ready      bool
	readyChan  chan struct{}
	logger     *zap.Logger
	*switcher
}

// NewListener returns a validated and preconfigured listener struct.
func NewListener(
	iteration *Iteration,
	tracker Tracker,
	input Input,
	marker *Container,
	logger *zap.Logger,
	state SwitcherState,
	workersLimit int,
) (*Listener, error) {
	return &Listener{
		iteration:  iteration,
		tracker:    tracker,
		input:      input,
		marker:     marker,
		initLoader: func() *Loader { return NewLoader(input.Read, int64(workersLimit)) },
		readyChan:  make(chan struct{}),
		logger:     logger,
		switcher:   newSwitcher(state),
	}, nil
}

// Listen scans the format's input for containers and tracks and downloads them.
func (l Listener) Listen(ctx context.Context) error {
	contCh := make(chan []*Container)
	doneCh := make(chan struct{})
	errCh := make(chan error)
	scanCtx, stopScan := context.WithCancel(ctx)
	if l.IsOn() {
		l.logger.Info("starting listener")
		go l.input.Scan(scanCtx, l.marker, contCh, doneCh, errCh)
	}
	go l.loader.Run()
	defer l.loader.Stop()
	if err := l.preloadContainers(); err != nil {
		l.logger.Warn("listener failed to preload unfinished containers", zap.NamedError("error_message", err))
	}
	for {
		if l.IsOff() {
			l.logger.Info("listener is off and waiting to be launched")
			select {
			case <-l.On():
				l.logger.Info("starting listener")
				go l.input.Scan(scanCtx, l.marker, contCh, doneCh, errCh)
			case <-ctx.Done():
				l.logger.Info("listener has been stopped by context")
				return nil
			}
		} else {
			select {
			case <-l.Off():
				l.logger.Info("listener has been turned off")
				stopScan()
				scanCtx, stopScan = context.WithCancel(ctx)
			case err := <-errCh:
				if err := l.handleListenError(err, nil); err != nil {
					return err
				}
			case <-ctx.Done():
				l.logger.Info("listener has been stopped by context")
				return nil
			case <-doneCh:
				l.logger.Info("listener has been stopped: all available containers have been scanned")
				if !l.ready {
					l.markAsReady()
				}
				return nil
			default:
				select {
				case cs := <-contCh:
					l.setContainersIteration(cs)
					resp, err := l.tracker.TrackContainers(cs)
					if err != nil {
						if err := l.handleListenError(err, cs); err != nil {
							return err
						}
					}
					if len(resp.Tracked) == 0 {
						l.logger.Info("no new containers have been returned by scan")
						continue
					}
					l.marker = resp.Tracked[len(resp.Tracked)-1]
					l.loader.Put(resp.Tracked)
					l.logger.Info("a bulk of containers has been tracked after scanning",
						zap.Int("amount", len(resp.Tracked)),
						zap.Int("already_tracked_scanned_containers", len(resp.Conflicted)),
					)
					if !l.ready {
						l.markAsReady()
						l.logger.Info("listener is ready to be listened to")
					}
				default:
				}
			}
		}
	}
}

// Ready returns true if the Listener has sent the first containers bulk to the Tracker.
func (l *Listener) Ready() bool {
	return l.ready
}

// ReadyChan returns a channel that notifies all subscribers with a value (being closed) once the
// Listener is ready.
func (l *Listener) ReadyChan() <-chan struct{} {
	return l.readyChan
}

// Prepare prepares the listener for a run.
func (l *Listener) Prepare() {
	l.loader = l.initLoader()
}

// Reset resets the Listener state to the default one.
func (l *Listener) Reset() {
	l.ready = false
	l.readyChan = make(chan struct{})
}

// preloadContainers gets tracked unfinished containers from the tracker and puts them to the loader
// to speed up their download progress.
func (l *Listener) preloadContainers() error {
	containers, err := l.tracker.GetUnfinishedContainers()
	if err != nil {
		return err
	}
	l.loader.Put(containers)
	l.logger.Info("preloaded unfinished containers to the loader", zap.Int("amount", len(containers)))
	return nil
}

// handleListenError tracks the error if it's an Issue, or returns the error otherwise.
func (l *Listener) handleListenError(err error, containers []*Container) error {
	issue, ok := err.(*Issue)
	if !ok {
		return err
	}
	if len(containers) == 0 {
		issue.complete(l.iteration, nil, StepListener)
		if err := l.tracker.TrackIssue(issue); err != nil {
			return fmt.Errorf("failed to track issue %s: %v", issue.Error(), err)
		}
	} else {
		for _, container := range containers {
			issue.complete(l.iteration, container, StepListener)
			if err := l.tracker.TrackIssue(issue); err != nil {
				return fmt.Errorf("failed to track issue %s: %v", issue.Error(), err)
			}
		}
	}
	return nil
}

// setContainersIteration populates the containers Iteration field with the listener one.
func (l *Listener) setContainersIteration(containers []*Container) {
	for _, c := range containers {
		c.IterationID = l.iteration.ID
	}
}

// markAsReady sets the Listener ready field to true and notifies all the l.readyChan listeners
// about the Listener is ready.
func (l *Listener) markAsReady() {
	l.ready = true
	close(l.readyChan)
}
