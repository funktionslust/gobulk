package gobulk

import (
	"sync"
)

// newSwitcher returns a new instance of the switcher.
func newSwitcher(state SwitcherState) *switcher {
	s := &switcher{
		mu:      &sync.Mutex{},
		onChan:  make(chan struct{}),
		offChan: make(chan struct{}),
	}
	s.setState(state)
	return s
}

// switcher is a struct to be embedded to other structures that are capable of being
// turned on and off.
type switcher struct {
	mu      *sync.Mutex
	onChan  chan struct{}
	offChan chan struct{}
}

// Switch switches the switcher state to on or off in dependence of the passed value.
func (s *switcher) Switch(state SwitcherState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	currentState := s.getState()
	if currentState != state {
		s.setState(state)
	}
}

// On returns a channel a successful read from which will mean that the switcher is on.
// It's always either On or Off channels returning a value.
// Usage example: case <-s.On(): (don't rely on the second receive value).
func (s *switcher) On() <-chan struct{} {
	return s.onChan
}

// Off returns a channel a successful read from which will mean that the switcher is off.
// It's always either On or Off channels returning a value.
// Usage example: case <-s.Off(): (don't rely on the second receive value).
func (s *switcher) Off() <-chan struct{} {
	return s.offChan
}

// IsOn returns whether the switcher is switched on.
func (s *switcher) IsOn() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.getState()
	return state == SwitcherStateOn
}

// IsOff returns whether the switcher is switched off.
func (s *switcher) IsOff() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.getState()
	return state == SwitcherStateOff
}

// setState closes the corresponding state channel and recreates another one.
func (s *switcher) setState(state SwitcherState) {
	switch state {
	case SwitcherStateOn:
		s.offChan = make(chan struct{})
		close(s.onChan)
	case SwitcherStateOff:
		s.onChan = make(chan struct{})
		close(s.offChan)
	}
}

// getState returns the current switcher state.
func (s *switcher) getState() SwitcherState {
	select {
	case <-s.offChan:
		return SwitcherStateOff
	case <-s.onChan:
		return SwitcherStateOn
	}
}

// SwitcherState represents a state of a switcher. Could be either on or off.
type SwitcherState int

const (
	// SwitcherStateOff used as constant to switch it off
	SwitcherStateOff SwitcherState = 0
	// SwitcherStateOn used as constant to switch it on
	SwitcherStateOn SwitcherState = 1
)
