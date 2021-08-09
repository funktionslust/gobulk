package gobulk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	nextContainerErrorID uint64 = 666
	nextContainerError          = fmt.Sprintf("containers with id %d are force-failed by the mockTracker NextContainer", nextContainerErrorID)

	planErrorID uint64 = 1000
	planError          = fmt.Sprintf("containers with id %d are force-failed by the formatMock Plan", planErrorID)

	executeErrorID uint64 = 1234
	executeError          = fmt.Sprintf("containers with id %d are force-failed by the formatMock Execute", executeErrorID)

	subOperationRootID      uint64 = 1555
	subOperationCreateID    uint64 = 1666
	subOperationFailedID    uint64 = executeErrorID
	subOperationCreateTitle        = "subOperationCreateTitle"
)

func TestSimpleRun(t *testing.T) {
	// ARRANGE
	c1data := map[string][]byte{"c1data": []byte(`{"title":"title_1","content":"hello world"}`)}
	_, runner, output, input, tracker := build(t, []*Container{
		{
			Data: c1data,
			ID:   1,
		},
	}, SwitcherStateOn, SwitcherStateOn, nil, nil)

	// ACT
	if err := Run(context.Background(), 0, runner); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	// ASSERT
	if assert.Equalf(t, 1, len(output.elements), "output elements number mismatch") {
		assert.Equalf(t, c1data["c1data"], output.elements["1_doc"], "output element data mismatch")
	}
	assert.Truef(t, output.isShutDown, "output expected to be shut down after run")
	assert.Truef(t, input.isShutDown, "input expected to be shut down after run")
	if assert.Equalf(t, 1, len(tracker.containers), "tracker containers number mismatch") {
		cont := tracker.containers[0]
		assert.Equalf(t, uint64(1), cont.ID, "tracker container id mismatch")
		assert.NotNil(t, cont.Started)
		assert.NotNil(t, cont.Finished)
	}
	assert.Equalf(t, 0, len(tracker.issues), "tracker issues number mismatch")
	assert.Truef(t, tracker.isShutDown, "tracker expected to be shut down after run")
}

func TestSimpleRunWithMarker(t *testing.T) {
	// ARRANGE
	c1data := map[string][]byte{"c1data": []byte(`{"title":"title_1","content":"hello world"}`)}
	c2data := map[string][]byte{"c2data": []byte(`{"title":"title_2","content":"greetings my lord"}`)}
	format, runner, output, input, tracker := build(t, []*Container{
		{
			Data: c1data,
			ID:   1,
		},
		{
			Data: c2data,
			ID:   2,
		},
	}, SwitcherStateOn, SwitcherStateOn, nil, nil)
	started := time.Now().Add(-time.Second)
	finished := time.Now()
	input.containers[0].Started = &started
	input.containers[0].Finished = &finished
	tracker.iteration = &Iteration{
		Tracker:              tracker,
		Input:                input,
		Output:               output,
		Format:               format,
		LastTrackedContainer: input.containers[0],
	}

	// ACT
	if err := Run(context.Background(), 0, runner); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	// ASSERT
	if assert.Equalf(t, 1, len(output.elements), "output elements number mismatch") {
		assert.Equalf(t, c2data["c2data"], output.elements["2_doc"], "output element data mismatch")
	}
	assert.Truef(t, output.isShutDown, "output expected to be shut down after run")
	assert.Truef(t, input.isShutDown, "input expected to be shut down after run")
	if assert.Equalf(t, 2, len(tracker.containers), "tracker containers number mismatch") {
		cont := tracker.containers[0]
		assert.Equalf(t, uint64(1), cont.ID, "tracker container id mismatch")
		assert.NotNil(t, cont.Started)
		assert.NotNil(t, cont.Finished)
		cont = tracker.containers[1]
		assert.Equalf(t, uint64(2), cont.ID, "tracker container id mismatch")
		assert.NotNil(t, cont.Started)
		assert.NotNil(t, cont.Finished)
	}
	assert.Equalf(t, input.containers[1].ID, tracker.iteration.LastTrackedContainer.ID, "last tracked container ID mismatch")
	assert.Equalf(t, input.containers[1].ID, tracker.iteration.LastProcessedContainer.ID, "last tracked container ID mismatch")
	assert.Equalf(t, 0, len(tracker.issues), "tracker issues number mismatch")
	assert.Truef(t, tracker.isShutDown, "tracker expected to be shut down after run")
}

func TestIssueTracking(t *testing.T) {
	t.Run("ReaderNextContainerIssue", func(t *testing.T) {
		cdata := map[uint64]map[string][]byte{
			1:                    {"1": []byte(`{"title":"title_1","content":"hello world"}`)},
			nextContainerErrorID: {"nextContainerErrorID": []byte(`{"title":"title_2","content":"greetings my lord"}`)},
		}
		inContainers := []*Container{{Data: cdata[1], ID: 1}, {Data: cdata[nextContainerErrorID], ID: nextContainerErrorID}}
		_, runner, output, input, tracker := build(t, inContainers, SwitcherStateOn, SwitcherStateOn, nil, nil)

		// ACT
		if err := Run(context.Background(), 0, runner); err != nil {
			t.Fatalf("run failed: %v", err)
		}

		// ASSERT
		if assert.Equalf(t, 1, len(output.elements), "output elements number mismatch") {
			assert.Equalf(t, cdata[1]["1"], output.elements["1_doc"], "output element data mismatch")
		}
		assert.Truef(t, output.isShutDown, "output expected to be shut down after run")
		assert.Truef(t, input.isShutDown, "input expected to be shut down after run")
		if assert.Equalf(t, 2, len(tracker.containers), "tracker containers number mismatch") {
			okCont := tracker.containers[0]
			assert.Equalf(t, uint64(1), okCont.ID, "tracker container id mismatch")
			assert.NotNil(t, okCont.Started)
			assert.NotNil(t, okCont.Finished)
			failCont := tracker.containers[1]
			assert.Equalf(t, nextContainerErrorID, failCont.ID, "tracker container id mismatch")
			assert.NotNil(t, failCont.Started)
			assert.Nil(t, failCont.Finished)
		}
		if assert.Equalf(t, 1, len(tracker.issues), "tracker issues number mismatch") {
			issue := tracker.issues[0]
			assert.Equalf(t, StepReader, issue.Step, "issue step mismatch")
			assert.Nilf(t, issue.Container, "read issue container is expected to be nil")
			assert.Equalf(t, runner.Iteration.ID, issue.Iteration.ID, "issue iteration mismatch")
			assert.Equalf(t, errors.New(nextContainerError), issue.Err, "issue error mismatch")
			assert.Equalf(t, IssueTypeDataIntegrity, issue.Type, "issue typr mismatch")
		}
		assert.Truef(t, tracker.isShutDown, "tracker expected to be shut down after run")
	})

	t.Run("ParserElementsIssue", func(t *testing.T) {
		// ARRANGE
		cdata := map[uint64]map[string][]byte{
			1: {"1": []byte(`{"title":"title_1","content":"invalid json`)},
		}
		inContainers := []*Container{{Data: cdata[1], ID: 1}}
		_, runner, output, input, tracker := build(t, inContainers, SwitcherStateOn, SwitcherStateOn, nil, nil)

		// ACT
		if err := Run(context.Background(), 0, runner); err != nil {
			t.Fatalf("run failed: %v", err)
		}

		// ASSERT
		assert.Equalf(t, 0, len(output.elements), "output elements number mismatch")
		assert.Truef(t, output.isShutDown, "output expected to be shut down after run")
		assert.Truef(t, input.isShutDown, "input expected to be shut down after run")
		if assert.Equalf(t, 1, len(tracker.containers), "tracker containers number mismatch") {
			failCont := tracker.containers[0]
			assert.Equalf(t, uint64(1), failCont.ID, "tracker container id mismatch")
			assert.NotNil(t, failCont.Started)
			assert.Nil(t, failCont.Finished)
		}
		if assert.Equalf(t, 1, len(tracker.issues), "tracker issues number mismatch") {
			issue := tracker.issues[0]
			assert.Equalf(t, StepParser, issue.Step, "issue step mismatch")
			assert.Equalf(t, inContainers[0].ID, issue.Container.ID, "issue container mismatch")
			assert.Equalf(t, runner.Iteration.ID, issue.Iteration.ID, "issue iteration mismatch")
			assert.Equalf(t, "unexpected end of JSON input", issue.Err.Error(), "issue error message mismatch")
			assert.Equalf(t, IssueTypeDataIntegrity, issue.Type, "issue typr mismatch")
		}
		assert.Truef(t, tracker.isShutDown, "tracker expected to be shut down after run")
	})

	t.Run("PlannerOperationsIssue", func(t *testing.T) {
		// ARRANGE
		cdata := map[uint64]map[string][]byte{
			planErrorID: {"planErrorID": []byte(`{"title":"title_1","content":"hello world"}`)},
		}
		inContainers := []*Container{{Data: cdata[planErrorID], ID: planErrorID}}
		_, runner, output, input, tracker := build(t, inContainers, SwitcherStateOn, SwitcherStateOn, nil, nil)

		// ACT
		if err := Run(context.Background(), 0, runner); err != nil {
			t.Fatalf("run failed: %v", err)
		}

		// ASSERT
		assert.Equalf(t, 0, len(output.elements), "output elements number mismatch")
		assert.Truef(t, output.isShutDown, "output expected to be shut down after run")
		assert.Truef(t, input.isShutDown, "input expected to be shut down after run")
		if assert.Equalf(t, 1, len(tracker.containers), "tracker containers number mismatch") {
			failCont := tracker.containers[0]
			assert.Equalf(t, planErrorID, failCont.ID, "tracker container id mismatch")
			assert.NotNil(t, failCont.Started)
			assert.Nil(t, failCont.Finished)
		}
		if assert.Equalf(t, 1, len(tracker.issues), "tracker issues number mismatch") {
			issue := tracker.issues[0]
			assert.Equalf(t, StepPlanner, issue.Step, "issue step mismatch")
			assert.Equalf(t, inContainers[0].ID, issue.Container.ID, "issue container mismatch")
			assert.Equalf(t, runner.Iteration.ID, issue.Iteration.ID, "issue iteration mismatch")
			assert.Equalf(t, planError, issue.Err.Error(), "issue error message mismatch")
			assert.Equalf(t, IssueTypeInfrastructure, issue.Type, "issue typr mismatch")
			assert.Equalf(t, "planErrorIssue", issue.Payload, "issue payload mismatch")
		}
		assert.Truef(t, tracker.isShutDown, "tracker expected to be shut down after run")
	})

	t.Run("ExecutorImportIssue", func(t *testing.T) {
		// ARRANGE
		cdata := map[uint64]map[string][]byte{
			executeErrorID: {"executeErrorID": []byte(`{"title":"title_1","content":"hello world"}`)},
		}
		inContainers := []*Container{{Data: cdata[executeErrorID], ID: executeErrorID}}
		_, runner, output, input, tracker := build(t, inContainers, SwitcherStateOn, SwitcherStateOn, nil, nil)

		// ACT
		if err := Run(context.Background(), 0, runner); err != nil {
			t.Fatalf("run failed: %v", err)
		}

		// ASSERT
		assert.Equalf(t, 0, len(output.elements), "output elements number mismatch")
		assert.Truef(t, output.isShutDown, "output expected to be shut down after run")
		assert.Truef(t, input.isShutDown, "input expected to be shut down after run")
		if assert.Equalf(t, 1, len(tracker.containers), "tracker containers number mismatch") {
			failCont := tracker.containers[0]
			assert.Equalf(t, executeErrorID, failCont.ID, "tracker container id mismatch")
			assert.NotNil(t, failCont.Started)
			assert.Nil(t, failCont.Finished)
		}
		if assert.Equalf(t, 1, len(tracker.issues), "tracker issues number mismatch") {
			issue := tracker.issues[0]
			assert.Equalf(t, StepExecutor, issue.Step, "issue step mismatch")
			assert.Equalf(t, inContainers[0].ID, issue.Container.ID, "issue container mismatch")
			assert.Equalf(t, runner.Iteration.ID, issue.Iteration.ID, "issue iteration mismatch")
			assert.Equalf(t, executeError, issue.Err.Error(), "issue error message mismatch")
			assert.Equalf(t, IssueTypeInfrastructure, issue.Type, "issue typr mismatch")
			assert.Equalf(t, "executeErrorIssue", issue.Payload, "issue payload mismatch")
		}
		assert.Truef(t, tracker.isShutDown, "tracker expected to be shut down after run")
	})
}

func TestStateSwitch(t *testing.T) {
	t.Run("ProcessingStopped", func(t *testing.T) {
		// ARRANGE
		cdata := map[uint64]map[string][]byte{
			1: {"1": []byte(`{"title":"title_1","content":"hello world"}`)},
		}
		_, runner, output, input, tracker := build(t, []*Container{
			{
				Data: cdata[1],
				ID:   1,
			},
		}, SwitcherStateOn, SwitcherStateOff, nil, nil)
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(time.Second)
			cancel()
		}()

		// ACT
		if err := Run(ctx, 0, runner); err != nil {
			t.Fatalf("run failed: %v", err)
		}

		// ASSERT
		assert.Equalf(t, 0, len(output.elements), "output elements number mismatch")
		assert.Truef(t, output.isShutDown, "output expected to be shut down after run")
		assert.Truef(t, input.isShutDown, "input expected to be shut down after run")
		if assert.Equalf(t, 1, len(input.containers), "input containers number mismatch") {
			cont := input.containers[0]
			assert.Equalf(t, uint64(1), cont.ID, "input container id mismatch")
			assert.Nil(t, cont.Started)
			assert.Nil(t, cont.Finished)
		}
		if assert.Equalf(t, 1, len(tracker.containers), "tracker containers number mismatch") {
			cont := tracker.containers[0]
			assert.Equalf(t, uint64(1), cont.ID, "tracker container id mismatch")
			assert.Nil(t, cont.Started)
			assert.Nil(t, cont.Finished)
		}
		assert.Equalf(t, 0, len(tracker.issues), "tracker issues number mismatch")
		assert.Truef(t, tracker.isShutDown, "tracker expected to be shut down after run")
	})

	t.Run("ListeningStopped", func(t *testing.T) {
		// ARRANGE
		cdata := map[uint64]map[string][]byte{
			1: {"1": []byte(`{"title":"title_1","content":"hello world"}`)},
			2: {"2": []byte(`{"title":"title_2","content":"greetings my lord"}`)},
		}
		_, runner, output, input, tracker := build(t, []*Container{
			{
				Data: cdata[2],
				ID:   2,
			},
		}, SwitcherStateOff, SwitcherStateOn, nil, nil)
		tracker.containers = append(tracker.containers, &Container{
			Data: cdata[1],
			ID:   1,
		})

		// ACT
		if err := Run(context.Background(), 0, runner); err != nil {
			t.Fatalf("run failed: %v", err)
		}

		// ASSERT
		if assert.Equalf(t, 1, len(output.elements), "output elements number mismatch") {
			assert.Equalf(t, cdata[1]["1"], output.elements["1_doc"], "output element data mismatch")
		}
		assert.Truef(t, output.isShutDown, "output expected to be shut down after run")
		assert.Truef(t, input.isShutDown, "input expected to be shut down after run")
		if assert.Equalf(t, 1, len(tracker.containers), "tracker containers number mismatch") {
			cont := tracker.containers[0]
			assert.Equalf(t, uint64(1), cont.ID, "tracker container id mismatch")
			assert.NotNil(t, cont.Started)
			assert.NotNil(t, cont.Finished)
		}
		assert.Equalf(t, 0, len(tracker.issues), "tracker issues number mismatch")
		assert.Truef(t, tracker.isShutDown, "tracker expected to be shut down after run")
	})

	t.Run("BothStopped", func(t *testing.T) {
		// ARRANGE
		cdata := map[uint64]map[string][]byte{
			1: {"1": []byte(`{"title":"title_1","content":"hello world"}`)},
			2: {"2": []byte(`{"title":"title_2","content":"greetings my lord"}`)},
		}
		_, runner, output, input, tracker := build(t, []*Container{
			{
				Data: cdata[2],
				ID:   2,
			},
		}, SwitcherStateOff, SwitcherStateOff, nil, nil)
		tracker.containers = append(tracker.containers, &Container{
			Data: cdata[1],
			ID:   1,
		})
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(time.Second)
			cancel()
		}()

		// ACT
		if err := Run(ctx, 0, runner); err != nil {
			t.Fatalf("run failed: %v", err)
		}

		// ASSERT
		assert.Equalf(t, 0, len(output.elements), "output elements number mismatch")
		assert.Truef(t, output.isShutDown, "output expected to be shut down after run")
		assert.Truef(t, input.isShutDown, "input expected to be shut down after run")
		if assert.Equalf(t, 1, len(tracker.containers), "tracker containers number mismatch") {
			cont := tracker.containers[0]
			assert.Equalf(t, uint64(1), cont.ID, "tracker container id mismatch")
			assert.Nil(t, cont.Started)
			assert.Nil(t, cont.Finished)
		}
		assert.Equalf(t, 0, len(tracker.issues), "tracker issues number mismatch")
		assert.Truef(t, tracker.isShutDown, "tracker expected to be shut down after run")
	})

	t.Run("StopListenerDuringProcess", func(t *testing.T) {
		// ARRANGE
		cdata := map[uint64]map[string][]byte{
			1: {"1": []byte(`{"title":"title_1","content":"hello world"}`)},
		}
		processListenChan := make(chan struct{})
		processWaitChan := make(chan struct{})
		_, runner, output, input, tracker := build(t, []*Container{
			{
				Data: cdata[1],
				ID:   1,
			},
		}, SwitcherStateOn, SwitcherStateOn, processListenChan, processWaitChan)
		go func() {
			<-processListenChan
			runner.Listener.Switch(SwitcherStateOff)
			for {
				if runner.Listener.IsOff() {
					break
				} else {
					runtime.Gosched()
				}
			}
			time.Sleep(500 * time.Millisecond)
			cdata[2] = map[string][]byte{"2": []byte(`{"title":"title_2","content":"greetings my lord"}`)}
			input.containers = append(input.containers, &Container{
				Data: cdata[2],
				ID:   2,
			})
			processWaitChan <- struct{}{}
		}()

		// ACT
		if err := Run(context.Background(), 0, runner); err != nil {
			t.Fatalf("run failed: %v", err)
		}

		// ASSERT
		if assert.Equalf(t, 2, len(input.containers), "input containers number mismatch") {
			cont := input.containers[0]
			assert.Equalf(t, uint64(1), cont.ID, "input container id mismatch")
			cont = input.containers[1]
			assert.Equalf(t, uint64(2), cont.ID, "input container id mismatch")
		}
		if assert.Equalf(t, 1, len(output.elements), "output elements number mismatch") {
			assert.Equalf(t, cdata[1]["1"], output.elements["1_doc"], "output element data mismatch")
		}
		assert.Truef(t, output.isShutDown, "output expected to be shut down after run")
		assert.Truef(t, input.isShutDown, "input expected to be shut down after run")
		if assert.Equalf(t, 1, len(tracker.containers), "tracker containers number mismatch") {
			cont := tracker.containers[0]
			assert.Equalf(t, uint64(1), cont.ID, "tracker container id mismatch")
			assert.NotNil(t, cont.Started)
			assert.NotNil(t, cont.Finished)
		}
		assert.Equalf(t, 0, len(tracker.issues), "tracker issues number mismatch")
		assert.Truef(t, tracker.isShutDown, "tracker expected to be shut down after run")
	})

	t.Run("StopRunnerDuringProcess", func(t *testing.T) {
		// ARRANGE
		cdata := map[uint64]map[string][]byte{
			1: {"1": []byte(`{"title":"title_1","content":"hello world"}`)},
		}
		processListenChan := make(chan struct{})
		processWaitChan := make(chan struct{})
		_, runner, output, input, tracker := build(t, []*Container{
			{
				Data: cdata[1],
				ID:   1,
			},
			{
				Data: map[string][]byte{"2": []byte(`{"title":"title_2","content":"greetings my lord"}`)},
				ID:   2,
			},
		}, SwitcherStateOn, SwitcherStateOn, processListenChan, processWaitChan)
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			<-processListenChan
			runner.Switch(SwitcherStateOff)
			for {
				if runner.IsOff() {
					break
				} else {
					runtime.Gosched()
				}
			}
			time.Sleep(500 * time.Millisecond)
			processWaitChan <- struct{}{}
		}()
		go func() {
			time.Sleep(time.Second)
			cancel()
		}()

		// ACT
		if err := Run(ctx, 0, runner); err != nil {
			t.Fatalf("run failed: %v", err)
		}

		// ASSERT
		if assert.Equalf(t, 2, len(input.containers), "input containers number mismatch") {
			cont := input.containers[0]
			assert.Equalf(t, uint64(1), cont.ID, "input container id mismatch")
			cont = input.containers[1]
			assert.Equalf(t, uint64(2), cont.ID, "input container id mismatch")
		}
		if assert.Equalf(t, 1, len(output.elements), "output elements number mismatch") {
			assert.Equalf(t, cdata[1]["1"], output.elements["1_doc"], "output element data mismatch")
		}
		assert.Truef(t, output.isShutDown, "output expected to be shut down after run")
		assert.Truef(t, input.isShutDown, "input expected to be shut down after run")
		if assert.Equalf(t, 2, len(tracker.containers), "tracker containers number mismatch") {
			cont := tracker.containers[0]
			assert.Equalf(t, uint64(1), cont.ID, "tracker container id mismatch")
			assert.NotNil(t, cont.Started)
			assert.NotNil(t, cont.Finished)
			cont = input.containers[1]
			assert.Equalf(t, uint64(2), cont.ID, "input container id mismatch")
			assert.Nil(t, cont.Started)
			assert.Nil(t, cont.Finished)
		}
		assert.Equalf(t, 0, len(tracker.issues), "tracker issues number mismatch")
		assert.Truef(t, tracker.isShutDown, "tracker expected to be shut down after run")
	})
}

func TestSubOperationExecution(t *testing.T) {
	// ARRANGE
	cdata := map[uint64]map[string][]byte{
		subOperationRootID: {"subOperationRootID": []byte(`{"title":"title_1","content":"hello world"}`)},
	}
	inContainers := []*Container{{Data: cdata[subOperationRootID], ID: subOperationRootID}}
	_, runner, output, input, tracker := build(t, inContainers, SwitcherStateOn, SwitcherStateOn, nil, nil)

	// ACT
	if err := Run(context.Background(), 0, runner); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	// ASSERT
	assert.Equalf(t, 2, len(output.elements), "output elements number mismatch")
	assert.Truef(t, output.isShutDown, "output expected to be shut down after run")
	assert.Truef(t, input.isShutDown, "input expected to be shut down after run")
	if assert.Equalf(t, 1, len(tracker.containers), "tracker containers number mismatch") {
		cont := input.containers[0]
		assert.Equalf(t, subOperationRootID, cont.ID, "input container id mismatch")
	}
	if assert.Equalf(t, 1, len(tracker.issues), "tracker issues number mismatch") {
		issue := tracker.issues[0]
		assert.Equalf(t, StepExecutor, issue.Step, "issue step mismatch")
		assert.Equalf(t, subOperationFailedID, issue.Container.ID, "issue container id mismatch")
	}
	if assert.Equalf(t, 2, len(output.elements), "output elements number mismatch") {
		_, ex := output.elements[fmt.Sprintf("%d_doc", subOperationRootID)]
		assert.Truef(t, ex, "element %s is expected to present in the output", subOperationRootID)
		data, ex := output.elements[fmt.Sprintf("%d_doc", subOperationCreateID)]
		if assert.Truef(t, ex, "element %s is expected to present in the output", subOperationCreateID) {
			var element elementMock
			if err := json.Unmarshal(data, &element); err != nil {
				t.Errorf("failed to unmarshal data to element: %v", err)
			} else {
				assert.Equalf(t, subOperationCreateTitle, element.Title, "subOperationCreateID element title mismatch")
			}
		}
	}
	assert.Truef(t, tracker.isShutDown, "tracker expected to be shut down after run")
}

func build(t *testing.T, inputContainers []*Container, listen, process SwitcherState, listenChan chan<- struct{}, waitChan <-chan struct{}) (Format, *Runner, *outputMock, *inputMock, *trackerMock) {
	tracker := &trackerMock{}
	input := &inputMock{containers: inputContainers}
	output := &outputMock{elements: make(map[string][]byte)}
	fMock := &formatMock{BaseFormat: NewBaseFormat("FormatMock", tracker, input, output, FormatWithExecutorBulkSize(100), FormatWithIssuesTracking())}
	if listenChan != nil {
		fMock.processListener = listenChan
	}
	if waitChan != nil {
		fMock.processWaiter = waitChan
	}
	cfg := RunnerConfig{
		Format:          fMock,
		ProcessIDPrefix: "test-process-1",
		ListenerState:   listen,
		ProcessState:    process,
		ParseChunkSize:  1,
		PlanChunkSize:   1,
	}
	runner, err := NewRunner(context.Background(), cfg)
	if err != nil {
		t.Fatalf("failed to create a runner: %v", err)
	}
	assert.Truef(t, tracker.isSetUp, "tracker expected to be set up before run")
	assert.Truef(t, input.isSetUp, "input expected to be set up before run")
	assert.Truef(t, output.isSetUp, "output expected to be set up before run")
	if t.Failed() {
		t.FailNow()
	}
	return fMock, runner, output, input, tracker
}

// ===============================================================
// ===============================================================
// ======================= TEST STRUCTURES =======================
// ===============================================================
// ===============================================================

// ======= Element =======

// elementMock is the mock implementation for Element interface.
type elementMock struct {
	*InputElement
	Title   string `json:"title"`
	Content string `json:"content"`
}

// Data returns parsed data of the element.
func (e *elementMock) Data() interface{} {
	return e
}

// ======= Format =======

type formatMock struct {
	BaseFormat
	// processListeners is a channel that gets notified when the format
	// runner starts parsing a container.
	processListener chan<- struct{}
	// processWaiter is a channel that waits for a notification before it
	// continues processing after notifying all the listeners.
	processWaiter <-chan struct{}
}

// Setup will be called once when creating a new runner, it can be used to initialize values e.g. using the output.
func (f *formatMock) Setup() error {
	return nil
}

// Parse uses baseElement.RawData to create one or multiple elements, base element is always part of returend elements
func (f *formatMock) Parse(container *Container, inputElement Element) (Element, error) {
	time.Sleep(50 * time.Millisecond)
	if f.processListener != nil {
		f.processListener <- struct{}{}
	}
	if f.processWaiter != nil {
		<-f.processWaiter
	}
	e := &elementMock{}
	if err := json.Unmarshal(inputElement.RawData(), e); err != nil {
		return nil, NewIssue(err, "", IssueTypeDataIntegrity, string(inputElement.RawData()))
	}
	return e, nil
}

// Plan creates, possibly multiple, executable operations based on an element
func (f *formatMock) Plan(container *Container, inputElements []Element) ([]*Operation, error) {
	time.Sleep(50 * time.Millisecond)
	var o *Operation
	for _, inputElement := range inputElements {
		e, ok := inputElement.(*elementMock)
		if !ok {
			return nil, fmt.Errorf("unexpected input element of type %T, expected elementMock", inputElement)
		}
		repos := []Repository{{Name: e.Title}}
		elements, err := f.Iteration.Output.Elements(repos, nil, func(outputData interface{}) (Element, error) {
			el, ok := outputData.(*elementMock)
			if !ok {
				return nil, fmt.Errorf("failed to cast %T item to *elementMock", outputData)
			}
			return el, nil
		}, 10)
		if err != nil {
			return nil, err
		}
		var op OperationType
		if len(elements) != 0 {
			op = OperationTypeUpdate
		} else {
			op = OperationTypeCreate
		}
		o = &Operation{
			ID:               container.ID,
			Iteration:        f.Iteration,
			Container:        container,
			Type:             op,
			OutputRepository: "repo",
			OutputIdentifier: fmt.Sprintf("%d_doc", container.ID),
			Created:          time.Now(),
			Data:             e,
		}
		if container.ID == planErrorID {
			return nil, NewIssue(errors.New(planError), "", IssueTypeInfrastructure, "planErrorIssue")
		}
		if container.ID == subOperationRootID {
			o.SubOperations = []*Operation{
				{
					ID:               subOperationCreateID,
					Iteration:        f.Iteration,
					Container:        &Container{ID: subOperationCreateID},
					Type:             OperationTypeCreate,
					OutputRepository: "repo",
					OutputIdentifier: fmt.Sprintf("%d_doc", subOperationCreateID),
					Created:          time.Now(),
					Data:             &elementMock{Title: subOperationCreateTitle},
				},
				{
					ID:               subOperationFailedID,
					Iteration:        f.Iteration,
					Container:        &Container{ID: subOperationFailedID},
					Type:             OperationTypeCreate,
					OutputRepository: "repo",
					OutputIdentifier: fmt.Sprintf("%d_doc", subOperationFailedID),
					Created:          time.Now(),
					Data:             &elementMock{},
				},
			}
		}
	}
	return []*Operation{o}, nil
}

// ======= Tracker =======

type trackerMock struct {
	BaseStorage
	iteration  *Iteration
	isSetUp    bool
	isShutDown bool
	containers []*Container
	issues     []*Issue
}

// Setup contains the storage preparations like connection etc. Is called only once
// at the very beginning of the work with the storage.
func (t *trackerMock) Setup() error {
	t.isSetUp = true
	return nil
}

// Shutdown is called only once at the very end of the work with the storage.
func (t *trackerMock) Shutdown() {
	t.isShutDown = true
}

// CurrentIteration retrieves the current iteration of a format, usually persisted in the tracker of the format
func (t *trackerMock) CurrentIteration(format Format) (*Iteration, error) {
	return t.iteration, nil
}

// GetUnfinishedContainers returns a list of containers which have already been tracked but haven't
// yet been finished.
func (t *trackerMock) GetUnfinishedContainers() ([]*Container, error) {
	return nil, nil
}

// NewIteration creates a new iteration based on the format definitions and saves it in the tracker
func (t *trackerMock) NewIteration(format Format, number uint) (*Iteration, error) {
	t.iteration = &Iteration{Format: format, Number: number, Tracker: format.Tracker(), Input: format.Input(), Output: format.Output()}
	return t.iteration, nil
}

// TrackContainers persists a list of containers (also persists iteration.LastTrackedContainer)
func (t *trackerMock) TrackContainers(containers []*Container) (*TrackContainersResponse, error) {
	t.containers = append(t.containers, containers...)
	t.iteration.LastTrackedContainer = containers[len(containers)-1]
	return &TrackContainersResponse{Tracked: containers}, nil
}

// NextContainer searches for a new processable container and locks it when it finds one
func (t *trackerMock) NextContainers(readStrategy Strategy, number int, opts ...TrackerNextContainersOpt) ([]*Container, error) {
	for _, container := range t.containers {
		if container.Started == nil && container.Finished == nil {
			st := time.Now()
			container.Started = &st
			if container.ID == nextContainerErrorID {
				return nil, NewIssue(errors.New(nextContainerError), "", IssueTypeDataIntegrity, "")
			}
			return []*Container{container}, nil
		}
	}
	return nil, nil
}

// TrackContainerOperations persists the containers operations and their error/success status after they've been executed.
func (t *trackerMock) TrackContainerOperations(container []*Container) (*ProcessContainersResult, error) {
	return NewProcessContainersResult(container, nil), nil
}

// FinishContainers sets the containers state as successfully and completely imported, it persists the containers operations and their error/success status after they've been executed
func (t *trackerMock) FinishContainers(containers []*Container) (*ProcessContainersResult, error) {
	fin := time.Now()
	for _, container := range containers {
		container.Finished = &fin
	}
	t.iteration.LastProcessedContainer = containers[len(containers)-1]
	return NewProcessContainersResult(containers, nil), nil
}

// TrackIssue persists an issue
func (t *trackerMock) TrackIssue(issue *Issue) error {
	t.issues = append(t.issues, issue)
	return nil
}

// ======= Input =======

type inputMock struct {
	BaseStorage
	isSetUp    bool
	isShutDown bool
	containers []*Container
}

// Setup contains the storage preparations like connection etc. Is called only once
// at the very beginning of the work with the storage.
func (i *inputMock) Setup() error {
	i.isSetUp = true
	return nil
}

// Shutdown is called only once at the very end of the work with the storage.
func (i *inputMock) Shutdown() {
	i.isShutDown = true
}

// Scan scans the storage for new containers and sends them to the channel
func (i *inputMock) Scan(ctx context.Context, marker *Container, contCh chan<- []*Container, doneCh chan<- struct{}, errCh chan<- error) {
	for idx := 0; ; {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if idx < len(i.containers) {
			select {
			case contCh <- []*Container{i.containers[idx]}:
				idx++
			default:
				select {
				case <-ctx.Done():
					return
				case contCh <- []*Container{i.containers[idx]}:
					idx++
				}
			}
		} else {
			runtime.Gosched()
		}
	}
}

// Reads the raw data of a container
func (i *inputMock) Read(container *Container) (map[string][]byte, error) {
	return container.Data, nil
}

// ======= Output =======

type outputMock struct {
	BaseStorage
	isSetUp    bool
	isShutDown bool
	elements   map[string][]byte
	repos      []Repository
}

// Setup contains the storage preparations like connection etc. Is called only once
// at the very beginning of the work with the storage.
func (o *outputMock) Setup() error {
	o.repos = []Repository{}
	o.isSetUp = true
	return nil
}

// Shutdown is called only once at the very end of the work with the storage.
func (o *outputMock) Shutdown() {
	o.isShutDown = true
}

// Repositories provides a list of all available repositories
func (o *outputMock) Repositories() []Repository {
	return o.repos
}

// Elements provides the already existing elements of the output related to a given element (in most cases one or none, sometimes multiple)
func (o *outputMock) Elements(locations []Repository, query interface{}, unmarshal UnmarshalOutputElement, expectedElementCount int) ([]Element, error) {
	results := make([]Element, 0)
	for _, repo := range locations {
		if hit, ex := o.elements[repo.Name]; ex {
			element, err := unmarshal(hit)
			if err != nil {
				return nil, err
			}
			if (len(results) + 1) <= expectedElementCount {
				results = append(results, element)
			} else {
				break
			}
		}
	}
	return results, nil
}

// Create adds new operation records
func (o *outputMock) Create(operations ...*Operation) (*OutputResponse, error) {
	resp := &OutputResponse{}
	for _, operation := range operations {
		if operation.Container.ID == executeErrorID {
			resp.Issues = append(resp.Issues, NewExecutionIssue(
				errors.New(executeError),
				"",
				operation.Container,
				operation,
				IssueTypeInfrastructure,
				"executeErrorIssue",
			))
			continue
		}
		element, ok := operation.Data.(*elementMock)
		if !ok {
			return nil, fmt.Errorf("'data' field cast to *elementMock failed")
		}
		b, err := json.Marshal(element)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal element: %v", err)
		}
		o.elements[operation.OutputIdentifier] = b
		resp.Succeeded = append(resp.Succeeded, operation)
	}
	return resp, nil
}

// Update modifies operation data of an existing output element
func (o *outputMock) Update(operations ...*Operation) (*OutputResponse, error) {
	resp := &OutputResponse{}
	for _, operation := range operations {
		if operation.Container.ID == executeErrorID {
			resp.Issues = append(resp.Issues, NewExecutionIssue(
				errors.New(executeError),
				"",
				operation.Container,
				operation,
				IssueTypeInfrastructure,
				"executeErrorIssue",
			))
			continue
		}
		element, ok := operation.Data.(*elementMock)
		if !ok {
			return nil, fmt.Errorf("'data' field cast to *elementMock failed")
		}
		b, err := json.Marshal(element)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal element: %v", err)
		}
		o.elements[operation.OutputIdentifier] = b
		resp.Succeeded = append(resp.Succeeded, operation)
	}
	return resp, nil
}

// Delete removes operation data of an existing output element
func (o *outputMock) Delete(operations ...*Operation) (*OutputResponse, error) {
	resp := &OutputResponse{}
	for _, operation := range operations {
		if operation.Container.ID == executeErrorID {
			resp.Issues = append(resp.Issues, NewExecutionIssue(
				errors.New(executeError),
				"",
				operation.Container,
				operation,
				IssueTypeInfrastructure,
				"executeErrorIssue",
			))
			continue
		}
		resp.Succeeded = append(resp.Succeeded, operation)
	}
	return resp, nil
}
