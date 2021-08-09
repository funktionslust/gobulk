package gobulk

import (
	"sync"
)

// newTasksList creates a new instance of *taskList.
func newTasksList() *tasksList {
	return &tasksList{
		mu:          &sync.Mutex{},
		tasksByKeys: make(map[string]struct{}, 100),
		queue:       make([]*Container, 0, 100),
	}
}

// tasksList represents a loader's tasks tracker. It contains logic regarding
// adding, deletion and iterating over containers to be loaded by workers.
type tasksList struct {
	mu          *sync.Mutex
	tasksByKeys map[string]struct{}
	queue       []*Container
}

// Add adds the passed containers to the tasks list. If a container has already
// been added to the queue and is still there, it's skipped.
func (l *tasksList) Add(containers []*Container) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, container := range containers {
		key := buildContainerKey(container)
		if _, ex := l.tasksByKeys[key]; !ex {
			l.tasksByKeys[key] = struct{}{}
			l.queue = append(l.queue, container)
		}
	}
}

// Delete deletes a container with the passed key from the tasks list if there
// is such a container in the tasks list.
func (l *tasksList) Delete(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ex := l.tasksByKeys[key]; ex {
		for idx, container := range l.queue {
			if buildContainerKey(container) == key {
				l.queue = append(l.queue[:idx], l.queue[idx+1:]...)
				delete(l.tasksByKeys, key)
				return
			}
		}
	}
}

// Next provides the next in the queue container. If the queue is empty, the result
// container is nil.
func (l *tasksList) Next() *Container {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.queue) == 0 {
		return nil
	}
	next := l.queue[0]
	l.queue = l.queue[1:]
	delete(l.tasksByKeys, buildContainerKey(next))
	return next
}

// Len returns the number of containers in the queue.
func (l *tasksList) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.tasksByKeys)
}
