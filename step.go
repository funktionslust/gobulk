package gobulk

// Step defines a step within the gobulk process.
type Step string

const (
	// StepListener describes the process of the Listener.
	StepListener Step = "listener"
	// StepReader describes the process of the Reader.
	StepReader Step = "reader"
	// StepParser describes the process of the Parser.
	StepParser Step = "parser"
	// StepPlanner describes the process of the Planner.
	StepPlanner Step = "planner"
	// StepExecutor describes the process of the Executor.
	StepExecutor Step = "executor"
	// StepOther describes a step different from all mentioned above.
	StepOther = "other"
)

// String converts a step to string.
func (s Step) String() string {
	return string(s)
}
