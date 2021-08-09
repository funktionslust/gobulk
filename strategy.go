package gobulk

// Strategy defines import strategies such as FIFO
type Strategy string

const (
	// StrategyFIFO first in, first out -> latest containers tracked get processed before older ones
	StrategyFIFO Strategy = "fifo"
	// StrategyLIFO last in, first out -> oldest containers tracked get processed before latest ones
	StrategyLIFO Strategy = "lifo"
)

// String converts a strategy to string
func (s Strategy) String() string {
	return string(s)
}
