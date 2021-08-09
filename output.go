package gobulk

// Output represents a storage that will be the destination for the handled data. The Output interface
// is used to mostly save, but also retrieve the results of import process.
type Output interface {
	Storage
	// Elements retrieves one or more elements from the output. It runs the passed query to find
	// data entries in the specified repositories. The unmarshal func is then applied over each of
	// the data entries to transform them to Element entries.
	Elements(repositories []Repository, query interface{}, unmarshal UnmarshalOutputElement, expectedElementCount int) ([]Element, error)
	// Create creates new records in the output.
	Create(operations ...*Operation) (*OutputResponse, error)
	// Update modifies existing records in the output.
	Update(operations ...*Operation) (*OutputResponse, error)
	// Delete removes existing records from the output.
	Delete(operations ...*Operation) (*OutputResponse, error)
	// Repositories provides a list of all available repositories.
	Repositories() []Repository
}

// UnmarshalOutputElement is a callback that is used to transform output-taken data to an element.
type UnmarshalOutputElement func(outputData interface{}) (Element, error)

// OutputResponse is a type that represents the response of bulk operation execution.
type OutputResponse struct {
	Succeeded []*Operation
	Issues    []*Issue
}
