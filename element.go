package gobulk

// Element represents a structured data element which is a parsing result from container raw data
// one container can have many elements. E.g. container: log file, element: log message
type Element interface {
	// RawData returns the []byte data that should be parsed
	RawData() []byte
	// ParsedData returns the  parsed data
	ParsedData() []interface{}
	// SetParsedData sets  parsed data
	SetParsedData(parsedData interface{})
	// Location returns the location of an element
	Location() string
}

// NewInputElement is used by parser to create a new base element
func NewInputElement(location string, rawData []byte) *InputElement {
	return &InputElement{
		location: location,
		rawData:  rawData,
	}
}

// InputElement must be used as basis for all elements
type InputElement struct {
	location   string
	rawData    []byte
	parsedData []interface{}
}

// RawData returns the []byte data that should be parsed
func (b *InputElement) RawData() []byte {
	return b.rawData
}

// ParsedData returns the  parsed data
func (b *InputElement) ParsedData() []interface{} {
	return b.parsedData
}

// SetParsedData sets  parsed data
func (b *InputElement) SetParsedData(parsedData interface{}) {
	b.parsedData = append(b.parsedData, parsedData)
}

// Location returns the location of an element
func (b *InputElement) Location() string {
	return b.location
}
