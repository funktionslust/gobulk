package utils

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"regexp"
	"strings"
)

// DecodeXML parses raw data into an XML struct.
func DecodeXML(output interface{}, data []byte, externalEntities map[string]string, opts ...decodeOpt) error {
	data = replaceEntities(data, MergeEntities(externalEntities, getInternalEntities(data)))
	decoder := xml.NewDecoder(bytes.NewReader(data))
	for _, opt := range opts {
		opt(decoder)
	}
	return decoder.Decode(&output)
}

// decodeOpt is an optional modifier for decoders used in the DecodeXML func.
type decodeOpt func(d *xml.Decoder)

// WithCharsetReader sets the charsetReader to the decoder CharsetReader.
func WithCharsetReader(charsetReader func(charset string, input io.Reader) (io.Reader, error)) decodeOpt {
	return func(d *xml.Decoder) {
		d.CharsetReader = charsetReader
	}
}

// WithStrict sets the decoder Strict to the passed strict value.
func WithStrict(strict bool) decodeOpt {
	return func(d *xml.Decoder) {
		d.Strict = strict
	}
}

// MergeEntities returns a merge result of the passed entity lists. Order the passed maps
// in the increasing importance order: in case of a conflict, the conflict entity value will
// be overwritten. Say, you passed two mappings, and they both have a value for an entity
// with the same name. The value taken from the second map will be used in the result.
func MergeEntities(entityLists ...map[string]string) map[string]string {
	var length int
	for _, entityList := range entityLists {
		length += len(entityList)
	}
	entities := make(map[string]string, length)
	for _, entityList := range entityLists {
		for k, v := range entityList {
			entities[k] = v
		}
	}
	return entities
}

// replaceEntities returns the data with all the entities replaced to their actual values.
func replaceEntities(data []byte, entities map[string]string) []byte {
	oldnew := make([]string, 0, len(entities))
	for entity, value := range entities {
		oldnew = append(oldnew, fmt.Sprintf("&%s;", entity), value)
	}
	r := strings.NewReplacer(oldnew...)
	return []byte(r.Replace(string(data)))
}

// getInternalEntities fetches all internal entities from the passed xml data DOCTYPE part.
func getInternalEntities(data []byte) map[string]string {
	internalEntities := make(map[string]string)
	for _, match := range internalEntitiesRegex.FindAllSubmatch(data, -1) {
		if len(match) == 3 {
			internalEntities[string(match[1])] = string(match[2])
		}
	}
	return internalEntities
}

var internalEntitiesRegex = regexp.MustCompile(`<!ENTITY ([^\s]*)[^\"]*\"(.*)\".*>`)
