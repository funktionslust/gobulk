package gobulk

import (
	"fmt"
	"path"
	"sort"
	"sync"

	"go.uber.org/zap"
)

const (
	parserParseBulkElementsMetricName = "parser_parse_bulk_elements"
	parserElementParseMetricName      = "parser_element_parse"
)

// Parser is responsible for using the Format's definitions to convert containers raw data
// into elements containing structured data.
type Parser struct {
	parse          func(container *Container, input Element) (Element, error)
	chunkSize      int
	metricsTracker MetricsTracker
	logger         *zap.Logger
}

// NewParser returns a preconfigured Parser struct.
func NewParser(
	parse func(container *Container, input Element) (Element, error),
	chunkSize int,
	metricsTracker MetricsTracker,
	logger *zap.Logger,
) *Parser {
	metricsTracker.Add(parserParseBulkElementsMetricName, "Time taken to get all elements of a single containers bulk")
	metricsTracker.Add(parserElementParseMetricName, "Time taken to parse one single container data piece")
	return &Parser{
		parse:          parse,
		chunkSize:      chunkSize,
		metricsTracker: metricsTracker,
		logger:         logger,
	}
}

// ParseBulkElements converts containers data into elements using the parsing logic defined in the
// Format and assigns the result elements to the corresponding containers. It returns successfully
// parsed containers bulk, issues mapped by failed container IDs and an error.
func (p *Parser) ParseBulkElements(containers []*Container) (*ProcessContainersResult, error) {
	p.logger.Info("parser start", zap.Int("bulk_size", len(containers)))
	p.metricsTracker.Start(parserParseBulkElementsMetricName)
	defer p.metricsTracker.Stop(parserParseBulkElementsMetricName)
	failed := NewContainerIssues()
	parsed, errors := p.parseContainers(containers)
	if len(errors) != 0 {
		for container, err := range errors {
			if issue, ok := err.(*Issue); ok {
				failed.Append(container, issue)
			} else {
				return nil, fmt.Errorf("parse container error: %v", err)
			}
		}
	}
	result := NewProcessContainersResult(parsed, failed)
	logStepResults(p.logger, "parse", result)
	return result, nil
}

// parseContainers parses the containers data and returns a successfully parsed containers
// slice sorted by container.ID and issues mapped by corresponding containers.
func (p *Parser) parseContainers(containers []*Container) ([]*Container, map[*Container]error) {
	parsedMu := &sync.Mutex{}
	parsed := make([]*Container, 0, len(containers))
	failedMu := &sync.Mutex{}
	failed := make(map[*Container]error)
	for _, chunk := range splitContainers(containers, p.chunkSize) {
		wg := &sync.WaitGroup{}
		for _, container := range chunk {
			wg.Add(1)
			go func(container *Container) {
				defer wg.Done()
				if err := p.parseContainer(container); err != nil {
					failedMu.Lock()
					defer failedMu.Unlock()
					failed[container] = err
				} else {
					parsedMu.Lock()
					defer parsedMu.Unlock()
					parsed = append(parsed, container)
				}
			}(container)
		}
		wg.Wait()
	}
	sort.SliceStable(parsed, func(i, j int) bool {
		return parsed[i].ID < parsed[j].ID
	})
	return parsed, failed
}

// parseContainer parses the container data and populates its Elements field with the result.
func (p *Parser) parseContainer(container *Container) error {
	containerLocation := path.Join(container.InputRepository, container.InputIdentifier)
	p.logger.Info("container parse start",
		zap.String("location", containerLocation),
		zap.Int("element count", len(container.Data)),
	)
	defer p.logger.Info("container parse end", zap.String("location", containerLocation))
	elements := make([]Element, 0, len(container.Data))
	for location, data := range container.Data {
		p.logger.Info("element parse start", zap.String("location", location))
		p.metricsTracker.Start(parserElementParseMetricName)
		locationElement, err := p.parse(container, NewInputElement(location, data))
		p.metricsTracker.Stop(parserElementParseMetricName)
		if err != nil {
			p.logger.Info("element parse error", zap.String("location", location), zap.NamedError("error_message", err))
			return err
		}
		elements = append(elements, locationElement)
		p.logger.Info("element parse end", zap.String("location", location))
	}
	container.Elements = elements
	container.Data = nil
	return nil
}
