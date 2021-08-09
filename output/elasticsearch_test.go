// +build integration

package output

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/olivere/elastic/v7"
	"github.com/stretchr/testify/assert"
)

const (
	indexName          = "docs-output-test"
	documentIdentifier = "docs-output-test"
)

func TestElasticsearchOutput_Setup(t *testing.T) {
	t.Run("Simple", func(t *testing.T) {
		output, err := buildOutput()
		assert.Nil(t, err)
		assert.Nil(t, output.Setup())
	})
	t.Run("IndexMappingsDoNotMatch", func(t *testing.T) {
		output, err := buildOutput()
		assert.Nil(t, err)
		output.Indices = []gobulk.Repository{
			{
				Name: indexName,
				Schema: map[string]interface{}{
					"properties": map[string]interface{}{
						"title": map[string]interface{}{
							"type": "text",
						},
					},
				}}}
		expectedErrorMsg := fmt.Sprintf("the mappings of %s index do not match", indexName)
		assert.EqualError(t, output.Setup(), expectedErrorMsg)
	})
}

func TestElasticsearchOutput_Create(t *testing.T) {
	output, err := buildOutput()
	assert.Nil(t, err)
	assert.Nil(t, output.Setup())

	data := map[string]interface{}{"title": "test"}
	op := buildCreateOperation(documentIdentifier, data)
	resp, err := output.Create(op)
	assert.Nil(t, err)
	assert.NotNil(t, resp.Succeeded)

	doc, err := findDocumentByID(output.client, documentIdentifier)
	assert.Nil(t, err)

	var source map[string]interface{}
	_ = json.Unmarshal(doc.Source, &source)
	assert.Equal(t, data, source)
}

func TestElasticsearchOutput_Update(t *testing.T) {
	output, err := buildOutput()
	assert.Nil(t, err)
	assert.Nil(t, output.Setup())

	data := map[string]interface{}{"title": "test"}
	op := buildCreateOperation(documentIdentifier, data)
	resp, err := output.Create(op)
	assert.Nil(t, err)
	assert.NotNil(t, resp.Succeeded)

	data = map[string]interface{}{"title": "test2"}
	op = buildUpdateOperation(documentIdentifier, data)
	resp, err = output.Update(op)
	assert.Nil(t, err)
	assert.NotNil(t, resp.Succeeded)

	doc, err := findDocumentByID(output.client, documentIdentifier)
	assert.Nil(t, err)

	var source map[string]interface{}
	_ = json.Unmarshal(doc.Source, &source)
	assert.Equal(t, data, source)
}

func TestElasticsearchOutput_Delete(t *testing.T) {
	output, err := buildOutput()
	assert.Nil(t, err)
	assert.Nil(t, output.Setup())

	op := buildDeleteOperation(documentIdentifier)
	resp, err := output.Delete(op)
	assert.Nil(t, err)
	assert.NotNil(t, resp.Succeeded)

	_, err = findDocumentByID(output.client, documentIdentifier)
	assert.NotNil(t, err)
	assert.True(t, elastic.IsNotFound(err))
}

func TestElasticsearchOutput_Elements(t *testing.T) {
	output, err := buildOutput()
	assert.Nil(t, err)
	assert.Nil(t, output.Setup())

	data := map[string]interface{}{"title": "test"}
	op := buildCreateOperation(documentIdentifier, data)
	resp, err := output.Create(op)
	assert.Nil(t, err)
	assert.NotNil(t, resp.Succeeded)

	_, err = output.client.Refresh(indexName).Do(output.Context)
	assert.Nil(t, err)

	repositories := []gobulk.Repository{{Name: indexName}}
	query := elastic.NewMatchQuery("_id", documentIdentifier)
	unmarshalFunc := getUnmarshalOutputElementFunc()
	elements, err := output.Elements(repositories, query, unmarshalFunc, 10)
	assert.Nil(t, err)
	assert.Len(t, elements, 1)
	assert.Equal(t, elements[0].ParsedData(), data)
}

// buildOutput builds an output instance with default settings.
func buildOutput() (*ElasticsearchOutput, error) {
	return &ElasticsearchOutput{
		ServerURL: fmt.Sprintf("http://%s:%s", os.Getenv("ES_OUTPUT_HOST"), os.Getenv("ES_OUTPUT_PORT")),
		Indices: []gobulk.Repository{
			{
				Name:     indexName,
				Settings: map[string]interface{}{"number_of_shards": 1},
				Schema: map[string]interface{}{
					"properties": map[string]interface{}{
						"title": map[string]interface{}{
							"type": "keyword",
						},
					},
				}},
		},
		BaseStorage: gobulk.BaseStorage{Context: context.Background()},
	}, nil
}

// buildCreateOperation a new instance of the create operation.
func buildCreateOperation(identifier string, data map[string]interface{}) *gobulk.Operation {
	return &gobulk.Operation{
		Iteration:        &gobulk.Iteration{},
		Container:        &gobulk.Container{},
		Type:             gobulk.OperationTypeCreate,
		OutputRepository: indexName,
		OutputIdentifier: identifier,
		Data:             data,
	}
}

// buildUpdateOperation a new instance of the update operation.
func buildUpdateOperation(identifier string, data map[string]interface{}) *gobulk.Operation {
	return &gobulk.Operation{
		Iteration:        &gobulk.Iteration{},
		Container:        &gobulk.Container{},
		Type:             gobulk.OperationTypeUpdate,
		OutputRepository: indexName,
		OutputIdentifier: identifier,
		Data:             data,
	}
}

// buildDeleteOperation a new instance of the delete operation.
func buildDeleteOperation(identifier string) *gobulk.Operation {
	return &gobulk.Operation{
		Iteration:        &gobulk.Iteration{},
		Container:        &gobulk.Container{},
		Type:             gobulk.OperationTypeDelete,
		OutputRepository: indexName,
		OutputIdentifier: identifier,
	}
}

// findDocumentByID finds a single document by its id.
func findDocumentByID(client *elastic.Client, identifier string) (*elastic.GetResult, error) {
	return client.Get().Index(indexName).Id(identifier).Do(context.Background())
}

// getUnmarshalOutputElementFunc returns function to unmarshal output element.
func getUnmarshalOutputElementFunc() gobulk.UnmarshalOutputElement {
	return func(outputData interface{}) (gobulk.Element, error) {
		var source map[string]interface{}
		hit := outputData.(*elastic.SearchHit)
		err := json.Unmarshal(hit.Source, &source)
		if err != nil {
			return nil, err
		}
		elem := gobulk.InputElement{}
		elem.SetParsedData(source)
		return &elem, nil
	}
}
