package output

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"
	"time"

	"github.com/funktionslust/gobulk"

	"github.com/divideandconquer/go-merge/merge"
	"github.com/go-test/deep"
	"github.com/olivere/elastic/v7"
)

// attemptsOnConflict represents the number of attempts on version conflict.
const attemptsOnConflict = 3

// ElasticsearchOutputConfig represents the ElasticsearchOutput configurable fields model.
type ElasticsearchOutputConfig struct {
	// ServerURL is the ES server URL with protocol and port. E.g. https://my.es.instance:9200.
	ServerURL string `validate:"required,url"`
	// Indices represents indices that will be created in case they don't exist.
	// Base index config support: set Repository.Name to foo-base to make sure it's definitions will
	// be merged with all configs that have the name foo-*.
	Indices []gobulk.Repository
	// IndicesPath represents the path to a directory that contains *.json files with createIndex
	// payload (mappings and settings).
	// The file name will be used as index name.
	// If a file with foo-base.json exists, the settings and mappings of it will be merged into all
	// other foo-*.json index configurations.
	// The json files will be parsed and set to ElasticsearchOutput.Indices on setup.
	IndicesPath string
	// IndexSuffixes (prefix -> suffix) suffix will be appended to all index names that have a matching
	// prefix, this can be useful for versioning (foo-a-1, foo-b-1, ... can be later used as foo-*-1).
	IndexSuffixes map[string]string
}

// NewElasticsearchOutput returns a new instance of the ElasticsearchOutput.
func NewElasticsearchOutput(cfg ElasticsearchOutputConfig) *ElasticsearchOutput {
	return &ElasticsearchOutput{
		Cfg: cfg,
	}
}

// ElasticsearchOutput represents an output that stores documents in Elasticsearch.
type ElasticsearchOutput struct {
	gobulk.BaseStorage
	Cfg    ElasticsearchOutputConfig
	client *elastic.Client
}

// Setup contains the storage preparations like connection etc. Is called only once at the very
// beginning of the work with the storage. As for the ElasticsearchOutput, it setups the internal
// client and indices.
func (o *ElasticsearchOutput) Setup() error {
	if err := o.setupClient(); err != nil {
		return err
	}
	if err := o.setupIndices(); err != nil {
		return err
	}
	return nil
}

// setupClient initializes a ES client for the output needs.
func (o *ElasticsearchOutput) setupClient() error {
	client, err := elastic.NewClient(elastic.SetURL(o.Cfg.ServerURL), elastic.SetSniff(false))
	if err != nil {
		return err
	}
	_, _, err = client.Ping(o.Cfg.ServerURL).Do(o.Context)
	if err != nil {
		return err
	}
	o.client = client
	return nil
}

// setupIndices initializes and validates the given indices.
func (o *ElasticsearchOutput) setupIndices() error {
	fileIndices, err := o.getRepositoriesFromIndicesPath()
	if err != nil {
		return err
	}
	o.Cfg.Indices = o.preprocessIndices(append(o.Cfg.Indices, fileIndices...))
	if len(o.Cfg.Indices) == 0 {
		return nil
	}
	indices, err := o.client.IndexGet(o.getListOfIndexNames(o.Cfg.Indices)...).IgnoreUnavailable(true).Do(o.Context)
	if err != nil {
		return err
	}
	for _, config := range o.Cfg.Indices {
		if index, ok := indices[config.Name]; ok {
			if err = o.validateIndex(index, config); err != nil {
				return err
			}
		} else {
			if err = o.createIndex(config); err != nil {
				return err
			}
		}
	}
	return nil
}

// preprocessIndices appends the index suffix to all index names and merges base index configurations
// to it.
func (o *ElasticsearchOutput) preprocessIndices(repositories []gobulk.Repository) []gobulk.Repository {
	baseIndices := make(map[string]gobulk.Repository, len(repositories))
	nonBaseIndices := make([]gobulk.Repository, 0, len(repositories))
	// find all base indices
	for _, repo := range repositories {
		nameWithoutBaseSuffix := strings.TrimSuffix(repo.Name, "-base")
		if nameWithoutBaseSuffix == repo.Name { // if it does not have the "-base" suffix
			nonBaseIndices = append(nonBaseIndices, repo)
		} else { // if it is a base index configuration
			baseIndices[nameWithoutBaseSuffix] = repo
		}
	}
	// set suffix and merge mapping and settings
	processedIndices := make([]gobulk.Repository, 0, len(nonBaseIndices))
	for _, repo := range nonBaseIndices {
		repo.Name = o.repositoryWithSuffix(repo.Name)
		for repoPrefix, baseRepo := range baseIndices { // if the index config has base configs they will be merged
			if strings.HasPrefix(repo.Name, repoPrefix) {
				repo.Schema = merge.Merge(baseRepo.Schema, repo.Schema).(map[string]interface{})
				repo.Settings = merge.Merge(baseRepo.Settings, repo.Settings).(map[string]interface{})
			}
		}

		processedIndices = append(processedIndices, repo)
	}
	return processedIndices
}

// getListOfIndexNames returns a list of index names from the given config.
func (o *ElasticsearchOutput) getListOfIndexNames(repositories []gobulk.Repository) []string {
	names := make([]string, 0, len(repositories))
	for _, index := range repositories {
		if index.Name != "" {
			names = append(names, index.Name)
		}
	}
	return names
}

// validateIndex validates the given index and passed config.
func (o *ElasticsearchOutput) validateIndex(existingIndex *elastic.IndicesGetResponse, configIndex gobulk.Repository) error {
	if diff := deep.Equal(existingIndex.Mappings, configIndex.Schema); diff != nil {
		return fmt.Errorf("the mappings of %s index do not match: "+strings.Join(diff[:], " || "), configIndex.Name)
	}
	return nil
}

// ElasticsearchIndexConfig represents the definition of configuration for a single index.
type ElasticsearchIndexConfig struct {
	Settings map[string]interface{}
	Mappings map[string]interface{}
}

// getRepositoriesFromIndicesPath reads index mappings files into config map.
func (o *ElasticsearchOutput) getRepositoriesFromIndicesPath() ([]gobulk.Repository, error) {
	if o.Cfg.IndicesPath == "" {
		return []gobulk.Repository{}, nil
	}
	files, err := ioutil.ReadDir(o.Cfg.IndicesPath)
	if err != nil {
		return nil, err
	}
	repositories := []gobulk.Repository{}
	for _, file := range files {
		filename := filepath.Join(o.Cfg.IndicesPath, file.Name())
		content, err := ioutil.ReadFile(filename)
		if err != nil {
			return nil, err
		}
		var repository ElasticsearchIndexConfig
		if err := json.Unmarshal(content, &repository); err != nil {
			return nil, err
		}
		repositories = append(repositories, gobulk.Repository{
			Name:     strings.TrimSuffix(file.Name(), ".json"),
			Settings: repository.Settings,
			Schema:   repository.Mappings,
		})
	}
	return repositories, nil
}

// Repositories provides a list of all available repositories.
func (o *ElasticsearchOutput) Repositories() []gobulk.Repository {
	return o.Cfg.Indices
}

// createIndex creates a new index with the given name and config.
func (o *ElasticsearchOutput) createIndex(configIndex gobulk.Repository) error {
	body := map[string]interface{}{"settings": configIndex.Settings, "mappings": configIndex.Schema}
	_, err := o.client.CreateIndex(configIndex.Name).BodyJson(body).Do(o.Context)
	if err != nil {
		return err
	}
	return nil
}

// Elements provides the already existing elements of the output related to a given element (in most
// cases one or none, sometimes multiple).
func (o *ElasticsearchOutput) Elements(repositories []gobulk.Repository, query interface{}, unmarshal gobulk.UnmarshalOutputElement, expectedElementCount int) ([]gobulk.Element, error) {
	esQuery := o.client.Search(o.getListOfIndexNames(repositories)...).Query(query.(elastic.Query)).Size(expectedElementCount).Pretty(true)
	result, err := esQuery.Do(o.Context)
	if err != nil {
		return nil, err
	}
	elements := make([]gobulk.Element, len(result.Hits.Hits))
	for i, hit := range result.Hits.Hits {
		element, err := unmarshal(hit)
		if err != nil {
			return nil, err
		}
		elements[i] = element
	}
	o.client.ClearCache("_all").Do(o.Context)
	return elements, nil
}

// Create creates new operation data of output elements.
func (o *ElasticsearchOutput) Create(operations ...*gobulk.Operation) (*gobulk.OutputResponse, error) {
	bulkService := o.client.Bulk()
	response := gobulk.OutputResponse{}
	added := make([]*gobulk.Operation, 0, len(operations))
	for _, operation := range operations {
		if issue := o.validateOperation(operation); issue != nil {
			response.Issues = append(response.Issues, issue)
			continue
		}
		added = append(added, operation)
		bulkService.Add(elastic.NewBulkIndexRequest().
			RetryOnConflict(attemptsOnConflict).
			Index(o.repositoryWithSuffix(operation.OutputRepository)).
			Id(operation.OutputIdentifier).
			Doc(operation.Data),
		)
	}
	o.Logger.Info("do create")
	bulkResponse, err := o.executeBulkWithRetries(bulkService, 15, 1)

	o.Logger.Info("done create")
	if err != nil {
		return nil, err
	}
	o.client.ClearCache("_all").Do(o.Context)
	return o.fillUpResponse(&response, bulkResponse.Indexed(), added)
}

// Update modifies operation data of existing output elements.
func (o *ElasticsearchOutput) Update(operations ...*gobulk.Operation) (*gobulk.OutputResponse, error) {
	bulkService := o.client.Bulk()
	response := gobulk.OutputResponse{}
	added := make([]*gobulk.Operation, 0, len(operations))
	for _, operation := range operations {
		if issue := o.validateOperation(operation); issue != nil {
			response.Issues = append(response.Issues, issue)
			continue
		}
		added = append(added, operation)
		bulkService.Add(elastic.NewBulkUpdateRequest().
			RetryOnConflict(attemptsOnConflict).
			Index(o.repositoryWithSuffix(operation.OutputRepository)).
			Id(operation.OutputIdentifier).
			Doc(operation.Data),
		)
	}
	o.Logger.Info("do update")
	bulkResponse, err := o.executeBulkWithRetries(bulkService, 15, 1)
	o.Logger.Info("done update")
	if err != nil {
		return nil, err
	}
	o.client.ClearCache("_all").Do(o.Context)
	return o.fillUpResponse(&response, bulkResponse.Updated(), added)
}

// Delete removes operation data of existing output elementы.
func (o *ElasticsearchOutput) Delete(operations ...*gobulk.Operation) (*gobulk.OutputResponse, error) {
	response := gobulk.OutputResponse{}
	bulkService := o.client.Bulk()
	for _, operation := range operations {
		bulkService.Add(elastic.NewBulkDeleteRequest().
			Index(o.repositoryWithSuffix(operation.OutputRepository)).
			Id(operation.OutputIdentifier),
		)
	}
	o.Logger.Info("do delete")
	bulkResponse, err := o.executeBulkWithRetries(bulkService, 15, 1)
	o.Logger.Info("done delete")
	if err != nil {
		return nil, err
	}
	o.client.ClearCache("_all").Do(o.Context)
	return o.fillUpResponse(&response, bulkResponse.Deleted(), operations)
}

// executeBulkWithRetries executes the bulkService operations with taking care of possible throttling
// from the ES server side as pause and retry.
func (o *ElasticsearchOutput) executeBulkWithRetries(bulkService *elastic.BulkService, retries int, try int) (*elastic.BulkResponse, error) {
	bulkResponse, err := bulkService.Do(o.Context)
	if err != nil && strings.Contains(err.Error(), "Error 429 (Too Many Requests)") {
		if try <= retries {
			o.Logger.Info("Automatic throttling due to Error 429 (Too Many Requests)")
			time.Sleep(time.Minute * 1)
			try = try + 1
			return o.executeBulkWithRetries(bulkService, retries, try)
		}
	}
	return bulkResponse, err
}

// validateOperation validates the given operation.
func (o *ElasticsearchOutput) validateOperation(operation *gobulk.Operation) *gobulk.Issue {
	_, ok := operation.Data.(string)
	if ok {
		return nil
	}
	_, ok = operation.Data.(map[string]interface{})
	if !ok {
		return gobulk.NewExecutionIssue(
			fmt.Errorf("could not cast operation.Data of document %s to map[string]interface{}", operation.OutputIdentifier),
			"", operation.Container, operation, gobulk.IssueTypeDataIntegrity, "",
		)
	}
	return nil
}

// fillUpResponse fills up the given response with executed and added operations.
func (o *ElasticsearchOutput) fillUpResponse(response *gobulk.OutputResponse, executed []*elastic.BulkResponseItem, added []*gobulk.Operation) (*gobulk.OutputResponse, error) {
	if len(executed) != len(added) {
		return nil, errors.New("length of executed and added operations do not match")
	}
	for i, item := range executed {
		operation := added[i]
		if !(item.Status >= 200 && item.Status <= 299) {
			if !(operation.Type == gobulk.OperationTypeDelete && item.Status == 404) {
				err := fmt.Errorf("Status: %+v || Result: %+v  || Error: %+v ", item.Status, item.Result, item.Error)
				message := "failed to perform %s operation over document %s in index %s: " + err.Error()
				response.Issues = append(response.Issues, gobulk.NewExecutionIssue(
					fmt.Errorf(message, operation.Type, operation.OutputIdentifier, o.repositoryWithSuffix(operation.OutputRepository)),
					"", operation.Container, operation, gobulk.IssueTypePersistance, "",
				))
			} else {
				// TODO how to handle deletes that couldn't be deleted?
				response.Succeeded = append(response.Succeeded, operation)
			}
		} else {
			response.Succeeded = append(response.Succeeded, operation)
		}
	}
	return response, nil
}

// repositoryWithSuffix appends a suffix to the repository based on the o.IndexSuffixes.
func (o *ElasticsearchOutput) repositoryWithSuffix(repository string) string {
	if o.Cfg.IndexSuffixes == nil {
		return repository
	}
	for prefix, suffix := range o.Cfg.IndexSuffixes {
		if strings.HasPrefix(repository, prefix) {
			return repository + suffix
		}
	}
	return repository
}
