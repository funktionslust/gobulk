package output

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/funktionslust/gobulk"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const (
	// gormOutputElementsBulkSize is a size of a single GORMOutput.Elements query.
	gormOutputElementsBulkSize = 1000
)

// GORMOutputConfig represents the GORMOutput config structure.
type GORMOutputConfig struct {
	Host     string           `validate:"required"`
	Database string           `validate:"required"`
	User     string           `validate:"required"`
	Password string           `validate:"required"`
	Port     string           `validate:"required"`
	Logger   logger.Interface `validate:"required"`
}

// NewGORMOutput returns a new instance of the *GORMOutput. The relatedModels parameter represents
// a list of models (tables) that the output is going to use. The models map is supposed to be a
// map from a table name to its model as go structure.
// Example: map[string]interface{}{"users": User{}}
func NewGORMOutput(cfg GORMOutputConfig, relatedModels map[string]interface{}) GORMOutput {
	return GORMOutput{
		Cfg:           cfg,
		RelatedModels: relatedModels,
	}
}

// GORMOutput provides interface for general output operations using gorm library. The
// GORMOutput, while performing operations execition, doesn't use operations OutputIdentifier
// field. Instead, it relies on the passed data structures primary keys as the identifiers
// for rows to be created, updated or deleted. However, it's a good practice to have the
// OutputIdentifier field set for at least two reasons:
// 1) the GORMOutput then is easier to be replaced with other kind of gobulk.Output, because
// e.g. ElasticsearchOutput does use the OutputIdentifier field.
// 2) performed operations, no matter succeeded or issued, are tracked in the corresponding
// tracker repository, where you can see details of them and get to know what's gone wrong.
type GORMOutput struct {
	gobulk.BaseStorage
	Cfg           GORMOutputConfig
	Client        *gorm.DB
	RelatedModels map[string]interface{}
	Tables        []gobulk.Repository
}

// Elements performs the query to the given list of repositories, transforms the result rows
// to elements using the unmarshal func, and returns the result list of elements.
//
// The query parameter is expected to be of *GORMOutputQuery type, carrying the SQL condition
// and corresponding variables.
// Query application explanation: for all the given repositories:
// SELECT * FROM ${repository.Name} WHERE ${query.Condition}, ${query.Vars}.
// For the following result: SELECT * FROM users WHERE users.age > 40 AND users.name = "Kate",
// Use repositories = []gobulk.Repository{{Name: "users"}},
// query = &output.GORMOutputQuery{Condition: "users.age > ? AND users.name = ?", Vars: []interface{}{40, "Kate"}}.
//
// In the unmarshal func, you should consider the outputData to be of type map[string]interface{}
// with keys as the result field names and values as the actual field values. See the rowToMap
// GORMOutput method for more details.
func (o *GORMOutput) Elements(repositories []gobulk.Repository, query interface{}, unmarshal gobulk.UnmarshalOutputElement, expectedElementCount int) ([]gobulk.Element, error) {
	elements := make([]gobulk.Element, 0, expectedElementCount)
	q, ok := query.(*GORMOutputQuery)
	if !ok {
		return nil, fmt.Errorf("unexpected type of query %T: expected *GORMOutputQuery", query)
	}
	for _, repo := range repositories {
		if _, ex := o.RelatedModels[repo.Name]; !ex {
			return nil, fmt.Errorf("invalid repository %s in the request: related repositories are %s", repo.Name, o.relatedTablesNames())
		}
		var offset int
		for ; ; offset += gormOutputElementsBulkSize {
			db := o.Client.Table(repo.Name).Offset(offset).Limit(gormOutputElementsBulkSize).Where(q.Condition, q.Vars...)
			if err := db.Error; err != nil {
				return nil, fmt.Errorf("failed to make query: %v", err)
			}
			rows, err := db.Rows()
			if err != nil {
				return nil, fmt.Errorf("failed to get query result rows: %v", err)
			}
			rowsCount := 0
			for rows.Next() {
				rowsCount++
				m, err := o.rowToMap(rows)
				if err != nil {
					return nil, err
				}
				el, err := unmarshal(m)
				if err != nil {
					return nil, fmt.Errorf("hit unmarshal error: %v", err)
				}
				elements = append(elements, el)
			}
			if rowsCount != gormOutputElementsBulkSize {
				break
			}
		}
	}
	return elements, nil
}

// Create performs CREATE for all passed operations, creating operation.Data in the operation.OutputRepository
// table.
func (o *GORMOutput) Create(operations ...*gobulk.Operation) (*gobulk.OutputResponse, error) {
	response := &gobulk.OutputResponse{}
	for _, op := range operations {
		if err := o.Client.Table(op.OutputRepository).Create(op.Data).Error; err != nil {
			response.Issues = append(response.Issues, o.newOperationIssue(op, err))
		} else {
			response.Succeeded = append(response.Succeeded, op)
		}
	}
	return response, nil
}

// Update performs UPDATE for all passed operations, creating operation.Data in the operation.OutputRepository
// table using the model primaryKey as the reference to the row to be updated.
func (o *GORMOutput) Update(operations ...*gobulk.Operation) (*gobulk.OutputResponse, error) {
	response := &gobulk.OutputResponse{}
	for _, op := range operations {
		if err := o.update(op); err != nil {
			response.Issues = append(response.Issues, o.newOperationIssue(op, err))
		} else {
			response.Succeeded = append(response.Succeeded, op)
		}
	}
	return response, nil
}

// Delete performs DELETE for all passed operations, using the operations Data model primaryKey as the reference
// to the row to be updated and the operation.OutputRepository as the table name.
func (o *GORMOutput) Delete(operations ...*gobulk.Operation) (*gobulk.OutputResponse, error) {
	response := &gobulk.OutputResponse{}
	for _, op := range operations {
		if err := o.delete(op); err != nil {
			response.Issues = append(response.Issues, o.newOperationIssue(op, err))
		} else {
			response.Succeeded = append(response.Succeeded, op)
		}
	}
	return response, nil
}

// Repositories provides a list of all available repositories (tables).
func (o *GORMOutput) Repositories() []gobulk.Repository {
	return o.Tables
}

// Shutdown is called only once at the very end of the work with the storage. As for the GORMOutput,
// it closes the initially opened db connection.
func (o *GORMOutput) Shutdown() {
	db, _ := o.Client.DB()
	if db != nil {
		db.Close()
	}
}

// relatedTablesNames returns a slice of related table names.
func (o *GORMOutput) relatedTablesNames() []string {
	tables := make([]string, 0, len(o.RelatedModels))
	for modelTable := range o.RelatedModels {
		tables = append(tables, modelTable)
	}
	return tables
}

// rowToMap reads the next row and writes the row data to the result map[string]interface{}
// with keys as the fields names and values as the actual field values.
func (o *GORMOutput) rowToMap(rows *sql.Rows) (map[string]interface{}, error) {
	columnNames, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to read row column names: %v", err)
	}
	columns := make([]interface{}, len(columnNames))
	columnPointers := make([]interface{}, len(columnNames))
	for i := range columns {
		columnPointers[i] = &columns[i]
	}
	if err := rows.Scan(columnPointers...); err != nil {
		return nil, fmt.Errorf("scan error: %v", err)
	}
	m := make(map[string]interface{})
	for i, colName := range columnNames {
		val := columnPointers[i].(*interface{})
		m[colName] = *val
	}
	return m, nil
}

// update runs update for the given operation and handles the result error.
func (o *GORMOutput) update(op *gobulk.Operation) error {
	db := o.Client.Table(op.OutputRepository).Save(op.Data)
	err := db.Error
	if err == nil && db.RowsAffected == 0 {
		err = fmt.Errorf("not a single row has been affected by the query")
	}
	return err
}

// delete runs delete for the given operation and handles the result error.
func (o *GORMOutput) delete(op *gobulk.Operation) error {
	db := o.Client.Table(op.OutputRepository).Delete(op.Data)
	err := db.Error
	if err == nil && db.RowsAffected == 0 {
		err = fmt.Errorf("not a single row has been affected by the query")
	}
	return err
}

// newOperationIssue creates a new output related issue based in the passed error and operation.
func (GORMOutput) newOperationIssue(op *gobulk.Operation, err error) *gobulk.Issue {
	var issueType gobulk.IssueType
	switch {
	case strings.Contains(err.Error(), "dial tcp: "):
		issueType = gobulk.IssueTypeInfrastructure
	default:
		issueType = gobulk.IssueTypeDataIntegrity
	}
	return gobulk.NewExecutionIssue(err, "", op.Container, op, issueType, "")
}

// GORMOutputQuery represents a structure containing parameters for GORMOutput queries.
// Example of the GORMOutputQuery usage:
// o.Client.Table(repository).Where(q.Condition, q.Vars...).
type GORMOutputQuery struct {
	Condition string
	Vars      []interface{}
}
