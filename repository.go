package gobulk

// Repository represents a repository like a database table or an Elasticsearch index
type Repository struct {
	Name     string
	Schema   map[string]interface{}
	Settings map[string]interface{}
}
