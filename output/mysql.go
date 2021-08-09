package output

import (
	"encoding/json"
	"fmt"

	"github.com/funktionslust/gobulk"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// MySQLOutput represents an output that stores documents to a MySQL database.
type MySQLOutput struct {
	GORMOutput
}

// Setup contains the storage preparations like connection etc. Is called only once
// at the very beginning of the work with the storage. As for the MySQLOutput, it tests
// the connection / read access of the tracker, prepares everything like migrations.
func (o *MySQLOutput) Setup() error {
	db, err := gorm.Open(mysql.Open(fmt.Sprintf("%s:%s@tcp(%s:%s)/?%s", o.Cfg.User, o.Cfg.Password, o.Cfg.Host, o.Cfg.Port, "charset=utf8mb4&parseTime=true")), &gorm.Config{DisableForeignKeyConstraintWhenMigrating: true})
	if err != nil {
		return fmt.Errorf("failed to establish a general connection: %s", err)
	}
	err = db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`  DEFAULT CHARACTER SET = `utf8mb4` DEFAULT COLLATE = `utf8mb4_unicode_ci`;", o.Cfg.Database)).Error
	if err != nil {
		return fmt.Errorf("failed to create database: %v", err)
	}
	db, err = gorm.Open(mysql.Open(fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?%s", o.Cfg.User, o.Cfg.Password, o.Cfg.Host, o.Cfg.Port, o.Cfg.Database, "charset=utf8mb4&parseTime=true")), &gorm.Config{DisableForeignKeyConstraintWhenMigrating: true})
	if err != nil {
		return fmt.Errorf("failed to establish a database connection: %v", err)
	}
	db = db.Set("gorm:table_options", "CHARSET=utf8mb4 ENGINE=InnoDB COLLATE=utf8mb4_unicode_ci")
	o.Client = db.Session(&gorm.Session{Logger: o.Cfg.Logger})
	for table, model := range o.RelatedModels {
		if err := o.Client.Table(table).AutoMigrate(model); err != nil {
			return fmt.Errorf("table %s auto-migration error: %v", table, err)
		}
	}
	if err := o.buildTablesInfo(); err != nil {
		return fmt.Errorf("failed to gather tables information: %v", err)
	}
	return nil
}

// buildTablesInfo gathers the database tables schemas and saves them as the t.tables.
func (o *MySQLOutput) buildTablesInfo() error {
	var tables []string
	if err := o.Client.Raw("SHOW tables").Scan(&tables).Error; err != nil {
		return fmt.Errorf("failed to get tables list: %v", err)
	}
	for _, table := range tables {
		if _, ex := o.RelatedModels[table]; !ex {
			continue
		}
		fields := []FieldDescription{}
		resp := o.Client.Raw("DESCRIBE " + table).Scan(&fields)
		if err := resp.Error; err != nil {
			return fmt.Errorf("failed to get table %s description: %v", table, err)
		}
		schema := make(map[string]interface{})
		for _, field := range fields {
			m, err := field.ToMap()
			if err != nil {
				return fmt.Errorf("failed to represent field description as a map: %v", err)
			}
			schema[field.Field] = m
		}
		o.Tables = append(o.Tables, gobulk.Repository{
			Name:   table,
			Schema: schema,
		})
	}
	return nil
}

// FieldDescription represents a MySQL table field description.
type FieldDescription struct {
	Field   string `json:"-"`
	Type    string
	Null    string
	Key     string
	Default string
	Extra   string
}

// ToMap returns the d representation as a map[string]interface.
func (d *FieldDescription) ToMap() (map[string]interface{}, error) {
	var result map[string]interface{}
	encoded, err := json.Marshal(d)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(encoded, &result); err != nil {
		return nil, err
	}
	return result, nil
}
