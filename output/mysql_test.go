package output

import (
	"fmt"
	"os"
	"testing"

	"github.com/funktionslust/gobulk"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const (
	testUserName = "Alice"
	testUserAge  = 25
)

func TestMySQLOutput_Create(t *testing.T) {
	output, err := buildOutput()
	if err != nil {
		t.Fatalf("output build error: %v", err)
	}
	user := &User{testUserName, testUserAge}
	t.Run("Simple", func(t *testing.T) {
		resp, err := output.Create(buildCreateOperation(user))
		assert.Nilf(t, err, "create error")
		assert.NotNilf(t, resp.Succeeded, "create response mismatch")
		if t.Failed() {
			return
		}
		row, err := getUserByName(output.Client, testUserName)
		assert.Nilf(t, err, "get test user error")
		assert.Equalf(t, user, row, "result users mismatch")
	})
	if t.Failed() {
		return
	}
	t.Run("DuplicateEntry", func(t *testing.T) {
		resp, err := output.Create(buildCreateOperation(user))
		assert.Nilf(t, err, "create error")
		if assert.Equalf(t, 1, len(resp.Issues), "create response issues mismatch") {
			assert.Containsf(t, resp.Issues[0].Err.Error(), "Duplicate entry 'Alice' for key 'PRIMARY'", "duplicate entry error mismatch")
		}
	})
}

func TestMySQLOutput_Update(t *testing.T) {
	output, err := buildOutput()
	if err != nil {
		t.Fatalf("output build error: %v", err)
	}
	if err := putUser(output, &User{testUserName, testUserAge}); err != nil {
		t.Fatal(err)
	}
	t.Run("Simple", func(t *testing.T) {
		grownUpUser := &User{testUserName, testUserAge + 1}
		resp, err := output.Update(buildUpdateOperation(grownUpUser))
		assert.Nilf(t, err, "update error")
		assert.NotNilf(t, resp.Succeeded, "update response mismatch")
		if t.Failed() {
			return
		}
		row, err := getUserByName(output.Client, testUserName)
		assert.Nilf(t, err, "get test user error")
		assert.Equalf(t, grownUpUser, row, "result users mismatch")
	})
	if t.Failed() {
		return
	}
	t.Run("NonexistentName", func(t *testing.T) {
		invalidUser := &User{testUserName + " the Second", testUserAge + 1}
		resp, err := output.Update(buildUpdateOperation(invalidUser))
		assert.Nilf(t, err, "update error")
		if assert.Equalf(t, 1, len(resp.Issues), "update response issues mismatch") {
			assert.Containsf(t, resp.Issues[0].Err.Error(), "not a single row has been affected by the query", "update of invalid user error mismatch")
		}
	})
}

func TestMySQLOutput_Delete(t *testing.T) {
	output, err := buildOutput()
	if err != nil {
		t.Fatalf("output build error: %v", err)
	}
	user := &User{testUserName, testUserAge}
	if err := putUser(output, user); err != nil {
		t.Fatal(err)
	}
	t.Run("Simple", func(t *testing.T) {
		resp, err := output.Delete(buildDeleteOperation(user))
		assert.Nilf(t, err, "delete error")
		assert.NotNilf(t, resp.Succeeded, "delete response mismatch")
		if t.Failed() {
			return
		}
		row, err := getUserByName(output.Client, testUserName)
		assert.Nilf(t, row, "deleted user is not nil")
		assert.Equalf(t, err.Error(), "record not found", "get deleted user error mismatch")
	})
	if t.Failed() {
		return
	}
	t.Run("NonexistentName", func(t *testing.T) {
		resp, err := output.Delete(buildDeleteOperation(user))
		assert.Nilf(t, err, "delete error")
		if assert.Equalf(t, 1, len(resp.Issues), "delete response issues mismatch") {
			assert.Containsf(t, resp.Issues[0].Err.Error(), "not a single row has been affected by the query", "delete of invalid user error mismatch")
		}
	})
}

func TestMySQLOutput_Elements(t *testing.T) {
	output, err := buildOutput()
	if err != nil {
		t.Fatalf("output build error: %v", err)
	}
	user1Name := "Bob"
	user1Age := 30
	user1 := &User{Name: user1Name, Age: user1Age}
	user2Name := "John"
	user2Age := 35
	user2 := &User{Name: user2Name, Age: user2Age}
	user3 := &User{Name: "Nick", Age: 30}
	user4 := &User{Name: "Josh", Age: 35}
	if err := putUser(output, user1); err != nil {
		t.Fatalf("failed to put a user: %v", err)
	}
	if err := putUser(output, user2); err != nil {
		t.Fatalf("failed to put a user: %v", err)
	}
	if err := putUser(output, user3); err != nil {
		t.Fatalf("failed to put a user: %v", err)
	}
	if err := putUser(output, user4); err != nil {
		t.Fatalf("failed to put a user: %v", err)
	}
	t.Run("ByNames", func(t *testing.T) {
		elements, err := output.Elements(
			[]gobulk.Repository{{Name: "users"}},
			&GORMOutputQuery{Condition: "name in ?", Vars: []interface{}{[]string{user1Name, user2Name}}},
			unmarshalUser, 2,
		)
		assert.Nilf(t, err, "elements error")
		if assert.Equalf(t, 2, len(elements), "elements count mismatch") {
			assert.Equalf(t, user1, elements[0], "user1 mismatch")
			assert.Equalf(t, user2, elements[1], "user2 mismatch")
		}
	})
	t.Run("ByAge", func(t *testing.T) {
		elements, err := output.Elements(
			[]gobulk.Repository{{Name: "users"}},
			&GORMOutputQuery{Condition: "age = ?", Vars: []interface{}{user1Age}},
			unmarshalUser, 2,
		)
		assert.Nilf(t, err, "elements error")
		if assert.Equalf(t, 2, len(elements), "elements count mismatch") {
			assert.Equalf(t, user1, elements[0], "user1 mismatch")
			assert.Equalf(t, user3, elements[1], "user3 mismatch")
		}
	})
	t.Run("ByNameAndAge", func(t *testing.T) {
		elements, err := output.Elements(
			[]gobulk.Repository{{Name: "users"}},
			&GORMOutputQuery{Condition: "name = ? and age = ?", Vars: []interface{}{user2Name, user2Age}},
			unmarshalUser, 1,
		)
		assert.Nilf(t, err, "elements error")
		if assert.Equalf(t, 1, len(elements), "elements count mismatch") {
			assert.Equalf(t, user2, elements[0], "user2 mismatch")
		}
	})
	t.Run("NoMatchingResults", func(t *testing.T) {
		elements, err := output.Elements(
			[]gobulk.Repository{{Name: "users"}},
			&GORMOutputQuery{Condition: "name = ? and age = ?", Vars: []interface{}{user1Name, user2Age}},
			unmarshalUser, 0,
		)
		assert.Nilf(t, err, "elements error")
		assert.Equalf(t, 0, len(elements), "elements count mismatch")
	})
}

// buildOutput builds an output instance with default settings.
func buildOutput() (*MySQLOutput, error) {
	output := &MySQLOutput{
		GORMOutput: NewGORMOutput(GORMOutputConfig{
			Host:     os.Getenv("MYSQL_OUTPUT_HOST"),
			Database: os.Getenv("MYSQL_OUTPUT_DATABASE"),
			User:     os.Getenv("MYSQL_OUTPUT_USER"),
			Password: os.Getenv("MYSQL_OUTPUT_PASSWORD"),
			Port:     os.Getenv("MYSQL_OUTPUT_POR"),
			Logger:   logger.Default.LogMode(logger.Silent),
		}, map[string]interface{}{"users": User{}}),
	}
	if err := output.Setup(); err != nil {
		return nil, fmt.Errorf("setup failed: %v", err)
	}
	if output.Client.Migrator().HasTable("users") {
		if err := output.Client.Migrator().DropTable("users"); err != nil {
			return nil, fmt.Errorf("drop users table failed: %v", err)
		}
	}
	if err := output.Client.AutoMigrate(User{}); err != nil {
		return nil, fmt.Errorf("migration failed: %v", err)
	}
	return output, nil
}

// putUser saves the user to the output.
func putUser(output *MySQLOutput, user *User) error {
	resp, err := output.Create(buildCreateOperation(user))
	if err != nil {
		return fmt.Errorf("user create error: %v", err)
	}
	if len(resp.Succeeded) != 1 {
		return fmt.Errorf("user create error: expected a successful response")
	}
	return nil
}

// buildCreateOperation returns a new create operation for the passed data.
func buildCreateOperation(data interface{}) *gobulk.Operation {
	return &gobulk.Operation{
		Type: gobulk.OperationTypeCreate,
		Data: data,
	}
}

// buildUpdateOperation returns a new update operation for the passed data.
func buildUpdateOperation(data interface{}) *gobulk.Operation {
	return &gobulk.Operation{
		Type: gobulk.OperationTypeUpdate,
		Data: data,
	}
}

// buildDeleteOperation returns a new delete operation for the passed data.
func buildDeleteOperation(data interface{}) *gobulk.Operation {
	return &gobulk.Operation{
		Type: gobulk.OperationTypeDelete,
		Data: data,
	}
}

// getUserByName retrieves a user row from the db by the specified name.
func getUserByName(client *gorm.DB, name string) (*User, error) {
	user := &User{}
	if err := client.Model(User{}).First(user, "name = ?", name).Error; err != nil {
		return nil, err
	}
	return user, nil
}

// unmarshalUser decodes the outputData to the result gobulk.Element.
func unmarshalUser(outputData interface{}) (gobulk.Element, error) {
	m, ok := outputData.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unxpected type of outputData %T: expected map[string]interface{}", outputData)
	}
	user := &User{
		Name: string(m["name"].([]byte)),
		Age:  int(m["age"].(int64)),
	}
	return user, nil
}

// User is a simple test structure.
type User struct {
	Name string `gorm:"primaryKey"`
	Age  int
}

func (u *User) Location() string {
	return u.Name
}

func (User) RawData() []byte { return nil }

func (User) ParsedData() []interface{} { return nil }

func (User) SetParsedData(parsedData interface{}) {}
