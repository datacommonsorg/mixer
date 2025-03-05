package sqldb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestId(t *testing.T) {
	sqlClient, err := NewSQLiteClient("../../test/sqlquery/key_value/datacommons.db")
	if err != nil {
		t.Fatalf("Could not open test database: %v", err)
	}

	assert.Equal(t, "sqlite-../../test/sqlquery/key_value/datacommons.db", sqlClient.id)
}
