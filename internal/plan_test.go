// SPDX-FileCopyrightText: 2026 Stefan Majewsky <majewsky@gmx.net>
// SPDX-License-Identifier: Apache-2.0

package internal_test

import (
	"reflect"
	"testing"
	"time"

	"go.xyrillian.de/oblast/info"
	"go.xyrillian.de/oblast/internal"
	"go.xyrillian.de/oblast/internal/assert"
)

func TestPlanFieldTraversal(t *testing.T) {
	type Log struct {
		info.TableNameIs  `db:"log_entries"`
		info.PrimaryKeyIs `db:"id"`
		ID                int64     `db:"id,auto"`
		CreatedAt         time.Time `db:"created_at"`
		Message           string
		private1          bool `db:"private1"` //nolint:unused
		Ignored           any  `db:"-"`
	}

	// assert on interface implementations
	var (
		_ info.IsTable               = Log{}
		_ info.IsTableWithPrimaryKey = Log{}
	)

	// check that the plan for Log:
	// 1. has no IndexByColumnName entries for marker types
	// 2. uses the field name as a column name for "Message"
	// 3. ignores "private1" because it cannot be written through reflection
	// 4. ignores "Ignored" because its column name is "-"
	// 5. recognizes "id" as an autofilled column
	plan, err := internal.BuildPlan(reflect.TypeFor[Log](), internal.PostgresDialect{})
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, plan.TableName, "log_entries")
	assert.DeepEqual(t, plan.AllColumnNames, []string{"id", "created_at", "Message"})
	assert.DeepEqual(t, plan.PrimaryKeyColumnNames, []string{"id"})
	assert.DeepEqual(t, plan.AutoColumnNames, []string{"id"})
	assert.DeepEqual(t, plan.IndexByColumnName, map[string][]int{
		"id":         {2},
		"created_at": {3},
		"Message":    {4},
	})

	assert.Equal(t, plan.Insert.Query,
		`INSERT INTO "log_entries" ("created_at", "Message") VALUES ($1, $2) RETURNING "id"`,
	)
	assert.DeepEqual(t, plan.Insert.ArgumentIndexes, [][]int{{3}, {4}})
	assert.Equal(t, plan.Update.Query,
		`UPDATE "log_entries" SET "created_at" = $1, "Message" = $2 WHERE "id" = $3`,
	)
	assert.DeepEqual(t, plan.Update.ArgumentIndexes, [][]int{{3}, {4}, {2}})
	assert.Equal(t, plan.Delete.Query,
		`DELETE FROM "log_entries" WHERE "id" = $1`,
	)
	assert.DeepEqual(t, plan.Delete.ArgumentIndexes, [][]int{{2}})

	type record struct {
		Log
		Foo      bool `db:"foo"`
		private2 bool `db:"private2"` //nolint:unused
	}

	// check that the plan for record:
	// 1. works at all, even though it as a whole is an unexported type
	// 2. traverses into Log and includes all of its fields as well
	// 3. completely ignores the marker types in type Log
	plan, err = internal.BuildPlan(reflect.TypeFor[record](), internal.PostgresDialect{})
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, plan.TableName, "")
	assert.DeepEqual(t, plan.AllColumnNames, []string{"id", "created_at", "Message", "foo"})
	assert.DeepEqual(t, plan.PrimaryKeyColumnNames, nil)
	assert.DeepEqual(t, plan.AutoColumnNames, []string{"id"}) // this is okay, it does not bear significance in practice since no queries are generated
	assert.DeepEqual(t, plan.IndexByColumnName, map[string][]int{
		"id":         {0, 2},
		"created_at": {0, 3},
		"Message":    {0, 4},
		"foo":        {1},
	})

	assert.Equal(t, plan.Insert.Query, "")
	assert.Equal(t, plan.Update.Query, "")
	assert.Equal(t, plan.Delete.Query, "")
}
