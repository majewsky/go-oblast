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
		Message           string    `db:"message"`
		private1          bool      `db:"private1"` //nolint:unused
	}

	// assert on interface implementations
	var (
		_ info.IsTable               = Log{}
		_ info.IsTableWithPrimaryKey = Log{}
	)

	// check that the plan for Log:
	// 1. has no IndexByColumnName entries for marker types
	// 2. ignores "private1" because it cannot be written through reflection
	// 3. recognizes "id" as an autofilled column
	plan, err := internal.BuildPlan(reflect.TypeFor[Log](), internal.PostgresDialect{})
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, plan.TableName, "log_entries")
	assert.DeepEqual(t, plan.PrimaryKeyColumns, []string{"id"})
	assert.DeepEqual(t, plan.AutoColumns, []string{"id"})
	assert.DeepEqual(t, plan.IndexByColumnName, map[string][]int{
		"id":         {2},
		"created_at": {3},
		"message":    {4},
	})

	type record struct {
		Log
		Keks     bool `db:"keks"`
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
	assert.DeepEqual(t, plan.PrimaryKeyColumns, nil)
	assert.DeepEqual(t, plan.AutoColumns, []string{"id"}) // this is okay, it does not bear significance in practice since no queries are generated
	assert.DeepEqual(t, plan.IndexByColumnName, map[string][]int{
		"id":         {0, 2},
		"created_at": {0, 3},
		"message":    {0, 4},
		"keks":       {1},
	})
}
