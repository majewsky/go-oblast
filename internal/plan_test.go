// SPDX-FileCopyrightText: 2026 Stefan Majewsky <majewsky@gmx.net>
// SPDX-License-Identifier: Apache-2.0

package internal_test

import (
	"reflect"
	"testing"
	"time"

	"go.xyrillian.de/oblast/internal"
	"go.xyrillian.de/oblast/internal/assert"
)

func TestPlanFieldTraversal(t *testing.T) {
	type Timestamps struct {
		CreatedAt time.Time  `db:"created_at"`
		UpdatedAt *time.Time `db:"updated_at"`
	}
	type yetMoreTimestamps struct {
		DeletedAt *time.Time `db:"deleted_at"`
	}
	type Log struct {
		ID       int64 `db:"id,auto"`
		Message  string
		private1 bool `db:"private1"` //nolint:unused
		Ignored  any  `db:"-"`
		Timestamps
		yetMoreTimestamps
	}

	// check that the plan for Log:
	// 1. has no IndexByColumnName entries for marker types
	// 2. uses the field name as a column name for "Message"
	// 3. ignores "private1" because it cannot be written through reflection
	// 4. ignores "Ignored" because its column name is "-"
	// 5. traverses into "Timestamps" and includes its fields as well
	// 6. traverses into "yetMoreTimestamps" as well (despite the extra pointer and the type being private)
	// 7. recognizes "id" as an autofilled column
	plan, err := internal.BuildPlan(reflect.TypeFor[Log](), internal.PostgresDialect{}, internal.PlanOpts{
		TableName:             "log_entries",
		PrimaryKeyColumnNames: []string{"id"},
	})
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, plan.TableName, "log_entries")
	assert.DeepEqual(t, plan.AllColumnNames, []string{"id", "Message", "created_at", "updated_at", "deleted_at"})
	assert.DeepEqual(t, plan.PrimaryKeyColumnNames, []string{"id"})
	assert.DeepEqual(t, plan.AutoColumnNames, []string{"id"})
	assert.DeepEqual(t, plan.IndexByColumnName, map[string][]int{
		"id":         {0},
		"Message":    {1},
		"created_at": {4, 0},
		"updated_at": {4, 1},
		"deleted_at": {5, 0},
	})
}

// TODO: test that, during Select(), assignment into embedded fields with pointer-to-struct type works (docs say that this might panic if we do not allocate into the pointer first)

func TestQueryConstructionBasic(t *testing.T) {
	type record struct {
		ID          int64 `db:",auto"`
		Description string
		CreatedAt   time.Time
	}
	opts := internal.PlanOpts{
		TableName:             "basic_records",
		PrimaryKeyColumnNames: []string{"ID"},
	}

	t.Run("PostgresDialect", func(t *testing.T) {
		plan, err := internal.BuildPlan(reflect.TypeFor[record](), internal.PostgresDialect{}, opts)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, plan.Insert.Query, `INSERT INTO "basic_records" ("Description", "CreatedAt") VALUES ($1, $2) RETURNING "ID"`)
		assert.DeepEqual(t, plan.Insert.ArgumentIndexes, [][]int{{1}, {2}})
		assert.Equal(t, plan.Update.Query, `UPDATE "basic_records" SET "Description" = $1, "CreatedAt" = $2 WHERE "ID" = $3`)
		assert.DeepEqual(t, plan.Update.ArgumentIndexes, [][]int{{1}, {2}, {0}})
		assert.Equal(t, plan.Delete.Query, `DELETE FROM "basic_records" WHERE "ID" = $1`)
		assert.DeepEqual(t, plan.Delete.ArgumentIndexes, [][]int{{0}})
	})

	t.Run("SqliteDialect", func(t *testing.T) {
		plan, err := internal.BuildPlan(reflect.TypeFor[record](), internal.SqliteDialect{}, opts)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, plan.Insert.Query, `INSERT INTO "basic_records" ("Description", "CreatedAt") VALUES (?, ?)`)
		assert.DeepEqual(t, plan.Insert.ArgumentIndexes, [][]int{{1}, {2}})
		assert.Equal(t, plan.Update.Query, `UPDATE "basic_records" SET "Description" = ?, "CreatedAt" = ? WHERE "ID" = ?`)
		assert.DeepEqual(t, plan.Update.ArgumentIndexes, [][]int{{1}, {2}, {0}})
		assert.Equal(t, plan.Delete.Query, `DELETE FROM "basic_records" WHERE "ID" = ?`)
		assert.DeepEqual(t, plan.Delete.ArgumentIndexes, [][]int{{0}})
	})
}

func TestQueryConstructionWithoutPrimaryKey(t *testing.T) {
	type relation struct {
		FooID int64 `db:"foo_id"`
		BarID int64 `db:"bar_id"`
	}
	opts := internal.PlanOpts{
		TableName: "foo_bar_relations",
	}

	t.Run("PostgresDialect", func(t *testing.T) {
		plan, err := internal.BuildPlan(reflect.TypeFor[relation](), internal.PostgresDialect{}, opts)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, plan.Insert.Query, `INSERT INTO "foo_bar_relations" ("foo_id", "bar_id") VALUES ($1, $2)`)
		assert.DeepEqual(t, plan.Insert.ArgumentIndexes, [][]int{{0}, {1}})
		assert.Equal(t, plan.Update.Query, "")
		assert.DeepEqual(t, plan.Update.ArgumentIndexes, nil)
		assert.Equal(t, plan.Delete.Query, "")
		assert.DeepEqual(t, plan.Delete.ArgumentIndexes, nil)
	})

	t.Run("SqliteDialect", func(t *testing.T) {
		plan, err := internal.BuildPlan(reflect.TypeFor[relation](), internal.SqliteDialect{}, opts)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, plan.Insert.Query, `INSERT INTO "foo_bar_relations" ("foo_id", "bar_id") VALUES (?, ?)`)
		assert.DeepEqual(t, plan.Insert.ArgumentIndexes, [][]int{{0}, {1}})
		assert.Equal(t, plan.Update.Query, "")
		assert.DeepEqual(t, plan.Update.ArgumentIndexes, nil)
		assert.Equal(t, plan.Delete.Query, "")
		assert.DeepEqual(t, plan.Delete.ArgumentIndexes, nil)
	})
}

func TestQueryConstructionImpossble(t *testing.T) {
	type unstructuredData struct {
		Foo int
		Bar string
	}
	opts := internal.PlanOpts{}

	testWith := func(dialect internal.Dialect) func(*testing.T) {
		return func(t *testing.T) {
			plan, err := internal.BuildPlan(reflect.TypeFor[unstructuredData](), dialect, opts)
			if err != nil {
				t.Error(err)
			}

			assert.Equal(t, plan.Insert.Query, "")
			assert.DeepEqual(t, plan.Insert.ArgumentIndexes, nil)
			assert.Equal(t, plan.Update.Query, "")
			assert.DeepEqual(t, plan.Update.ArgumentIndexes, nil)
			assert.Equal(t, plan.Delete.Query, "")
			assert.DeepEqual(t, plan.Delete.ArgumentIndexes, nil)
		}
	}

	t.Run("PostgresDialect", testWith(internal.PostgresDialect{}))
	t.Run("SqliteDialect", testWith(internal.SqliteDialect{}))
}

func TestQueryConstructionWithMultiplePrimaryKeyColumns(t *testing.T) {
	type record struct {
		GroupID   int64     `db:"group_id"`
		Name      string    `db:"name"`
		CreatedAt time.Time `db:"created_at"`
	}
	opts := internal.PlanOpts{
		TableName:             "complex_records",
		PrimaryKeyColumnNames: []string{"group_id", "name"},
	}

	t.Run("PostgresDialect", func(t *testing.T) {
		plan, err := internal.BuildPlan(reflect.TypeFor[record](), internal.PostgresDialect{}, opts)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, plan.Insert.Query, `INSERT INTO "complex_records" ("group_id", "name", "created_at") VALUES ($1, $2, $3)`)
		assert.DeepEqual(t, plan.Insert.ArgumentIndexes, [][]int{{0}, {1}, {2}})
		assert.Equal(t, plan.Update.Query, `UPDATE "complex_records" SET "created_at" = $1 WHERE "group_id" = $2 AND "name" = $3`)
		assert.DeepEqual(t, plan.Update.ArgumentIndexes, [][]int{{2}, {0}, {1}})
		assert.Equal(t, plan.Delete.Query, `DELETE FROM "complex_records" WHERE "group_id" = $1 AND "name" = $2`)
		assert.DeepEqual(t, plan.Delete.ArgumentIndexes, [][]int{{0}, {1}})
	})

	t.Run("SqliteDialect", func(t *testing.T) {
		plan, err := internal.BuildPlan(reflect.TypeFor[record](), internal.SqliteDialect{}, opts)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, plan.Insert.Query, `INSERT INTO "complex_records" ("group_id", "name", "created_at") VALUES (?, ?, ?)`)
		assert.DeepEqual(t, plan.Insert.ArgumentIndexes, [][]int{{0}, {1}, {2}})
		assert.Equal(t, plan.Update.Query, `UPDATE "complex_records" SET "created_at" = ? WHERE "group_id" = ? AND "name" = ?`)
		assert.DeepEqual(t, plan.Update.ArgumentIndexes, [][]int{{2}, {0}, {1}})
		assert.Equal(t, plan.Delete.Query, `DELETE FROM "complex_records" WHERE "group_id" = ? AND "name" = ?`)
		assert.DeepEqual(t, plan.Delete.ArgumentIndexes, [][]int{{0}, {1}})
	})
}

func TestQueryConstructionWithMultipleAutoColumns(t *testing.T) {
	type record struct {
		ID        int64     `db:"id,auto"`
		Name      string    `db:"name"`
		CreatedAt time.Time `db:"created_at,auto"`
	}
	opts := internal.PlanOpts{
		TableName:             "autogenerated_records",
		PrimaryKeyColumnNames: []string{"id"},
	}

	t.Run("PostgresDialect", func(t *testing.T) {
		plan, err := internal.BuildPlan(reflect.TypeFor[record](), internal.PostgresDialect{}, opts)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, plan.Insert.Query, `INSERT INTO "autogenerated_records" ("name") VALUES ($1) RETURNING "id", "created_at"`)
		assert.DeepEqual(t, plan.Insert.ArgumentIndexes, [][]int{{1}})
		assert.Equal(t, plan.Update.Query, `UPDATE "autogenerated_records" SET "name" = $1, "created_at" = $2 WHERE "id" = $3`)
		assert.DeepEqual(t, plan.Update.ArgumentIndexes, [][]int{{1}, {2}, {0}})
		assert.Equal(t, plan.Delete.Query, `DELETE FROM "autogenerated_records" WHERE "id" = $1`)
		assert.DeepEqual(t, plan.Delete.ArgumentIndexes, [][]int{{0}})
	})

	t.Run("SqliteDialect", func(t *testing.T) {
		_, err := internal.BuildPlan(reflect.TypeFor[record](), internal.SqliteDialect{}, opts)
		assert.Equal(t, err.Error(), `cannot use type go.xyrillian.de/oblast/internal_test.record for queries: multiple columns are marked as auto-filled (id, created_at), but this SQL dialect only supports at most one per table`)
	})
}
