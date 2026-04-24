// SPDX-FileCopyrightText: 2026 Stefan Majewsky <majewsky@gmx.net>
// SPDX-License-Identifier: Apache-2.0

package oblast

// ^ NOTE: This is testing internal types and thus must reside in the same package.

import (
	"reflect"
	"testing"
	"time"

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
	plan, err := buildPlan(reflect.TypeFor[Log](), PostgresDialect(), planOpts{
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

func TestQueryConstructionBasic(t *testing.T) {
	type record struct {
		ID          int64 `db:",auto"`
		Description string
		CreatedAt   time.Time `db:"CreatedAt"`
	}
	opts := planOpts{
		TableName:             "basic_records",
		PrimaryKeyColumnNames: []string{"ID"},
	}

	t.Run("MariaDBDialect", func(t *testing.T) {
		plan, err := buildPlan(reflect.TypeFor[record](), MariaDBDialect(), opts)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, plan.Select.Query, "SELECT `ID`, `Description`, `CreatedAt` FROM `basic_records` WHERE ")
		assert.DeepEqual(t, plan.Select.ArgumentIndexes, nil)
		assert.DeepEqual(t, plan.Select.ScanIndexes, [][]int{{0}, {1}, {2}})
		assert.Equal(t, plan.Insert.Query, "INSERT INTO `basic_records` (`Description`, `CreatedAt`) VALUES (?, ?) RETURNING `ID`")
		assert.DeepEqual(t, plan.Insert.ArgumentIndexes, [][]int{{1}, {2}})
		assert.DeepEqual(t, plan.Insert.ScanIndexes, [][]int{{0}})
		assert.Equal(t, plan.Upsert.Query, "")
		assert.DeepEqual(t, plan.Upsert.ArgumentIndexes, nil)
		assert.DeepEqual(t, plan.Upsert.ScanIndexes, nil)
		assert.Equal(t, plan.Update.Query, "UPDATE `basic_records` SET `Description` = ?, `CreatedAt` = ? WHERE `ID` = ?")
		assert.DeepEqual(t, plan.Update.ArgumentIndexes, [][]int{{1}, {2}, {0}})
		assert.DeepEqual(t, plan.Update.ScanIndexes, nil)
		assert.Equal(t, plan.Delete.Query, "DELETE FROM `basic_records` WHERE `ID` = ?")
		assert.DeepEqual(t, plan.Delete.ArgumentIndexes, [][]int{{0}})
		assert.DeepEqual(t, plan.Delete.ScanIndexes, nil)
	})

	t.Run("PostgresDialect", func(t *testing.T) {
		plan, err := buildPlan(reflect.TypeFor[record](), PostgresDialect(), opts)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, plan.Select.Query, `SELECT "ID", "Description", "CreatedAt" FROM "basic_records" WHERE `)
		assert.DeepEqual(t, plan.Select.ArgumentIndexes, nil)
		assert.DeepEqual(t, plan.Select.ScanIndexes, [][]int{{0}, {1}, {2}})
		assert.Equal(t, plan.Insert.Query, `INSERT INTO "basic_records" ("Description", "CreatedAt") VALUES ($1, $2) RETURNING "ID"`)
		assert.DeepEqual(t, plan.Insert.ArgumentIndexes, [][]int{{1}, {2}})
		assert.DeepEqual(t, plan.Insert.ScanIndexes, [][]int{{0}})
		assert.Equal(t, plan.Upsert.Query, "")
		assert.DeepEqual(t, plan.Upsert.ArgumentIndexes, nil)
		assert.DeepEqual(t, plan.Upsert.ScanIndexes, nil)
		assert.Equal(t, plan.Update.Query, `UPDATE "basic_records" SET "Description" = $1, "CreatedAt" = $2 WHERE "ID" = $3`)
		assert.DeepEqual(t, plan.Update.ArgumentIndexes, [][]int{{1}, {2}, {0}})
		assert.DeepEqual(t, plan.Update.ScanIndexes, nil)
		assert.Equal(t, plan.Delete.Query, `DELETE FROM "basic_records" WHERE "ID" = $1`)
		assert.DeepEqual(t, plan.Delete.ArgumentIndexes, [][]int{{0}})
		assert.DeepEqual(t, plan.Delete.ScanIndexes, nil)
	})

	t.Run("SqliteDialect", func(t *testing.T) {
		plan, err := buildPlan(reflect.TypeFor[record](), SqliteDialect(), opts)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, plan.Select.Query, `SELECT "ID", "Description", "CreatedAt" FROM "basic_records" WHERE `)
		assert.DeepEqual(t, plan.Select.ArgumentIndexes, nil)
		assert.DeepEqual(t, plan.Select.ScanIndexes, [][]int{{0}, {1}, {2}})
		assert.Equal(t, plan.Insert.Query, `INSERT INTO "basic_records" ("Description", "CreatedAt") VALUES (?, ?) RETURNING "ID"`)
		assert.DeepEqual(t, plan.Insert.ArgumentIndexes, [][]int{{1}, {2}})
		assert.DeepEqual(t, plan.Insert.ScanIndexes, [][]int{{0}})
		assert.Equal(t, plan.Upsert.Query, "")
		assert.DeepEqual(t, plan.Upsert.ArgumentIndexes, nil)
		assert.DeepEqual(t, plan.Upsert.ScanIndexes, nil)
		assert.Equal(t, plan.Update.Query, `UPDATE "basic_records" SET "Description" = ?, "CreatedAt" = ? WHERE "ID" = ?`)
		assert.DeepEqual(t, plan.Update.ArgumentIndexes, [][]int{{1}, {2}, {0}})
		assert.DeepEqual(t, plan.Update.ScanIndexes, nil)
		assert.Equal(t, plan.Delete.Query, `DELETE FROM "basic_records" WHERE "ID" = ?`)
		assert.DeepEqual(t, plan.Delete.ArgumentIndexes, [][]int{{0}})
		assert.DeepEqual(t, plan.Delete.ScanIndexes, nil)
	})
}

func TestQueryConstructionWithOnlyPrimaryKey(t *testing.T) {
	type relation struct {
		FooID int64 `db:"foo_id"`
		BarID int64 `db:"bar_id"`
	}
	opts := planOpts{
		TableName:             "foo_bar_relations",
		PrimaryKeyColumnNames: []string{"foo_id", "bar_id"},
	}

	t.Run("MariaDBDialect", func(t *testing.T) {
		plan, err := buildPlan(reflect.TypeFor[relation](), MariaDBDialect(), opts)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, plan.Select.Query, "SELECT `foo_id`, `bar_id` FROM `foo_bar_relations` WHERE ")
		assert.DeepEqual(t, plan.Select.ArgumentIndexes, nil)
		assert.DeepEqual(t, plan.Select.ScanIndexes, [][]int{{0}, {1}})
		assert.Equal(t, plan.Insert.Query, "INSERT INTO `foo_bar_relations` (`foo_id`, `bar_id`) VALUES (?, ?)")
		assert.DeepEqual(t, plan.Insert.ArgumentIndexes, [][]int{{0}, {1}})
		assert.DeepEqual(t, plan.Insert.ScanIndexes, nil)
		assert.Equal(t, plan.Upsert.Query, "INSERT INTO `foo_bar_relations` (`foo_id`, `bar_id`) VALUES (?, ?) ON DUPLICATE KEY UPDATE `foo_id` = VALUES(`foo_id`)")
		assert.DeepEqual(t, plan.Upsert.ArgumentIndexes, [][]int{{0}, {1}})
		assert.DeepEqual(t, plan.Upsert.ScanIndexes, nil)
		assert.Equal(t, plan.Update.Query, "")
		assert.DeepEqual(t, plan.Update.ArgumentIndexes, nil)
		assert.DeepEqual(t, plan.Update.ScanIndexes, nil)
		assert.Equal(t, plan.Delete.Query, "DELETE FROM `foo_bar_relations` WHERE `foo_id` = ? AND `bar_id` = ?")
		assert.DeepEqual(t, plan.Delete.ArgumentIndexes, [][]int{{0}, {1}})
		assert.DeepEqual(t, plan.Delete.ScanIndexes, nil)
	})

	t.Run("PostgresDialect", func(t *testing.T) {
		plan, err := buildPlan(reflect.TypeFor[relation](), PostgresDialect(), opts)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, plan.Select.Query, `SELECT "foo_id", "bar_id" FROM "foo_bar_relations" WHERE `)
		assert.DeepEqual(t, plan.Select.ArgumentIndexes, nil)
		assert.DeepEqual(t, plan.Select.ScanIndexes, [][]int{{0}, {1}})
		assert.Equal(t, plan.Insert.Query, `INSERT INTO "foo_bar_relations" ("foo_id", "bar_id") VALUES ($1, $2)`)
		assert.DeepEqual(t, plan.Insert.ArgumentIndexes, [][]int{{0}, {1}})
		assert.DeepEqual(t, plan.Insert.ScanIndexes, nil)
		assert.Equal(t, plan.Upsert.Query, `INSERT INTO "foo_bar_relations" ("foo_id", "bar_id") VALUES ($1, $2) ON CONFLICT ("foo_id", "bar_id") DO NOTHING`)
		assert.DeepEqual(t, plan.Upsert.ArgumentIndexes, [][]int{{0}, {1}})
		assert.DeepEqual(t, plan.Upsert.ScanIndexes, nil)
		assert.Equal(t, plan.Update.Query, "")
		assert.DeepEqual(t, plan.Update.ArgumentIndexes, nil)
		assert.DeepEqual(t, plan.Update.ScanIndexes, nil)
		assert.Equal(t, plan.Delete.Query, `DELETE FROM "foo_bar_relations" WHERE "foo_id" = $1 AND "bar_id" = $2`)
		assert.DeepEqual(t, plan.Delete.ArgumentIndexes, [][]int{{0}, {1}})
		assert.DeepEqual(t, plan.Delete.ScanIndexes, nil)
	})

	t.Run("SqliteDialect", func(t *testing.T) {
		plan, err := buildPlan(reflect.TypeFor[relation](), SqliteDialect(), opts)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, plan.Select.Query, `SELECT "foo_id", "bar_id" FROM "foo_bar_relations" WHERE `)
		assert.DeepEqual(t, plan.Select.ArgumentIndexes, nil)
		assert.DeepEqual(t, plan.Select.ScanIndexes, [][]int{{0}, {1}})
		assert.Equal(t, plan.Insert.Query, `INSERT INTO "foo_bar_relations" ("foo_id", "bar_id") VALUES (?, ?)`)
		assert.DeepEqual(t, plan.Insert.ArgumentIndexes, [][]int{{0}, {1}})
		assert.DeepEqual(t, plan.Insert.ScanIndexes, nil)
		assert.Equal(t, plan.Upsert.Query, `INSERT INTO "foo_bar_relations" ("foo_id", "bar_id") VALUES (?, ?) ON CONFLICT ("foo_id", "bar_id") DO NOTHING`)
		assert.DeepEqual(t, plan.Upsert.ArgumentIndexes, [][]int{{0}, {1}})
		assert.DeepEqual(t, plan.Upsert.ScanIndexes, nil)
		assert.Equal(t, plan.Update.Query, "")
		assert.DeepEqual(t, plan.Update.ArgumentIndexes, nil)
		assert.DeepEqual(t, plan.Update.ScanIndexes, nil)
		assert.Equal(t, plan.Delete.Query, `DELETE FROM "foo_bar_relations" WHERE "foo_id" = ? AND "bar_id" = ?`)
		assert.DeepEqual(t, plan.Delete.ArgumentIndexes, [][]int{{0}, {1}})
		assert.DeepEqual(t, plan.Delete.ScanIndexes, nil)
	})
}

func TestQueryConstructionWithoutPrimaryKey(t *testing.T) {
	type relation struct {
		FooID int64 `db:"foo_id"`
		BarID int64 `db:"bar_id"`
	}
	opts := planOpts{
		TableName: "foo_bar_relations",
	}

	t.Run("MariaDBDialect", func(t *testing.T) {
		plan, err := buildPlan(reflect.TypeFor[relation](), MariaDBDialect(), opts)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, plan.Select.Query, "SELECT `foo_id`, `bar_id` FROM `foo_bar_relations` WHERE ")
		assert.DeepEqual(t, plan.Select.ArgumentIndexes, nil)
		assert.DeepEqual(t, plan.Select.ScanIndexes, [][]int{{0}, {1}})
		assert.Equal(t, plan.Insert.Query, "INSERT INTO `foo_bar_relations` (`foo_id`, `bar_id`) VALUES (?, ?)")
		assert.DeepEqual(t, plan.Insert.ArgumentIndexes, [][]int{{0}, {1}})
		assert.DeepEqual(t, plan.Insert.ScanIndexes, nil)
		assert.Equal(t, plan.Upsert.Query, "")
		assert.DeepEqual(t, plan.Upsert.ArgumentIndexes, nil)
		assert.DeepEqual(t, plan.Upsert.ScanIndexes, nil)
		assert.Equal(t, plan.Update.Query, "")
		assert.DeepEqual(t, plan.Update.ArgumentIndexes, nil)
		assert.DeepEqual(t, plan.Update.ScanIndexes, nil)
		assert.Equal(t, plan.Delete.Query, "")
		assert.DeepEqual(t, plan.Delete.ArgumentIndexes, nil)
		assert.DeepEqual(t, plan.Delete.ScanIndexes, nil)
	})

	t.Run("PostgresDialect", func(t *testing.T) {
		plan, err := buildPlan(reflect.TypeFor[relation](), PostgresDialect(), opts)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, plan.Select.Query, `SELECT "foo_id", "bar_id" FROM "foo_bar_relations" WHERE `)
		assert.DeepEqual(t, plan.Select.ArgumentIndexes, nil)
		assert.DeepEqual(t, plan.Select.ScanIndexes, [][]int{{0}, {1}})
		assert.Equal(t, plan.Insert.Query, `INSERT INTO "foo_bar_relations" ("foo_id", "bar_id") VALUES ($1, $2)`)
		assert.DeepEqual(t, plan.Insert.ArgumentIndexes, [][]int{{0}, {1}})
		assert.DeepEqual(t, plan.Insert.ScanIndexes, nil)
		assert.Equal(t, plan.Upsert.Query, "")
		assert.DeepEqual(t, plan.Upsert.ArgumentIndexes, nil)
		assert.DeepEqual(t, plan.Upsert.ScanIndexes, nil)
		assert.Equal(t, plan.Update.Query, "")
		assert.DeepEqual(t, plan.Update.ArgumentIndexes, nil)
		assert.DeepEqual(t, plan.Update.ScanIndexes, nil)
		assert.Equal(t, plan.Delete.Query, "")
		assert.DeepEqual(t, plan.Delete.ArgumentIndexes, nil)
		assert.DeepEqual(t, plan.Delete.ScanIndexes, nil)
	})

	t.Run("SqliteDialect", func(t *testing.T) {
		plan, err := buildPlan(reflect.TypeFor[relation](), SqliteDialect(), opts)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, plan.Select.Query, `SELECT "foo_id", "bar_id" FROM "foo_bar_relations" WHERE `)
		assert.DeepEqual(t, plan.Select.ArgumentIndexes, nil)
		assert.DeepEqual(t, plan.Select.ScanIndexes, [][]int{{0}, {1}})
		assert.Equal(t, plan.Insert.Query, `INSERT INTO "foo_bar_relations" ("foo_id", "bar_id") VALUES (?, ?)`)
		assert.DeepEqual(t, plan.Insert.ArgumentIndexes, [][]int{{0}, {1}})
		assert.DeepEqual(t, plan.Insert.ScanIndexes, nil)
		assert.Equal(t, plan.Upsert.Query, "")
		assert.DeepEqual(t, plan.Upsert.ArgumentIndexes, nil)
		assert.DeepEqual(t, plan.Upsert.ScanIndexes, nil)
		assert.Equal(t, plan.Update.Query, "")
		assert.DeepEqual(t, plan.Update.ArgumentIndexes, nil)
		assert.DeepEqual(t, plan.Update.ScanIndexes, nil)
		assert.Equal(t, plan.Delete.Query, "")
		assert.DeepEqual(t, plan.Delete.ArgumentIndexes, nil)
		assert.DeepEqual(t, plan.Delete.ScanIndexes, nil)
	})
}

func TestQueryConstructionImpossble(t *testing.T) {
	type unstructuredData struct {
		Foo int
		Bar *string
	}
	opts := planOpts{}

	testWith := func(dialect Dialect) func(*testing.T) {
		return func(t *testing.T) {
			plan, err := buildPlan(reflect.TypeFor[unstructuredData](), dialect, opts)
			if err != nil {
				t.Error(err)
			}

			assert.Equal(t, plan.Select.Query, "")
			assert.DeepEqual(t, plan.Select.ArgumentIndexes, nil)
			assert.DeepEqual(t, plan.Select.ScanIndexes, nil)
			assert.Equal(t, plan.Insert.Query, "")
			assert.DeepEqual(t, plan.Insert.ArgumentIndexes, nil)
			assert.DeepEqual(t, plan.Insert.ScanIndexes, nil)
			assert.Equal(t, plan.Upsert.Query, "")
			assert.DeepEqual(t, plan.Upsert.ArgumentIndexes, nil)
			assert.DeepEqual(t, plan.Upsert.ScanIndexes, nil)
			assert.Equal(t, plan.Update.Query, "")
			assert.DeepEqual(t, plan.Update.ArgumentIndexes, nil)
			assert.DeepEqual(t, plan.Update.ScanIndexes, nil)
			assert.Equal(t, plan.Delete.Query, "")
			assert.DeepEqual(t, plan.Delete.ArgumentIndexes, nil)
			assert.DeepEqual(t, plan.Delete.ScanIndexes, nil)
		}
	}

	t.Run("MariaDBDialect", testWith(MariaDBDialect()))
	t.Run("PostgresDialect", testWith(PostgresDialect()))
	t.Run("SqliteDialect", testWith(SqliteDialect()))
}

func TestQueryConstructionWithMultiplePrimaryKeyColumns(t *testing.T) {
	type record struct {
		GroupID   int64     `db:"group_id"`
		Name      string    `db:"name"`
		CreatedAt time.Time `db:"created_at"`
	}
	opts := planOpts{
		TableName:             "complex_records",
		PrimaryKeyColumnNames: []string{"group_id", "name"},
	}

	t.Run("MariaDBDialect", func(t *testing.T) {
		plan, err := buildPlan(reflect.TypeFor[record](), MariaDBDialect(), opts)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, plan.Select.Query, "SELECT `group_id`, `name`, `created_at` FROM `complex_records` WHERE ")
		assert.DeepEqual(t, plan.Select.ArgumentIndexes, nil)
		assert.DeepEqual(t, plan.Select.ScanIndexes, [][]int{{0}, {1}, {2}})
		assert.Equal(t, plan.Insert.Query, "INSERT INTO `complex_records` (`group_id`, `name`, `created_at`) VALUES (?, ?, ?)")
		assert.DeepEqual(t, plan.Insert.ArgumentIndexes, [][]int{{0}, {1}, {2}})
		assert.DeepEqual(t, plan.Insert.ScanIndexes, nil)
		assert.Equal(t, plan.Upsert.Query, "INSERT INTO `complex_records` (`group_id`, `name`, `created_at`) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE `created_at` = VALUES(`created_at`)")
		assert.DeepEqual(t, plan.Upsert.ArgumentIndexes, [][]int{{0}, {1}, {2}})
		assert.DeepEqual(t, plan.Upsert.ScanIndexes, nil)
		assert.Equal(t, plan.Update.Query, "UPDATE `complex_records` SET `created_at` = ? WHERE `group_id` = ? AND `name` = ?")
		assert.DeepEqual(t, plan.Update.ArgumentIndexes, [][]int{{2}, {0}, {1}})
		assert.DeepEqual(t, plan.Update.ScanIndexes, nil)
		assert.Equal(t, plan.Delete.Query, "DELETE FROM `complex_records` WHERE `group_id` = ? AND `name` = ?")
		assert.DeepEqual(t, plan.Delete.ArgumentIndexes, [][]int{{0}, {1}})
		assert.DeepEqual(t, plan.Delete.ScanIndexes, nil)
	})

	t.Run("PostgresDialect", func(t *testing.T) {
		plan, err := buildPlan(reflect.TypeFor[record](), PostgresDialect(), opts)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, plan.Select.Query, `SELECT "group_id", "name", "created_at" FROM "complex_records" WHERE `)
		assert.DeepEqual(t, plan.Select.ArgumentIndexes, nil)
		assert.DeepEqual(t, plan.Select.ScanIndexes, [][]int{{0}, {1}, {2}})
		assert.Equal(t, plan.Insert.Query, `INSERT INTO "complex_records" ("group_id", "name", "created_at") VALUES ($1, $2, $3)`)
		assert.DeepEqual(t, plan.Insert.ArgumentIndexes, [][]int{{0}, {1}, {2}})
		assert.DeepEqual(t, plan.Insert.ScanIndexes, nil)
		assert.Equal(t, plan.Upsert.Query, `INSERT INTO "complex_records" ("group_id", "name", "created_at") VALUES ($1, $2, $3) ON CONFLICT ("group_id", "name") DO UPDATE SET "created_at" = EXCLUDED."created_at"`)
		assert.DeepEqual(t, plan.Upsert.ArgumentIndexes, [][]int{{0}, {1}, {2}})
		assert.DeepEqual(t, plan.Upsert.ScanIndexes, nil)
		assert.Equal(t, plan.Update.Query, `UPDATE "complex_records" SET "created_at" = $1 WHERE "group_id" = $2 AND "name" = $3`)
		assert.DeepEqual(t, plan.Update.ArgumentIndexes, [][]int{{2}, {0}, {1}})
		assert.DeepEqual(t, plan.Update.ScanIndexes, nil)
		assert.Equal(t, plan.Delete.Query, `DELETE FROM "complex_records" WHERE "group_id" = $1 AND "name" = $2`)
		assert.DeepEqual(t, plan.Delete.ArgumentIndexes, [][]int{{0}, {1}})
		assert.DeepEqual(t, plan.Delete.ScanIndexes, nil)
	})

	t.Run("SqliteDialect", func(t *testing.T) {
		plan, err := buildPlan(reflect.TypeFor[record](), SqliteDialect(), opts)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, plan.Select.Query, `SELECT "group_id", "name", "created_at" FROM "complex_records" WHERE `)
		assert.DeepEqual(t, plan.Select.ArgumentIndexes, nil)
		assert.DeepEqual(t, plan.Select.ScanIndexes, [][]int{{0}, {1}, {2}})
		assert.Equal(t, plan.Insert.Query, `INSERT INTO "complex_records" ("group_id", "name", "created_at") VALUES (?, ?, ?)`)
		assert.DeepEqual(t, plan.Insert.ArgumentIndexes, [][]int{{0}, {1}, {2}})
		assert.DeepEqual(t, plan.Insert.ScanIndexes, nil)
		assert.Equal(t, plan.Upsert.Query, `INSERT INTO "complex_records" ("group_id", "name", "created_at") VALUES (?, ?, ?) ON CONFLICT ("group_id", "name") DO UPDATE SET "created_at" = EXCLUDED."created_at"`)
		assert.DeepEqual(t, plan.Upsert.ArgumentIndexes, [][]int{{0}, {1}, {2}})
		assert.DeepEqual(t, plan.Upsert.ScanIndexes, nil)
		assert.Equal(t, plan.Update.Query, `UPDATE "complex_records" SET "created_at" = ? WHERE "group_id" = ? AND "name" = ?`)
		assert.DeepEqual(t, plan.Update.ArgumentIndexes, [][]int{{2}, {0}, {1}})
		assert.DeepEqual(t, plan.Update.ScanIndexes, nil)
		assert.Equal(t, plan.Delete.Query, `DELETE FROM "complex_records" WHERE "group_id" = ? AND "name" = ?`)
		assert.DeepEqual(t, plan.Delete.ArgumentIndexes, [][]int{{0}, {1}})
		assert.DeepEqual(t, plan.Delete.ScanIndexes, nil)
	})
}

func TestQueryConstructionWithMultipleAutoColumns(t *testing.T) {
	type record struct {
		ID        int64     `db:"id,auto"`
		Name      string    `db:"name"`
		CreatedAt time.Time `db:"created_at,auto"`
	}
	opts := planOpts{
		TableName:             "autogenerated_records",
		PrimaryKeyColumnNames: []string{"id"},
	}

	t.Run("MariaDBDialect", func(t *testing.T) {
		plan, err := buildPlan(reflect.TypeFor[record](), MariaDBDialect(), opts)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, plan.Select.Query, "SELECT `id`, `name`, `created_at` FROM `autogenerated_records` WHERE ")
		assert.DeepEqual(t, plan.Select.ArgumentIndexes, nil)
		assert.DeepEqual(t, plan.Select.ScanIndexes, [][]int{{0}, {1}, {2}})
		assert.Equal(t, plan.Insert.Query, "INSERT INTO `autogenerated_records` (`name`) VALUES (?) RETURNING `id`, `created_at`")
		assert.DeepEqual(t, plan.Insert.ArgumentIndexes, [][]int{{1}})
		assert.DeepEqual(t, plan.Insert.ScanIndexes, [][]int{{0}, {2}})
		assert.Equal(t, plan.Upsert.Query, "")
		assert.DeepEqual(t, plan.Upsert.ArgumentIndexes, nil)
		assert.DeepEqual(t, plan.Upsert.ScanIndexes, nil)
		assert.Equal(t, plan.Update.Query, "UPDATE `autogenerated_records` SET `name` = ?, `created_at` = ? WHERE `id` = ?")
		assert.DeepEqual(t, plan.Update.ArgumentIndexes, [][]int{{1}, {2}, {0}})
		assert.DeepEqual(t, plan.Update.ScanIndexes, nil)
		assert.Equal(t, plan.Delete.Query, "DELETE FROM `autogenerated_records` WHERE `id` = ?")
		assert.DeepEqual(t, plan.Delete.ArgumentIndexes, [][]int{{0}})
		assert.DeepEqual(t, plan.Delete.ScanIndexes, nil)
	})

	t.Run("PostgresDialect", func(t *testing.T) {
		plan, err := buildPlan(reflect.TypeFor[record](), PostgresDialect(), opts)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, plan.Select.Query, `SELECT "id", "name", "created_at" FROM "autogenerated_records" WHERE `)
		assert.DeepEqual(t, plan.Select.ArgumentIndexes, nil)
		assert.DeepEqual(t, plan.Select.ScanIndexes, [][]int{{0}, {1}, {2}})
		assert.Equal(t, plan.Insert.Query, `INSERT INTO "autogenerated_records" ("name") VALUES ($1) RETURNING "id", "created_at"`)
		assert.DeepEqual(t, plan.Insert.ArgumentIndexes, [][]int{{1}})
		assert.DeepEqual(t, plan.Insert.ScanIndexes, [][]int{{0}, {2}})
		assert.Equal(t, plan.Upsert.Query, "")
		assert.DeepEqual(t, plan.Upsert.ArgumentIndexes, nil)
		assert.DeepEqual(t, plan.Upsert.ScanIndexes, nil)
		assert.Equal(t, plan.Update.Query, `UPDATE "autogenerated_records" SET "name" = $1, "created_at" = $2 WHERE "id" = $3`)
		assert.DeepEqual(t, plan.Update.ArgumentIndexes, [][]int{{1}, {2}, {0}})
		assert.DeepEqual(t, plan.Update.ScanIndexes, nil)
		assert.Equal(t, plan.Delete.Query, `DELETE FROM "autogenerated_records" WHERE "id" = $1`)
		assert.DeepEqual(t, plan.Delete.ArgumentIndexes, [][]int{{0}})
		assert.DeepEqual(t, plan.Delete.ScanIndexes, nil)
	})

	t.Run("SqliteDialect", func(t *testing.T) {
		plan, err := buildPlan(reflect.TypeFor[record](), SqliteDialect(), opts)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, plan.Select.Query, `SELECT "id", "name", "created_at" FROM "autogenerated_records" WHERE `)
		assert.DeepEqual(t, plan.Select.ArgumentIndexes, nil)
		assert.DeepEqual(t, plan.Select.ScanIndexes, [][]int{{0}, {1}, {2}})
		assert.Equal(t, plan.Insert.Query, `INSERT INTO "autogenerated_records" ("name") VALUES (?) RETURNING "id", "created_at"`)
		assert.DeepEqual(t, plan.Insert.ArgumentIndexes, [][]int{{1}})
		assert.DeepEqual(t, plan.Insert.ScanIndexes, [][]int{{0}, {2}})
		assert.Equal(t, plan.Upsert.Query, "")
		assert.DeepEqual(t, plan.Upsert.ArgumentIndexes, nil)
		assert.DeepEqual(t, plan.Upsert.ScanIndexes, nil)
		assert.Equal(t, plan.Update.Query, `UPDATE "autogenerated_records" SET "name" = ?, "created_at" = ? WHERE "id" = ?`)
		assert.DeepEqual(t, plan.Update.ArgumentIndexes, [][]int{{1}, {2}, {0}})
		assert.DeepEqual(t, plan.Update.ScanIndexes, nil)
		assert.Equal(t, plan.Delete.Query, `DELETE FROM "autogenerated_records" WHERE "id" = ?`)
		assert.DeepEqual(t, plan.Delete.ArgumentIndexes, [][]int{{0}})
		assert.DeepEqual(t, plan.Delete.ScanIndexes, nil)
	})
}

func TestPlanErrorCases(t *testing.T) {
	type recordUsedViaPointer struct {
		ID int64 `db:"id"`
	}

	_, err := NewStore[*recordUsedViaPointer](SqliteDialect())
	assert.Equal(t, err.Error(), `cannot use type *oblast.recordUsedViaPointer for queries: `+
		`expected struct type, but got kind "ptr"`)

	type recordWithDuplicateTags struct {
		Foo int64 `db:"Bar"`
		Qux float64
		Bar string
	}
	_, err = NewStore[recordWithDuplicateTags](SqliteDialect())
	assert.Equal(t, err.Error(), `cannot use type oblast.recordWithDuplicateTags for queries: `+
		"duplicate tag `db:\"Bar\"` on field index [0], but also on field index [2]")

	type recordWithUnusedTransparentStruct struct {
		ID        int64
		CreatedAt time.Time // has no exported fields!
	}
	_, err = NewStore[recordWithUnusedTransparentStruct](SqliteDialect())
	assert.Equal(t, err.Error(), `cannot use type oblast.recordWithUnusedTransparentStruct for queries: `+
		"field \"CreatedAt\" of type time.Time does not contain any mapped fields (to map this whole field to a DB column, add an explicit `db:\"...\"` tag)")

	type recordWithPKButNoTableName struct {
		ID   int64  `db:"id"`
		Name string `db:"name"`
	}
	_, err = NewStore[recordWithPKButNoTableName](SqliteDialect(),
		PrimaryKeyIs("id"),
	)
	assert.Equal(t, err.Error(), `cannot use type oblast.recordWithPKButNoTableName for queries: `+
		`cannot declare a primary key without also providing the TableNameIs option`)

	type recordWithUnknownPK struct {
		ID   int64  `db:"id"`
		Name string `db:"name"`
	}
	_, err = NewStore[recordWithUnknownPK](SqliteDialect(),
		TableNameIs("records"),
		PrimaryKeyIs("record_id"),
	)
	assert.Equal(t, err.Error(), `cannot use type oblast.recordWithUnknownPK for queries: `+
		"no field has tag `db:\"record_id\"`, but a field of this name was declared in the primary key")

	type recordWithWeirdTagOption struct {
		ID          int64  `db:",auto"`
		Name        string `db:",unique"`
		Description string
	}
	_, err = NewStore[recordWithWeirdTagOption](SqliteDialect())
	assert.Equal(t, err.Error(), `cannot use type oblast.recordWithWeirdTagOption for queries: `+
		"unknown option `db:\",unique\"` on field \"Name\"")
}
