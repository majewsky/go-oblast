// SPDX-FileCopyrightText: 2026 Stefan Majewsky <majewsky@gmx.net>
// SPDX-License-Identifier: Apache-2.0

package oblast_test

import (
	"database/sql"
	"testing"
	"time"

	"go.xyrillian.de/oblast"
	"go.xyrillian.de/oblast/internal/assert"
	"go.xyrillian.de/oblast/internal/mock"
	"go.xyrillian.de/oblast/internal/must"
)

func TestSelectReturningSomeRecords(t *testing.T) {
	md := mock.NewDriver()
	db := sql.OpenDB(md)

	type basicRecord struct {
		ID   int64  `db:"id"`
		Name string `db:"name"`
	}
	store := must.Return(oblast.NewStore[basicRecord](
		oblast.SqliteDialect(),
		oblast.TableNameIs("basic_records"),
		oblast.PrimaryKeyIs("id"),
	))(t)

	t.Run("using Store.Select", func(t *testing.T) {
		md.ForQuery(`SELECT * FROM basic_records WHERE id < ?`).
			ExpectQueryWithArgs(3).
			AndReturnColumns("name", "id").
			WithRow("foo", 1).
			WithRow("bar", 2)
		records := must.Return(store.Select(db, `SELECT * FROM basic_records WHERE id < ?`, 3))(t)
		assert.SliceEqual(t, records,
			basicRecord{1, "foo"},
			basicRecord{2, "bar"},
		)
	})

	t.Run("using Store.SelectWhere", func(t *testing.T) {
		md.ForQuery(`SELECT "id", "name" FROM "basic_records" WHERE id < ?`).
			ExpectQueryWithArgs(3).
			AndReturnColumns("id", "name").
			WithRow(1, "ffoo").
			WithRow(2, "bbar")
		records := must.Return(store.SelectWhere(db, `id < ?`, 3))(t)
		assert.SliceEqual(t, records,
			basicRecord{1, "ffoo"},
			basicRecord{2, "bbar"},
		)
	})

	t.Run("using Store.SelectOne", func(t *testing.T) {
		md.ForQuery(`SELECT * FROM basic_records WHERE id < ?`).
			ExpectQueryWithArgs(3).
			AndReturnColumns("name", "id").
			WithRow("fffoo", 1).
			WithRow("bbbar", 2)
		record := must.Return(store.SelectOne(db, `SELECT * FROM basic_records WHERE id < ?`, 3))(t)
		assert.Equal(t, record, basicRecord{1, "fffoo"})
	})

	t.Run("using Store.SelectOneWhere", func(t *testing.T) {
		md.ForQuery(`SELECT "id", "name" FROM "basic_records" WHERE id < ?`).
			ExpectQueryWithArgs(3).
			AndReturnColumns("id", "name").
			WithRow(1, "ffffoo").
			WithRow(2, "bbbbar")
		record := must.Return(store.SelectOneWhere(db, `id < ?`, 3))(t)
		assert.Equal(t, record, basicRecord{1, "ffffoo"})
	})
}

func TestSelectReturningNoRecords(t *testing.T) {
	md := mock.NewDriver()
	db := sql.OpenDB(md)

	type basicRecord struct {
		ID   int64  `db:"id"`
		Name string `db:"name"`
	}
	store := must.Return(oblast.NewStore[basicRecord](
		oblast.SqliteDialect(),
		oblast.TableNameIs("basic_records"),
		oblast.PrimaryKeyIs("id"),
	))(t)

	t.Run("using Store.Select", func(t *testing.T) {
		md.ForQuery(`SELECT * FROM basic_records WHERE id < ?`).
			ExpectQueryWithArgs(3).
			AndReturnColumns("name", "id")
		records := must.Return(store.Select(db, `SELECT * FROM basic_records WHERE id < ?`, 3))(t)
		assert.SliceEqual(t, records, nil...)
	})

	t.Run("using Store.SelectWhere", func(t *testing.T) {
		md.ForQuery(`SELECT "id", "name" FROM "basic_records" WHERE id < ?`).
			ExpectQueryWithArgs(3).
			AndReturnColumns("id", "name")
		records := must.Return(store.SelectWhere(db, `id < ?`, 3))(t)
		assert.SliceEqual(t, records, nil...)
	})

	t.Run("using Store.SelectOne", func(t *testing.T) {
		md.ForQuery(`SELECT * FROM basic_records WHERE id < ?`).
			ExpectQueryWithArgs(3).
			AndReturnColumns("name", "id")
		_, err := store.SelectOne(db, `SELECT * FROM basic_records WHERE id < ?`, 3)
		assert.Equal(t, err.Error(), sql.ErrNoRows.Error())
	})

	t.Run("using Store.SelectOneWhere", func(t *testing.T) {
		md.ForQuery(`SELECT "id", "name" FROM "basic_records" WHERE id < ?`).
			ExpectQueryWithArgs(3).
			AndReturnColumns("id", "name")
		_, err := store.SelectOneWhere(db, `id < ?`, 3)
		assert.Equal(t, err.Error(), sql.ErrNoRows.Error())
	})
}

func TestSelectIntoUnexpectedField(t *testing.T) {
	md := mock.NewDriver()
	db := sql.OpenDB(md)

	type basicRecord struct {
		ID          int64  `db:"id"`
		Description string `db:"desc"` // but DB knows only the field "name"!
	}
	store := must.Return(oblast.NewStore[basicRecord](
		oblast.SqliteDialect(),
		oblast.TableNameIs("basic_records"),
		oblast.PrimaryKeyIs("id"),
	))(t)

	expectedError := "result has column \"name\" in position 0, but no field in type basicRecord has `db:\"name\"`"

	// NOTE: This problem cannot occur with SelectWhere() and SelectOneWhere() because of their use of query generation.

	t.Run("using Store.Select", func(t *testing.T) {
		md.ForQuery(`SELECT * FROM basic_records WHERE id < ?`).
			ExpectQueryWithArgs(3).
			AndReturnColumns("name", "id").
			WithRow("foo", 1).
			WithRow("bar", 2)
		_, err := store.Select(db, `SELECT * FROM basic_records WHERE id < ?`, 3)
		assert.Equal(t, err.Error(), expectedError)
	})

	t.Run("using Store.SelectOne", func(t *testing.T) {
		md.ForQuery(`SELECT * FROM basic_records WHERE id < ?`).
			ExpectQueryWithArgs(3).
			AndReturnColumns("name", "id").
			WithRow("ffoo", 1).
			WithRow("bbar", 2)
		_, err := store.SelectOne(db, `SELECT * FROM basic_records WHERE id < ?`, 3)
		assert.Equal(t, err.Error(), expectedError)
	})
}

func TestSelectWithScanError(t *testing.T) {
	md := mock.NewDriver()
	db := sql.OpenDB(md)

	type basicRecord struct {
		ID        int64     `db:"id"`
		CreatedAt time.Time `db:"created_at"` // but the DB will give us strings that are not timestamps
	}
	store := must.Return(oblast.NewStore[basicRecord](
		oblast.SqliteDialect(),
		oblast.TableNameIs("basic_records"),
		oblast.PrimaryKeyIs("id"),
	))(t)

	expectedError := `sql: Scan error on column index 1, name "created_at": unsupported Scan, storing driver.Value type string into type *time.Time`

	t.Run("using Store.Select", func(t *testing.T) {
		md.ForQuery(`SELECT * FROM basic_records WHERE id < ?`).
			ExpectQueryWithArgs(3).
			AndReturnColumns("id", "created_at").
			WithRow(1, "foo").
			WithRow(2, "bar")
		_, err := store.Select(db, `SELECT * FROM basic_records WHERE id < ?`, 3)
		assert.Equal(t, err.Error(), expectedError)
	})

	t.Run("using Store.SelectWhere", func(t *testing.T) {
		md.ForQuery(`SELECT "id", "created_at" FROM "basic_records" WHERE id < ?`).
			ExpectQueryWithArgs(3).
			AndReturnColumns("id", "created_at").
			WithRow(1, "ffoo").
			WithRow(2, "bbar")
		_, err := store.SelectWhere(db, `id < ?`, 3)
		assert.Equal(t, err.Error(), expectedError)
	})

	t.Run("using Store.SelectOne", func(t *testing.T) {
		md.ForQuery(`SELECT * FROM basic_records WHERE id < ?`).
			ExpectQueryWithArgs(3).
			AndReturnColumns("id", "created_at").
			WithRow(1, "fffoo").
			WithRow(2, "bbbar")
		_, err := store.SelectOne(db, `SELECT * FROM basic_records WHERE id < ?`, 3)
		assert.Equal(t, err.Error(), expectedError)
	})

	t.Run("using Store.SelectOneWhere", func(t *testing.T) {
		md.ForQuery(`SELECT "id", "created_at" FROM "basic_records" WHERE id < ?`).
			ExpectQueryWithArgs(3).
			AndReturnColumns("id", "created_at").
			WithRow(1, "ffffoo").
			WithRow(2, "bbbbar")
		_, err := store.SelectOneWhere(db, `id < ?`, 3)
		assert.Equal(t, err.Error(), expectedError)
	})
}

// TODO: test error capture during Rows.Close()
// TODO: check for maximum test coverage in select.go
