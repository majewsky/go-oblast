// SPDX-FileCopyrightText: 2026 Stefan Majewsky <majewsky@gmx.net>
// SPDX-License-Identifier: Apache-2.0

package oblast_test

import (
	"database/sql"
	"strconv"
	"testing"

	"go.xyrillian.de/oblast"
	"go.xyrillian.de/oblast/internal/assert"
	"go.xyrillian.de/oblast/internal/mock"
	"go.xyrillian.de/oblast/internal/must"
)

func TestInsertBasicUsingLastInsertId(t *testing.T) {
	md := mock.NewDriver()
	db := sql.OpenDB(md)

	type basicRecord struct {
		ID   int64  `db:"id,auto"`
		Name string `db:"name"`
	}
	store := oblast.MustNewStore[basicRecord](
		oblast.SqliteDialect(),
		oblast.TableNameIs("basic_records"),
		oblast.PrimaryKeyIs("id"),
	)

	for _, batchSize := range []int{1, oblast.PrepareThreshold - 1, oblast.PrepareThreshold + 1} {
		t.Run("N="+strconv.Itoa(batchSize), func(t *testing.T) {
			records := make([]basicRecord, batchSize)
			for idx := range batchSize {
				records[idx] = basicRecord{Name: "new"}
				md.ForQuery(`INSERT INTO "basic_records" ("name") VALUES (?)`).
					ExpectExecWithArgs("new").
					AndReturnLastInsertId(int64(42 + idx)).
					AndReturnRowsAffected(1)
			}
			records = must.Return(store.Insert(db, records...))(t)
			for idx, r := range records {
				assert.Equal(t, r.ID, int64(42+idx))
			}
		})
	}
}

func TestInsertBasicUsingReturningClause(t *testing.T) {
	md := mock.NewDriver()
	db := sql.OpenDB(md)

	type basicRecord struct {
		ID   int64  `db:"id,auto"`
		Name string `db:"name"`
	}
	store := oblast.MustNewStore[basicRecord](
		oblast.PostgresDialect(),
		oblast.TableNameIs("basic_records"),
		oblast.PrimaryKeyIs("id"),
	)

	for _, batchSize := range []int{1, oblast.PrepareThreshold - 1, oblast.PrepareThreshold + 1} {
		t.Run("N="+strconv.Itoa(batchSize), func(t *testing.T) {
			records := make([]basicRecord, batchSize)
			for idx := range batchSize {
				records[idx] = basicRecord{Name: "new"}
				md.ForQuery(`INSERT INTO "basic_records" ("name") VALUES ($1) RETURNING "id"`).
					ExpectQueryWithArgs("new").
					AndReturnColumns("id").
					WithRow(int64(42 + idx))
			}
			records = must.Return(store.Insert(db, records...))(t)
			for idx, r := range records {
				assert.Equal(t, r.ID, int64(42+idx))
			}
		})
	}
}

func TestUpdateBasic(t *testing.T) {
	md := mock.NewDriver()
	db := sql.OpenDB(md)

	type basicRecord struct {
		ID   int64  `db:"id,auto"`
		Name string `db:"name"`
	}
	store := oblast.MustNewStore[basicRecord](
		oblast.PostgresDialect(),
		oblast.TableNameIs("basic_records"),
		oblast.PrimaryKeyIs("id"),
	)

	for _, batchSize := range []int{1, oblast.PrepareThreshold - 1, oblast.PrepareThreshold + 1} {
		t.Run("N="+strconv.Itoa(batchSize), func(t *testing.T) {
			records := make([]basicRecord, batchSize)
			for idx := range batchSize {
				r := basicRecord{ID: int64(42 + idx), Name: "updated"}
				records[idx] = r
				md.ForQuery(`UPDATE "basic_records" SET "name" = $1 WHERE "id" = $2`).
					ExpectExecWithArgs(r.Name, r.ID).
					AndReturnRowsAffected(1)
			}
			must.Succeed(t, store.Update(db, records...))
		})
	}
}

func TestDeleteBasic(t *testing.T) {
	md := mock.NewDriver()
	db := sql.OpenDB(md)

	type basicRecord struct {
		ID   int64  `db:"id,auto"`
		Name string `db:"name"`
	}
	store := oblast.MustNewStore[basicRecord](
		oblast.PostgresDialect(),
		oblast.TableNameIs("basic_records"),
		oblast.PrimaryKeyIs("id"),
	)

	for _, batchSize := range []int{1, oblast.PrepareThreshold - 1, oblast.PrepareThreshold + 1} {
		t.Run("N="+strconv.Itoa(batchSize), func(t *testing.T) {
			records := make([]basicRecord, batchSize)
			for idx := range batchSize {
				r := basicRecord{ID: int64(42 + idx), Name: "removed"}
				records[idx] = r
				md.ForQuery(`DELETE FROM "basic_records" WHERE "id" = $1`).
					ExpectExecWithArgs(r.ID).
					AndReturnRowsAffected(1)
			}
			must.Succeed(t, store.Delete(db, records...))
		})
	}
}

// TODO: more test coverage for query.go
