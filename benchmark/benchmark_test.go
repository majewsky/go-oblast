// SPDX-FileCopyrightText: 2026 Stefan Majewsky <majewsky@gmx.net>
// SPDX-License-Identifier: Apache-2.0

package main_test

import (
	"crypto/sha256"
	"database/sql"
	"fmt"
	"strconv"
	"testing"

	"github.com/go-gorp/gorp/v3"
	_ "github.com/mattn/go-sqlite3"
	"go.xyrillian.de/oblast"
)

func BenchmarkSelect(b *testing.B) {
	const totalRecordCount = 1000

	db, err := sql.Open("sqlite3", "file:foobar?mode=memory&cache=shared")
	if err != nil {
		b.Fatal(err)
	}

	// fill in some random-looking, but deterministic data
	_, err = db.Exec(`CREATE TABLE entries (id INTEGER, message TEXT)`)
	if err != nil {
		b.Fatal(err)
	}
	stmt, err := db.Prepare(`INSERT INTO entries (id, message) VALUES (?, ?)`)
	if err != nil {
		b.Fatal(err)
	}
	for idx := range totalRecordCount {
		buf := sha256.Sum256([]byte(strconv.Itoa(idx)))
		_, err = stmt.Exec(idx, fmt.Sprintf("sha256:%x", buf[:]))
		if err != nil {
			b.Fatal(err)
		}
	}
	err = stmt.Close()
	if err != nil {
		b.Fatal(err)
	}

	// test with different sizes of resultsets (N=1 is an OLTP-like workload,
	// then the larger N lean more towards the OLAP side of things)
	for _, selectedRecordCount := range []int{1, 10, 100, 1000} {
		b.Run("N="+strconv.Itoa(selectedRecordCount), func(b *testing.B) {
			// prepare the functions that will be benched
			type record struct {
				ID      int    `db:"id"`
				Message string `db:"message"`
			}
			store, err := oblast.NewStore[record](oblast.SqliteDialect())
			if err != nil {
				b.Fatal(err)
			}
			gdb := gorp.DbMap{Db: db, Dialect: gorp.SqliteDialect{}}
			query := `SELECT * FROM entries WHERE id < ` + strconv.Itoa(selectedRecordCount) //nolint:gosec

			selectWithOblast := func(b *testing.B) {
				records, err := store.Select(db, query)
				if err != nil {
					b.Error(err)
				}
				if len(records) != selectedRecordCount {
					b.Errorf("expected %d, but got %d records", selectedRecordCount, len(records))
				}
			}

			selectWithGorp := func(b *testing.B) {
				var records []record
				_, err := gdb.Select(&records, query)
				if err != nil {
					b.Error(err)
				}
				if len(records) != selectedRecordCount {
					b.Errorf("expected %d, but got %d records", selectedRecordCount, len(records))
				}
			}

			selectWithSqlite := func(b *testing.B) {
				var count int
				rows, err := db.Query(query)
				if err != nil {
					b.Error(err)
				}
				var (
					id      int64
					message string
				)
				for rows.Next() {
					err := rows.Scan(&id, &message)
					if err != nil {
						b.Error(err)
					}
					if id != 20000 && message != "" { // always true; ensures that values are not optimized away
						count++
					}
				}
				err = rows.Close()
				if err != nil {
					b.Error(err)
				}
				if count != selectedRecordCount {
					b.Errorf("expected %d, but got %d records", selectedRecordCount, count)
				}
			}

			// run once to prewarm caches
			selectWithOblast(b)
			selectWithGorp(b)
			if b.Failed() {
				b.FailNow()
			}

			// run actual benchmark
			b.Run("via Gorp", func(b *testing.B) {
				for range b.N {
					selectWithGorp(b)
				}
			})
			b.Run("via Oblast", func(b *testing.B) {
				for range b.N {
					selectWithOblast(b)
				}
			})
			b.Run("just SQLite", func(b *testing.B) {
				for range b.N {
					selectWithSqlite(b)
				}
			})
		})
	}
}
