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
	"go.xyrillian.de/oblast/internal/assert"
)

const totalRecordCount = 1000

func makeTestDB(t testing.TB) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name()))
	if err != nil {
		return nil, err
	}

	// fill in some random-looking, but deterministic data
	_, err = db.Exec(`CREATE TABLE entries (id INTEGER, message TEXT)`)
	if err != nil {
		return nil, err
	}
	stmt, err := db.Prepare(`INSERT INTO entries (id, message) VALUES (?, ?)`)
	if err != nil {
		return nil, err
	}
	for idx := range totalRecordCount {
		buf := sha256.Sum256([]byte(strconv.Itoa(idx)))
		_, err = stmt.Exec(idx, fmt.Sprintf("sha256:%x", buf[:]))
		if err != nil {
			return nil, err
		}
	}
	err = stmt.Close()
	if err != nil {
		return nil, err
	}

	return db, nil
}

func BenchmarkSelectMany(b *testing.B) {
	db, err := makeTestDB(b)
	if err != nil {
		b.Fatal(err)
	}

	// test with different sizes of resultsets (N=1 is an OLTP-like workload,
	// then the larger N lean more towards the OLAP side of things)
	for selectedRecordCount := 1; selectedRecordCount < totalRecordCount; selectedRecordCount *= 10 {
		b.Run("N="+strconv.Itoa(selectedRecordCount), func(b *testing.B) {
			// prepare the functions that will be benched
			type record struct {
				ID      int    `db:"id"`
				Message string `db:"message"`
			}
			store, err := oblast.NewStore[record](
				oblast.SqliteDialect(),
				oblast.TableNameIs("entries"),
			)
			if err != nil {
				b.Fatal(err)
			}
			gdb := gorp.DbMap{Db: db, Dialect: gorp.SqliteDialect{}}
			partialQuery := `id < ` + strconv.Itoa(selectedRecordCount)
			query := `SELECT * FROM entries WHERE ` + partialQuery //nolint:gosec

			selectWithOblast := func(b *testing.B) {
				records, err := store.Select(db, query)
				if err != nil {
					b.Error(err)
				}
				assert.Equal(b, len(records), selectedRecordCount)
			}

			selectWithOblastWhere := func(b *testing.B) {
				records, err := store.SelectWhere(db, partialQuery)
				if err != nil {
					b.Error(err)
				}
				assert.Equal(b, len(records), selectedRecordCount)
			}

			selectWithGorp := func(b *testing.B) {
				var records []record
				_, err := gdb.Select(&records, query)
				if err != nil {
					b.Error(err)
				}
				assert.Equal(b, len(records), selectedRecordCount)
			}

			selectWithSqlite := func(b *testing.B) {
				var count int
				rows, err := db.Query(query) //nolint:rowserrcheck // false positive
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
				assert.Equal(b, count, selectedRecordCount)
			}

			// run once to prewarm caches
			selectWithOblast(b)
			selectWithGorp(b)
			if b.Failed() {
				b.FailNow()
			}

			// run actual benchmark
			b.Run("via Gorp using Select", func(b *testing.B) {
				for range b.N {
					selectWithGorp(b)
				}
			})
			b.Run("via Oblast using Select", func(b *testing.B) {
				for range b.N {
					selectWithOblast(b)
				}
			})
			b.Run("via Oblast using SelectWhere", func(b *testing.B) {
				for range b.N {
					selectWithOblastWhere(b)
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

func BenchmarkSelectOne(b *testing.B) {
	db, err := makeTestDB(b)
	if err != nil {
		b.Fatal(err)
	}

	// grab a "random" record from the DB, not just the first or the last
	recordID := min(totalRecordCount*2/3, totalRecordCount)

	// prepare the functions that will be benched
	type record struct {
		ID      int    `db:"id"`
		Message string `db:"message"`
	}
	store, err := oblast.NewStore[record](
		oblast.SqliteDialect(),
		oblast.TableNameIs("entries"),
	)
	if err != nil {
		b.Fatal(err)
	}
	gdb := gorp.DbMap{Db: db, Dialect: gorp.SqliteDialect{}}
	partialQuery := `id = ` + strconv.Itoa(recordID)
	query := `SELECT * FROM entries WHERE ` + partialQuery

	selectWithOblast := func(b *testing.B) {
		r, err := store.SelectOne(db, query)
		if err != nil {
			b.Error(err)
		}
		assert.Equal(b, r.ID, recordID)
	}

	selectWithOblastWhere := func(b *testing.B) {
		r, err := store.SelectOneWhere(db, partialQuery)
		if err != nil {
			b.Error(err)
		}
		assert.Equal(b, r.ID, recordID)
	}

	selectWithGorp := func(b *testing.B) {
		var r record
		err := gdb.SelectOne(&r, query)
		if err != nil {
			b.Error(err)
		}
		assert.Equal(b, r.ID, recordID)
	}

	selectWithSqlite := func(b *testing.B) {
		var (
			id      int64
			message string
		)
		err := db.QueryRow(query).Scan(&id, &message)
		if err != nil {
			b.Error(err)
		}
		assert.Equal(b, id, int64(recordID))
	}

	// run once to prewarm caches
	selectWithOblast(b)
	selectWithGorp(b)
	if b.Failed() {
		b.FailNow()
	}

	// run actual benchmark
	b.Run("via Gorp using SelectOne", func(b *testing.B) {
		for range b.N {
			selectWithGorp(b)
		}
	})
	b.Run("via Oblast using SelectOne", func(b *testing.B) {
		for range b.N {
			selectWithOblast(b)
		}
	})
	b.Run("via Oblast using SelectOneWhere", func(b *testing.B) {
		for range b.N {
			selectWithOblastWhere(b)
		}
	})
	b.Run("just SQLite", func(b *testing.B) {
		for range b.N {
			selectWithSqlite(b)
		}
	})
}
