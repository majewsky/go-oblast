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
	"go.xyrillian.de/oblast/internal/must"
)

var (
	totalRecordCountForSelect = 10000
	batchSizesForSelect       = []int{1, 10, 100, 1000}
	batchSizesForInsertDelete = []int{1, 2, 4, 8, 16, 100}
)

func makeTestDB(t testing.TB, recordCount int) *sql.DB {
	db := must.Return(sql.Open("sqlite3", fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())))(t)
	_ = must.Return(db.Exec(`CREATE TABLE entries (id INTEGER, message TEXT, PRIMARY KEY (id AUTOINCREMENT))`))(t)

	if recordCount > 0 {
		// fill in some random-looking, but deterministic data
		stmt := must.Return(db.Prepare(`INSERT INTO entries (id, message) VALUES (?, ?)`))(t)
		for idx := range recordCount {
			buf := sha256.Sum256([]byte(strconv.Itoa(idx)))
			_ = must.Return(stmt.Exec(idx, fmt.Sprintf("sha256:%x", buf[:])))(t)
		}
		must.Succeed(t, stmt.Close())
	}

	return db
}

type OblastEntry struct {
	ID      int    `db:"id,auto"`
	Message string `db:"message"`
}

type GorpEntry struct {
	ID      int    `db:"id"`
	Message string `db:"message"`
}

func BenchmarkSelectMany(b *testing.B) {
	db := makeTestDB(b, totalRecordCountForSelect)

	// test with different sizes of resultsets (N=1 is an OLTP-like workload,
	// then the larger N lean more towards the OLAP side of things)
	for _, batchSize := range batchSizesForSelect {
		b.Run("N="+strconv.Itoa(batchSize), func(b *testing.B) {
			// prepare the functions that will be benched
			store, err := oblast.NewStore[OblastEntry](
				oblast.SqliteDialect(),
				oblast.TableNameIs("entries"),
				oblast.PrimaryKeyIs("id"),
			)
			if err != nil {
				b.Fatal(err)
			}
			gdb := gorp.DbMap{Db: db, Dialect: gorp.SqliteDialect{}}
			partialQuery := `id < ` + strconv.Itoa(batchSize)
			query := `SELECT * FROM entries WHERE ` + partialQuery

			selectWithOblast := func(b *testing.B) {
				records := must.Return(store.Select(db, query))(b)
				assert.Equal(b, len(records), batchSize)
			}

			selectWithOblastWhere := func(b *testing.B) {
				records := must.Return(store.SelectWhere(db, partialQuery))(b)
				assert.Equal(b, len(records), batchSize)
			}

			selectWithGorp := func(b *testing.B) {
				var records []GorpEntry
				_ = must.Return(gdb.Select(&records, query))(b)
				assert.Equal(b, len(records), batchSize)
			}

			selectWithSqlite := func(b *testing.B) {
				var count int
				rows := must.Return(db.Query(query))(b) //nolint:rowserrcheck // false positive
				var (
					id      int64
					message string
				)
				for rows.Next() {
					must.Succeed(b, rows.Scan(&id, &message))
					if id != 20000 && message != "" { // always true; ensures that values are not optimized away
						count++
					}
				}
				must.Succeed(b, rows.Close())
				assert.Equal(b, count, batchSize)
			}

			// run once to prewarm caches
			selectWithOblast(b)
			selectWithGorp(b)
			if b.Failed() {
				b.FailNow()
			}

			// run actual benchmark
			b.Run("via Gorp using Select", func(b *testing.B) {
				for b.Loop() {
					selectWithGorp(b)
				}
			})
			b.Run("via Oblast using Select", func(b *testing.B) {
				for b.Loop() {
					selectWithOblast(b)
				}
			})
			b.Run("via Oblast using SelectWhere", func(b *testing.B) {
				for b.Loop() {
					selectWithOblastWhere(b)
				}
			})
			b.Run("just SQLite", func(b *testing.B) {
				for b.Loop() {
					selectWithSqlite(b)
				}
			})
		})
	}
}

func BenchmarkSelectOne(b *testing.B) {
	db := makeTestDB(b, totalRecordCountForSelect)

	// grab a "random" record from the DB, not just the first or the last
	recordID := min(totalRecordCountForSelect*2/3, totalRecordCountForSelect)

	// prepare the functions that will be benched
	store, err := oblast.NewStore[OblastEntry](
		oblast.SqliteDialect(),
		oblast.TableNameIs("entries"),
		oblast.PrimaryKeyIs("id"),
	)
	if err != nil {
		b.Fatal(err)
	}
	gdb := gorp.DbMap{Db: db, Dialect: gorp.SqliteDialect{}}
	partialQuery := `id = ` + strconv.Itoa(recordID)
	query := `SELECT * FROM entries WHERE ` + partialQuery

	selectWithOblast := func(b *testing.B) {
		r := must.Return(store.SelectOne(db, query))(b)
		assert.Equal(b, r.ID, recordID)
	}

	selectWithOblastWhere := func(b *testing.B) {
		r := must.Return(store.SelectOneWhere(db, partialQuery))(b)
		assert.Equal(b, r.ID, recordID)
	}

	selectWithGorp := func(b *testing.B) {
		var r GorpEntry
		must.Succeed(b, gdb.SelectOne(&r, query))
		assert.Equal(b, r.ID, recordID)
	}

	selectWithSqlite := func(b *testing.B) {
		var (
			id      int64
			message string
		)
		must.Succeed(b, db.QueryRow(query).Scan(&id, &message))
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
		for b.Loop() {
			selectWithGorp(b)
		}
	})
	b.Run("via Oblast using SelectOne", func(b *testing.B) {
		for b.Loop() {
			selectWithOblast(b)
		}
	})
	b.Run("via Oblast using SelectOneWhere", func(b *testing.B) {
		for b.Loop() {
			selectWithOblastWhere(b)
		}
	})
	b.Run("just SQLite", func(b *testing.B) {
		for b.Loop() {
			selectWithSqlite(b)
		}
	})
}

func BenchmarkInsertAndDelete(b *testing.B) {
	db := makeTestDB(b, 0)

	// prepare the functions that will be benched
	store, err := oblast.NewStore[OblastEntry](
		oblast.SqliteDialect(),
		oblast.TableNameIs("entries"),
		oblast.PrimaryKeyIs("id"),
	)
	if err != nil {
		b.Fatal(err)
	}
	gdb := gorp.DbMap{Db: db, Dialect: gorp.SqliteDialect{}}
	gdb.AddTableWithName(GorpEntry{}, "entries").SetKeys(true, "id")

	// test with different amounts of records
	for _, batchSize := range batchSizesForInsertDelete {
		b.Run("N="+strconv.Itoa(batchSize), func(b *testing.B) {
			// prepare the functions that will be benched
			insertAndDeleteWithOblast := func(b *testing.B) {
				records := make([]OblastEntry, batchSize)
				for idx := range records {
					records[idx] = OblastEntry{Message: "hello"}
				}
				records = must.Return(store.Insert(db, records...))(b)
				for _, r := range records {
					if r.ID == 0 {
						b.Errorf("ID was not filled!")
					}
				}
				must.Succeed(b, store.Delete(db, records...))
			}

			insertAndDeleteWithGorp := func(b *testing.B) {
				records := make([]any, batchSize)
				for idx := range records {
					records[idx] = &GorpEntry{Message: "hello"}
				}
				must.Succeed(b, gdb.Insert(records...))
				for _, r := range records {
					if r.(*GorpEntry).ID == 0 {
						b.Errorf("ID was not filled!")
					}
				}
				_ = must.Return(gdb.Delete(records...))(b)
			}

			insertAndDeleteWithStraightSqlite := func(b *testing.B) {
				ids := make([]int64, batchSize)
				for idx := range ids {
					result := must.Return(db.Exec(`INSERT INTO entries (message) VALUES (?)`, "hello"))(b)
					ids[idx] = must.Return(result.LastInsertId())(b)
				}
				for _, id := range ids {
					_ = must.Return(db.Exec(`DELETE FROM entries WHERE id = ?`, id))(b)
				}
			}

			insertAndDeleteWithPreparedSqlite := func(b *testing.B) {
				ids := make([]int64, batchSize)
				stmtInsert := must.Return(db.Prepare(`INSERT INTO entries (message) VALUES (?)`))(b)
				defer stmtInsert.Close()
				for idx := range ids {
					result := must.Return(stmtInsert.Exec("hello"))(b)
					ids[idx] = must.Return(result.LastInsertId())(b)
				}
				stmtDelete := must.Return(db.Prepare(`DELETE FROM entries WHERE id = ?`))(b)
				defer stmtDelete.Close()
				for _, id := range ids {
					_ = must.Return(stmtDelete.Exec(id))(b)
				}
			}

			// run once to prewarm caches
			insertAndDeleteWithOblast(b)
			insertAndDeleteWithGorp(b)

			b.Run("via Gorp", func(b *testing.B) {
				for b.Loop() {
					insertAndDeleteWithGorp(b)
				}
			})
			b.Run("via Oblast", func(b *testing.B) {
				// TODO: extremely bad results for the insert/delete benchmark -> investigate
				for b.Loop() {
					insertAndDeleteWithOblast(b)
				}
			})
			b.Run("just straight SQLite", func(b *testing.B) {
				for b.Loop() {
					insertAndDeleteWithStraightSqlite(b)
				}
			})
			b.Run("just prepared SQLite", func(b *testing.B) {
				for b.Loop() {
					insertAndDeleteWithPreparedSqlite(b)
				}
			})
		})
	}
}
