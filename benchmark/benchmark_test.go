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

const totalRecordCountForSelect = 10000

func makeTestDB(t testing.TB, recordCount int) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name()))
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(`CREATE TABLE entries (id INTEGER, message TEXT, PRIMARY KEY (id AUTOINCREMENT))`)
	if err != nil {
		return nil, err
	}

	if recordCount > 0 {
		// fill in some random-looking, but deterministic data
		stmt, err := db.Prepare(`INSERT INTO entries (id, message) VALUES (?, ?)`)
		if err != nil {
			return nil, err
		}
		for idx := range recordCount {
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
	}

	return db, nil
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
	db, err := makeTestDB(b, totalRecordCountForSelect)
	if err != nil {
		b.Fatal(err)
	}

	// test with different sizes of resultsets (N=1 is an OLTP-like workload,
	// then the larger N lean more towards the OLAP side of things)
	for selectedRecordCount := 1; selectedRecordCount < totalRecordCountForSelect; selectedRecordCount *= 10 {
		b.Run("N="+strconv.Itoa(selectedRecordCount), func(b *testing.B) {
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
				var records []GorpEntry
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
	db, err := makeTestDB(b, totalRecordCountForSelect)
	if err != nil {
		b.Fatal(err)
	}

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
		var r GorpEntry
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

func BenchmarkInsertAndDeleteOne(b *testing.B) {
	db, err := makeTestDB(b, 0)
	if err != nil {
		b.Fatal(err)
	}

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

	insertAndDeleteWithOblast := func(b *testing.B) {
		record := OblastEntry{Message: "hello"}
		err := store.Insert(db, &record)
		if err != nil {
			b.Error(err)
		}
		if record.ID == 0 {
			b.Errorf("ID was not filled!")
		}
		err = store.Delete(db, record)
		if err != nil {
			b.Error(err)
		}
	}
	insertAndDeleteWithGorp := func(b *testing.B) {
		record := GorpEntry{Message: "hello"}
		err := gdb.Insert(&record)
		if err != nil {
			b.Error(err)
		}
		if record.ID == 0 {
			b.Errorf("ID was not filled!")
		}
		_, err = gdb.Delete(&record)
		if err != nil {
			b.Error(err)
		}
	}
	insertAndDeleteWithSqlite := func(b *testing.B) {
		result, err := db.Exec(`INSERT INTO entries (message) VALUES (?)`, "hello")
		if err != nil {
			b.Error(err)
		}
		id, err := result.LastInsertId()
		if err != nil {
			b.Error(err)
		}
		_, err = db.Exec(`DELETE FROM entries WHERE id = ?`, id)
		if err != nil {
			b.Error(err)
		}
	}

	// run once to prewarm caches
	insertAndDeleteWithOblast(b)
	insertAndDeleteWithGorp(b)

	b.Run("via Gorp", func(b *testing.B) {
		for range b.N {
			insertAndDeleteWithGorp(b)
		}
	})
	b.Run("via Oblast", func(b *testing.B) {
		// TODO: extremely bad results for the insert/delete benchmark -> investigate
		for range b.N {
			insertAndDeleteWithOblast(b)
		}
	})
	b.Run("via SQLite", func(b *testing.B) {
		for range b.N {
			insertAndDeleteWithSqlite(b)
		}
	})
}
