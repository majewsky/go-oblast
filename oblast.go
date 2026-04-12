// SPDX-FileCopyrightText: 2026 Stefan Majewsky <majewsky@gmx.net>
// SPDX-License-Identifier: Apache-2.0

// Package oblast is an ORM library for Go, focusing specifically on just the loading and storing of records in the most efficient manner possible.
// No utilities are provided for generating DDL or managing schema migrations, or for building complex OLAP queries.
//
// # Usage pattern
//
// To use this library, first declare a record type, and create a [Store] for it once to analyze the type and prepare the respective OLTP queries:
//
//	type LogEntry struct {
//		ID        int64     `db:"id,auto"`
//		CreatedAt time.Time `db:"created_at"`
//		Message   string    `db:"message"`
//	}
//	var logEntryStore = oblast.NewStore[LogEntry](
//		oblast.PostgresDialect(),
//		oblast.TableNameIs("log_entries"),
//		oblast.PrimaryKeyIs("id"),
//	)
//
// Then use it many times to perform load and store operations:
//
//	func doStuff(db *sql.DB) error {
//		newEntry := LogEntry{
//			CreatedAt: time.Now(),
//			Message: "Hello World.",
//		}
//		err := logEntryStore.Insert(db, &newEntry)
//		if err != nil {
//			return err
//		}
//		fmt.Printf("created log entry %d", newEntry.ID)
//
//		allEntries, err := logEntryStore.SelectWhere(db, `created_at < NOW()`)
//		if err != nil {
//		  return err
//		}
//		fmt.Printf("there are %d log entries so far", len(allEntries))
//	}
package oblast // import "go.xyrillian.de/oblast"

import (
	"database/sql"

	"go.xyrillian.de/oblast/internal"
)

// PlanOption is an option that can be given to NewStore() to influence query planning for a certain type of record.
type PlanOption func(*internal.PlanOpts)

// TableNameIs is a PlanOption for record types that correspond to exactly one database table (as opposed to a join of multiple tables).
// This option is required to enable any of the methods of [Store] that use partially or fully auto-generated query strings.
func TableNameIs(name string) PlanOption {
	return func(opts *internal.PlanOpts) { opts.TableName = name }
}

// PrimaryKeyIs is a PlanOption for record types that correspond to a database table with a primary key.
// This option is required to enable use of the [Store.Update] and [Store.Delete] methods.
func PrimaryKeyIs(columnNames ...string) PlanOption {
	return func(opts *internal.PlanOpts) { opts.PrimaryKeyColumnNames = columnNames }
}

// Handle is an interface for functions providing direct DB access.
// It covers methods provided by both *sql.DB and *sql.Tx, thus allowing functions using it to be used both within and outside of transactions.
type Handle interface {
	Exec(query string, args ...any) (sql.Result, error)
	Prepare(query string) (*sql.Stmt, error)
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
}

// static assertion that the respective types implement the interface
var (
	_ Handle = &sql.DB{}
	_ Handle = &sql.Tx{}
)
