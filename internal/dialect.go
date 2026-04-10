// SPDX-FileCopyrightText: 2026 Stefan Majewsky <majewsky@gmx.net>
// SPDX-License-Identifier: Apache-2.0

package internal

import (
	"strconv"
	"strings"
)

// Dialect is a copy of the interface of the same name in package oblast.
// We cannot refer to that interface within this package because that would constitute a cyclic dependency.
type Dialect interface {
	Placeholder(i int) string
	QuoteIdentifier(name string) string
	UsesLastInsertID() bool
	InsertSuffixForAutoColumns(columns []string) string
}

// PostgresDialect is the dialect of PostgreSQL databases.
type PostgresDialect struct{}

func (PostgresDialect) Placeholder(i int) string           { return "$" + strconv.Itoa(i) }
func (PostgresDialect) QuoteIdentifier(name string) string { return `"` + name + `"` }
func (PostgresDialect) UsesLastInsertID() bool             { return false }

func (p PostgresDialect) InsertSuffixForAutoColumns(columns []string) string {
	quotedColumns := make([]string, len(columns))
	for idx, name := range columns {
		quotedColumns[idx] = p.QuoteIdentifier(name)
	}
	return ` RETURNING ` + strings.Join(quotedColumns, ", ")
}

// SqliteDialect is the dialect of SQLite databases.
type SqliteDialect struct{}

func (SqliteDialect) Placeholder(_ int) string                           { return "?" }
func (SqliteDialect) QuoteIdentifier(name string) string                 { return `"` + name + `"` }
func (SqliteDialect) UsesLastInsertID() bool                             { return true }
func (SqliteDialect) InsertSuffixForAutoColumns(columns []string) string { return "" }
