// SPDX-FileCopyrightText: 2026 Stefan Majewsky <majewsky@gmx.net>
// SPDX-License-Identifier: Apache-2.0

package oblast

import (
	"fmt"
	"strconv"
	"strings"
)

// Dialect accounts for differences between different SQL dialects
// that are relevant to query generation within Oblast.
//
// # Compatibility notice
//
// This interface may be extended, even within minor versions, when doing so is
// required to add support for new DB dialects that differ from previously
// supported dialects in unexpected ways.
type Dialect interface {
	// Placeholder returns the placeholder for the i-th query argument.
	// Most dialects use "?", but e.g. PostgreSQL uses "$1", "$2" and so on.
	// The argument numbers from 0 like a slice index.
	Placeholder(i int) string

	// QuoteIdentifier wraps the name of a column or table in quotes,
	// in order to avoid the name from being interpreted as a keyword.
	QuoteIdentifier(name string) string

	// UsesLastInsertID returns whether values for auto-generated columns are
	// collected from LastInsertID(). If false, the INSERT query must instead
	// yield a result row containing the values.
	UsesLastInsertID() bool

	// InsertSuffixForAutoColumns is appended to `INSERT (...) VALUES (...)`
	// statements to collect values for auto-filled columns.
	//
	// If UsesLastInsertID is true, this is usually not needed and the empty
	// string can be returned.
	InsertSuffixForAutoColumns(columns []string) string

	// UpsertClause generates an "ON CONFLICT" or similar clause
	// that can be appended to an INSERT query to make it fall back to
	// behave like UPDATE if a record with the same primary key already exists.
	// This is only used for record types that have a primary key.
	UpsertClause(pkColumns, otherColumns []string) string
}

// MysqlDialect is the dialect of MySQL and MariaDB databases.
func MysqlDialect() Dialect {
	return mysqlDialect{}
}

type mysqlDialect struct{}

func (mysqlDialect) Placeholder(_ int) string                           { return "?" }
func (mysqlDialect) QuoteIdentifier(name string) string                 { return "`" + name + "`" }
func (mysqlDialect) UsesLastInsertID() bool                             { return true }
func (mysqlDialect) InsertSuffixForAutoColumns(columns []string) string { return "" }

func (d mysqlDialect) UpsertClause(pkColumns, otherColumns []string) string {
	clauses := make([]string, max(1, len(otherColumns)))
	if len(otherColumns) == 0 {
		// we need at least one UPDATE clause; if there are no non-PK columns,
		// we can just use one of the PK columns, updating those is a safe no-op
		clauses[0] = fmt.Sprintf(`%[1]s = VALUES(%[1]s)`, d.QuoteIdentifier(pkColumns[0]))
	} else {
		for idx, name := range otherColumns {
			clauses[idx] = fmt.Sprintf(`%[1]s = VALUES(%[1]s)`, d.QuoteIdentifier(name))
		}
	}
	return ` ON DUPLICATE KEY UPDATE ` + strings.Join(clauses, ", ")
}

// PostgresDialect is the dialect of PostgreSQL databases.
func PostgresDialect() Dialect {
	return postgresDialect{}
}

type postgresDialect struct{}

func (postgresDialect) Placeholder(i int) string           { return "$" + strconv.Itoa(i+1) }
func (postgresDialect) QuoteIdentifier(name string) string { return `"` + name + `"` }
func (postgresDialect) UsesLastInsertID() bool             { return false }

func (d postgresDialect) InsertSuffixForAutoColumns(columns []string) string {
	quotedColumns := make([]string, len(columns))
	for idx, name := range columns {
		quotedColumns[idx] = d.QuoteIdentifier(name)
	}
	return ` RETURNING ` + strings.Join(quotedColumns, ", ")
}

func (d postgresDialect) UpsertClause(pkColumns, otherColumns []string) string {
	quotedPkColumns := make([]string, len(pkColumns))
	for idx, name := range pkColumns {
		quotedPkColumns[idx] = d.QuoteIdentifier(name)
	}
	clauses := make([]string, len(otherColumns))
	for idx, name := range otherColumns {
		clauses[idx] = fmt.Sprintf(`%[1]s = EXCLUDED.%[1]s`, d.QuoteIdentifier(name))
	}
	if len(otherColumns) == 0 {
		return fmt.Sprintf(` ON CONFLICT (%s) DO NOTHING`, strings.Join(quotedPkColumns, ", "))
	} else {
		return fmt.Sprintf(` ON CONFLICT (%s) DO UPDATE SET %s`,
			strings.Join(quotedPkColumns, ", "), strings.Join(clauses, ", "))
	}
}

// SqliteDialect is the dialect of SQLite databases.
func SqliteDialect() Dialect {
	return sqliteDialect{}
}

type sqliteDialect struct{}

func (sqliteDialect) Placeholder(_ int) string                           { return "?" }
func (sqliteDialect) QuoteIdentifier(name string) string                 { return `"` + name + `"` }
func (sqliteDialect) UsesLastInsertID() bool                             { return true }
func (sqliteDialect) InsertSuffixForAutoColumns(columns []string) string { return "" }
func (sqliteDialect) UpsertClause(pkColumns, otherColumns []string) string {
	return postgresDialect{}.UpsertClause(pkColumns, otherColumns)
}
