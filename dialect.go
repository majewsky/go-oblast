// SPDX-FileCopyrightText: 2026 Stefan Majewsky <majewsky@gmx.net>
// SPDX-License-Identifier: Apache-2.0

package oblast

import "go.xyrillian.de/oblast/internal"

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
}

// PostgresDialect is the dialect of PostgreSQL databases.
func PostgresDialect() Dialect {
	return internal.PostgresDialect{}
}

// SqliteDialect is the dialect of SQLite databases.
func SqliteDialect() Dialect {
	return internal.SqliteDialect{}
}
