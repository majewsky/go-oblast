// SPDX-FileCopyrightText: 2026 Stefan Majewsky <majewsky@gmx.net>
// SPDX-License-Identifier: Apache-2.0

package oblast

import (
	"context"
	"database/sql"
	"fmt"

	"go.xyrillian.de/oblast/handle"
)

// Handle contains behavior that database handles must offer to Oblast.
// The standard-library types [*sql.DB] and [*sql.Tx] can satisfy this interface through the [Wrap] function.
// Custom implementations of this interface can be used to connect non-std database drivers to Oblast.
type Handle = handle.Handle

// StdHandle is an interface covered by both [*sql.DB] and [*sql.Tx].
// It appears in the signature of function [Wrap].
type StdHandle interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// static assertion that the respective types implement the interface
var (
	_ StdHandle = &sql.DB{}
	_ StdHandle = &sql.Tx{}
)

// Wrap converts an [*sql.DB] or [*sql.Tx] into a [Handle] that can be used with Oblast functions.
func Wrap(dbOrTx StdHandle) Handle {
	return wrappedHandle{dbOrTx}
}

type wrappedHandle struct {
	db StdHandle
}

// Prepare implements the [Handle] interface.
func (h wrappedHandle) Prepare(ctx context.Context, query string, repeated bool) (handle.Statement, error) {
	if !repeated {
		return wrappedStatement{h.db, query, nil}, nil
	}
	stmt, err := h.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("during Prepare(): %w", err)
	}
	return wrappedStatement{h.db, query, stmt}, nil
}

// Query implements the [Handle] interface.
func (h wrappedHandle) Query(ctx context.Context, query string, args []any) (handle.Rows, error) {
	return h.db.QueryContext(ctx, query, args...) //nolint:rowserrcheck // the caller does the check
}

type wrappedStatement struct {
	db    StdHandle
	query string
	stmt  *sql.Stmt // nil if repeated = false
}

// Close implements the [Statement] interface.
func (s wrappedStatement) Close() error {
	if s.stmt == nil {
		return nil
	}
	return s.stmt.Close()
}

// Exec implements the [Statement] interface.
func (s wrappedStatement) Exec(ctx context.Context, args []any) (sql.Result, error) {
	if s.stmt == nil {
		return s.db.ExecContext(ctx, s.query, args...)
	} else {
		return s.stmt.ExecContext(ctx, args...)
	}
}

// QueryRow implements the [Statement] interface.
func (s wrappedStatement) QueryRow(ctx context.Context, args, slots []any) error {
	if s.stmt == nil {
		return s.db.QueryRowContext(ctx, s.query, args...).Scan(slots...)
	} else {
		return s.stmt.QueryRowContext(ctx, args...).Scan(slots...)
	}
}
