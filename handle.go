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

// SqlHandle wraps types like [*sql.DB] or [*sql.Tx] into a [Handle] that can be used with Oblast.
type SqlHandle[T SqlExecutor] struct {
	// The original database or transaction handle.
	// It is safe to read this field to execute operations that Oblast does not handle (e.g. transactions, savepoints or OLAP queries).
	Base T

	// If this is not true, then any methods on this type will panic.
	// This is just to enforce that the handle is constructed with Wrap(), thus guaranteeing future compatibility if actual important private struct fields are added later.
	ok bool
}

// Wrap converts an [*sql.DB] or [*sql.Tx] into a [Handle] that can be used with Oblast functions.
func Wrap[T SqlExecutor](dbOrTx T) SqlHandle[T] {
	return SqlHandle[T]{Base: dbOrTx, ok: true}
}

// SqlExecutor is an interface covered by both [*sql.DB] and [*sql.Tx].
// It appears in the signature of function [Wrap].
type SqlExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// static assertion that the respective types implement the interface
var (
	_ SqlExecutor = &sql.DB{}
	_ SqlExecutor = &sql.Tx{}
)

// OblastPrepare implements the [Handle] interface.
func (h SqlHandle[T]) OblastPrepare(ctx context.Context, query string, repeated bool) (handle.Statement, error) {
	if !h.ok {
		panic("SqlHandle was not constructed through oblast.Wrap()!")
	}
	if !repeated {
		return wrappedStatement{h.Base, query, nil}, nil
	}
	stmt, err := h.Base.PrepareContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("during Prepare(): %w", err)
	}
	return wrappedStatement{h.Base, query, stmt}, nil
}

// OblastQuery implements the [Handle] interface.
func (h SqlHandle[T]) OblastQuery(ctx context.Context, query string, args []any) (handle.Rows, error) {
	if !h.ok {
		panic("SqlHandle was not constructed through oblast.Wrap()!")
	}
	return h.Base.QueryContext(ctx, query, args...) //nolint:rowserrcheck // the caller does the check
}

type wrappedStatement struct {
	db    SqlExecutor
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
