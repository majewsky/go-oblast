// SPDX-FileCopyrightText: 2026 Stefan Majewsky <majewsky@gmx.net>
// SPDX-License-Identifier: Apache-2.0

package oblast_pgx

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.xyrillian.de/oblast/handle"
)

type Handle interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

var (
	_ Handle = &pgx.Conn{}
	_ Handle = pgx.Tx(nil)
)

// TODO: offer wrapping for pgxpool.Pool and pgxpool.Conn?
func Wrap(h Handle) handle.Handle {
	switch h := h.(type) {
	case *pgx.Conn:
		return wrappedHandle{h}
	case pgx.Tx:
		return wrappedHandle{h}
	default:
		panic(fmt.Sprintf("unexpected type: %#v", h))
	}
}

var preparedStatementId atomic.Uint64

type wrappedHandle struct {
	inner Handle
}

// Prepare implements the [handle.Handle] interface.
func (h wrappedHandle) Prepare(ctx context.Context, query string, repeated bool) (handle.Statement, error) {
	if !repeated {
		return wrappedUnpreparedStatement{query, h.inner}, nil
	}

	name := "oblast_pgx_" + strconv.FormatUint(preparedStatementId.Add(1), 10)
	switch inner := h.inner.(type) {
	case *pgx.Conn:
		stmt, err := inner.Prepare(ctx, name, query)
		return wrappedPreparedStatement{ctx, stmt, h.inner}, err
	case pgx.Tx:
		stmt, err := inner.Conn().Prepare(ctx, name, query)
		return wrappedPreparedStatement{ctx, stmt, h.inner}, err
	default:
		panic("unreachable") // because of the check in func Wrap()
	}
}

// Releases a prepared statement.
func deallocate(ctx context.Context, h Handle, stmt *pgconn.StatementDescription) error {
	switch h := h.(type) {
	case *pgx.Conn:
		return h.Deallocate(ctx, stmt.Name)
	case pgx.Tx:
		return h.Conn().Deallocate(ctx, stmt.Name)
	default:
		panic("unreachable") // because of the check in func Wrap()
	}
}

// Query implements the [handle.Handle] interface.
func (h wrappedHandle) Query(ctx context.Context, query string, args []any) (handle.Rows, error) {
	rows, err := h.inner.Query(ctx, query, args...)
	return wrappedRows{rows}, err
}
