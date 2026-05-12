// SPDX-FileCopyrightText: 2026 Stefan Majewsky <majewsky@gmx.net>
// SPDX-License-Identifier: Apache-2.0

package oblast_pgx

import (
	"context"
	"database/sql"

	"github.com/jackc/pgx/v5/pgconn"
	"go.xyrillian.de/oblast/handle"
)

type wrappedPreparedStatement struct {
	ctx       context.Context
	statement *pgconn.StatementDescription
	handle    Handle
}

type wrappedUnpreparedStatement struct {
	query  string
	handle Handle
}

var (
	_ handle.Statement = wrappedPreparedStatement{}
	_ handle.Statement = wrappedUnpreparedStatement{}
)

// Close implements the [handle.Statement] interface.
func (s wrappedPreparedStatement) Close() error {
	return deallocate(s.ctx, s.handle, s.statement)
}

// Close implements the [handle.Statement] interface.
func (s wrappedUnpreparedStatement) Close() error {
	return nil
}

// Exec implements the [handle.Statement] interface.
func (s wrappedPreparedStatement) Exec(ctx context.Context, args []any) (sql.Result, error) {
	result, err := s.handle.Exec(ctx, s.statement.Name, args...)
	return wrappedResult{result}, err
}

// Exec implements the [handle.Statement] interface.
func (s wrappedUnpreparedStatement) Exec(ctx context.Context, args []any) (sql.Result, error) {
	result, err := s.handle.Exec(ctx, s.query, args...)
	return wrappedResult{result}, err
}

// QueryRow implements the [handle.Statement] interface.
func (s wrappedPreparedStatement) QueryRow(ctx context.Context, args, slots []any) error {
	return s.handle.QueryRow(ctx, s.statement.Name, args...).Scan(slots...)
}

// QueryRow implements the [handle.Statement] interface.
func (s wrappedUnpreparedStatement) QueryRow(ctx context.Context, args, slots []any) error {
	return s.handle.QueryRow(ctx, s.query, args...).Scan(slots...)
}
