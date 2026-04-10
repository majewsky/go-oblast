// SPDX-FileCopyrightText: 2026 Stefan Majewsky <majewsky@gmx.net>
// SPDX-License-Identifier: Apache-2.0

package oblast

import (
	"context"
	"database/sql"
	"reflect"
	"sync"
)

// DB wraps an [sql.DB] instance for use with Oblast's query interface.
type DB struct {
	*sql.DB
	dialect   Dialect
	plans     map[reflect.Type]plan
	planMutex sync.Mutex
}

func NewDB(db *sql.DB, dialect Dialect) *DB {
	return &DB{
		DB:      db,
		dialect: dialect,
		plans:   make(map[reflect.Type]plan),
	}
}

func Keks[T IsTable](ctx context.Context, db *DB) error {
	_, err := db.getPlan(reflect.TypeFor[T]())
	return err
}

// TODO: Begin() -> custom Tx type
