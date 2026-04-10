// SPDX-FileCopyrightText: 2026 Stefan Majewsky <majewsky@gmx.net>
// SPDX-License-Identifier: Apache-2.0

package oblast

import (
	"database/sql"
	"reflect"
	"sync"

	"go.xyrillian.de/oblast/internal"
)

// DB wraps an [sql.DB] instance for use with Oblast's query interface.
type DB struct {
	*sql.DB
	dialect   Dialect
	plans     map[reflect.Type]internal.Plan
	planMutex sync.Mutex
}

func NewDB(db *sql.DB, dialect Dialect) *DB {
	return &DB{
		DB:      db,
		dialect: dialect,
		plans:   make(map[reflect.Type]internal.Plan),
	}
}

func (d *DB) getPlan(t reflect.Type) (internal.Plan, error) {
	d.planMutex.Lock()
	defer d.planMutex.Unlock()
	p, ok := d.plans[t]
	if ok {
		return p, nil
	}
	p, err := internal.BuildPlan(t, d.dialect)
	if err == nil {
		d.plans[t] = p
	}
	return p, err
}

// TODO: Begin() -> custom Tx type; add interface to allow Select() et all to take either *DB or *Tx
