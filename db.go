// SPDX-FileCopyrightText: 2026 Stefan Majewsky <majewsky@gmx.net>
// SPDX-License-Identifier: Apache-2.0

package oblast

import (
	"context"
	"database/sql"
	"fmt"
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

func Select[T any](ctx context.Context, db *DB, query string, args ...any) ([]T, error) {
	// TODO: minimize function body to avoid binary size blowup from monomorphization
	// TODO: catch error from rows.Close(), if any
	// TODO: add context to errors

	plan, err := db.getPlan(reflect.TypeFor[T]())
	if err != nil {
		return nil, err
	}
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columnNames, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	indexes := make([][]int, len(columnNames))
	for idx, columnName := range columnNames {
		var ok bool
		indexes[idx], ok = plan.IndexByColumnName[columnName]
		if !ok {
			var zero T
			return nil, fmt.Errorf("result has column %q in position %d, but no field in %T has `db:%[1]q`",
				columnName, idx, zero)
		}
	}

	var result []T
	slots := make([]any, len(indexes))
	for rows.Next() {
		var target T
		rvalue := reflect.ValueOf(&target).Elem()
		for idx, index := range indexes {
			slots[idx] = rvalue.FieldByIndex(index).Addr().Interface()
		}
		err := rows.Scan(slots...)
		if err != nil {
			return nil, err
		}
		result = append(result, target)
	}

	return result, nil
}
