// SPDX-FileCopyrightText: 2026 Stefan Majewsky <majewsky@gmx.net>
// SPDX-License-Identifier: Apache-2.0

package oblast

import (
	"database/sql"
	"fmt"
	"reflect"

	"go.xyrillian.de/oblast/internal"
)

func Select[T any](db *DB, query string, args ...any) ([]T, error) {
	// TODO: minimize function body to avoid binary size blowup from monomorphization
	// TODO: catch error from rows.Close(), if any
	// TODO: add context to errors

	plan, err := db.getPlan(reflect.TypeFor[T]())
	if err != nil {
		return nil, err
	}
	rows, indexes, err := db.startQuery(plan, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

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

func (db *DB) startQuery(plan internal.Plan, query string, args ...any) (rows *sql.Rows, indexes [][]int, err error) {
	rows, err = db.Query(query, args...)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if err != nil {
			closeErr := rows.Close()
			if closeErr != nil {
				err = fmt.Errorf("%w (additional error during rows.Close: %s)", err, closeErr.Error())
			}
		}
	}()

	columnNames, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}
	indexes = make([][]int, len(columnNames))
	for idx, columnName := range columnNames {
		var ok bool
		indexes[idx], ok = plan.IndexByColumnName[columnName]
		if !ok {
			return nil, nil, fmt.Errorf(
				"result has column %q in position %d, but no field in record type has `db:%[1]q`",
				columnName, idx,
			)
		}
	}

	return rows, indexes, nil
}
