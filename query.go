// SPDX-FileCopyrightText: 2026 Stefan Majewsky <majewsky@gmx.net>
// SPDX-License-Identifier: Apache-2.0

package oblast

import (
	"database/sql"
	"fmt"
	"reflect"

	"go.xyrillian.de/oblast/internal"
)

func Select[T any](db *DB, query string, args ...any) (result []T, returnedError error) {
	// NOTE: This function body should be as short as possible to reduce the binary size after monomorphization.
	//       Any expression that does not depend on type T should be factored out into a reusable function.

	plan, err := db.getPlan(reflect.TypeFor[T]())
	if err != nil {
		return nil, err
	}
	rows, indexes, err := db.startQuery(plan, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() {
		returnedError = mergeRowsCloseError(returnedError, rows.Close())
	}()

	slots := make([]any, len(indexes))
	for rows.Next() {
		var target T
		err = db.collectRow(rows, reflect.ValueOf(&target).Elem(), slots, indexes)
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
		return nil, nil, fmt.Errorf("during Query(): %w", err)
	}
	defer func() {
		if err != nil {
			closeErr := rows.Close()
			if closeErr != nil {
				err = fmt.Errorf("%w (additional error during rows.Close(): %s)", err, closeErr.Error())
			}
		}
	}()

	columnNames, err := rows.Columns()
	if err != nil {
		return nil, nil, fmt.Errorf("during rows.Columns(): %w", err)
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

func (db *DB) collectRow(rows *sql.Rows, v reflect.Value, slots []any, indexes [][]int) error {
	for idx, index := range indexes {
		slots[idx] = v.FieldByIndex(index).Addr().Interface()
	}
	err := rows.Scan(slots...)
	if err != nil {
		return fmt.Errorf("during rows.Scan(): %w", err)
	}
	return nil
}

func mergeRowsCloseError(err, closeErr error) error {
	switch {
	case closeErr == nil:
		return err
	case err == nil:
		return fmt.Errorf("during rows.Close(): %w", closeErr)
	default:
		return fmt.Errorf("%w (additional error during rows.Close(): %s)", err, closeErr.Error())
	}
}
