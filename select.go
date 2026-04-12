// SPDX-FileCopyrightText: 2026 Stefan Majewsky <majewsky@gmx.net>
// SPDX-License-Identifier: Apache-2.0

package oblast

import (
	"database/sql"
	"fmt"
	"reflect"

	"go.xyrillian.de/oblast/internal"
)

// Select executes the provided SQL query and fills an instance of the record type R for each row in the result set,
// according to the column names reported by the database as part of the result set.
//
// An error is returned if any column name in the result set does not correspond to an addressable field in R.
func (s Store[R]) Select(db Handle, query string, args ...any) (result []R, returnedError error) {
	// NOTE: This function body should be as short as possible to reduce the binary size after monomorphization.
	//       Any expression that does not depend on type R should be factored out into a reusable function.

	rows, indexes, err := startQuery(db, s.plan, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() {
		returnedError = mergeRowsCloseError(returnedError, rows.Close())
	}()

	slots := make([]any, len(indexes))
	for rows.Next() {
		var target R
		err = collectRow(rows, reflect.ValueOf(&target).Elem(), slots, indexes)
		if err != nil {
			return nil, err
		}
		result = append(result, target)
	}

	return result, nil
}

func startQuery(db Handle, plan internal.Plan, query string, args ...any) (rows *sql.Rows, indexes [][]int, err error) {
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

func collectRow(rows *sql.Rows, v reflect.Value, slots []any, indexes [][]int) error {
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

// SelectOne executes the provided SQL query and fills an instance of the record type R if there is exactly one row in the result set,
// according to the column names reported by the database as part of the result set.
//
// If there are no rows in the result set, [sql.ErrNoRows] is returned.
// If there are multiple rows in the result set, [ErrMultipleRows] is returned.
//
// Warning: Because of limitations in the interface of database/sql, this function is built on [Store.Select] and cannot be any faster than it.
// For maximum performance, use [Store.SelectOneWhere] which avoids the overhead of potentially having to read multiple rows.
func (s Store[R]) SelectOne(db Handle, query string, args ...any) (result R, err error) {
	// NOTE: This function body should be as short as possible to reduce the binary size after monomorphization.
	//       Any expression that does not depend on type R should be factored out into a reusable function.
	var results []R
	results, err = s.Select(db, query, args...)
	if err == nil {
		switch len(results) {
		case 0:
			err = sql.ErrNoRows
		case 1:
			result = results[0]
		default:
			err = ErrMultipleRows
		}
	}
	return
}
