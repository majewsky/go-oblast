// SPDX-FileCopyrightText: 2026 Stefan Majewsky <majewsky@gmx.net>
// SPDX-License-Identifier: Apache-2.0

package oblast

import (
	"fmt"
	"reflect"
	"strings"
)

// MissingRecordError is returned by [Store.Update] if one of the rows to be updated does not exist in the DB.
type MissingRecordError[R any] struct {
	// The record that was provided to [Store.Update],
	// but for which no row with the same primary key values could be located.
	Record R
	plan   plan
}

// Error implements the builtin/error interface.
func (e MissingRecordError[R]) Error() string {
	keyDescs := make([]string, len(e.plan.PrimaryKeyColumnNames))
	v := reflect.ValueOf(e.Record)
	for idx, columnName := range e.plan.PrimaryKeyColumnNames {
		keyDescs[idx] = fmt.Sprintf("%s = %#v", columnName, v.FieldByIndex(e.plan.IndexByColumnName[columnName]))
	}
	return "could not UPDATE record that does not exist in the database: " + strings.Join(keyDescs, ", ")
}

// An error type that optionally contains either one of the following or both:
// - a core error from an IO operation (e.g. a database read)
// - an auxiliary error from closing or otherwise cleaning up the respective IO handle
type ioError struct {
	MainError        error
	CleanupError     error
	CleanupOperation string
}

func newIOError(err error, cleanupOperation string, cleanupErr error) error {
	if err == nil && cleanupErr == nil {
		return nil
	}
	return ioError{err, cleanupErr, cleanupOperation}
}

// Error implements the builtin/error interface.
func (e ioError) Error() string {
	switch {
	case e.CleanupError == nil:
		return e.MainError.Error()
	case e.MainError == nil:
		return fmt.Sprintf("during %s(): %s", e.CleanupOperation, e.CleanupError.Error())
	default:
		return fmt.Sprintf("%s (additional error during %s(): %s)", e.MainError.Error(), e.CleanupOperation, e.CleanupError.Error())
	}
}

// Unwrap implements the interface implied by the documentation of package errors.
func (e ioError) Unwrap() []error {
	result := make([]error, 0, 2)
	if e.MainError != nil {
		result = append(result, e.MainError)
	}
	if e.CleanupError != nil {
		result = append(result, e.CleanupError)
	}
	return result
}
