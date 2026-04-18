// SPDX-FileCopyrightText: 2026 Stefan Majewsky <majewsky@gmx.net>
// SPDX-License-Identifier: Apache-2.0

package oblast

import (
	"errors"
	"testing"

	"go.xyrillian.de/oblast/internal/assert"
)

type fooError struct{}
type barError struct{}
type bazError struct{}

func (fooError) Error() string { return "foo" }
func (barError) Error() string { return "bar" }
func (bazError) Error() string { return "baz" }

func TestIOError(t *testing.T) {
	err := newIOError(nil, "File.Close", nil)
	assert.Equal(t, err == nil, true)

	err = newIOError(fooError{}, "File.Close", nil)
	assert.ErrEqual(t, err, "foo")
	assert.DeepEqual(t, err, error(fooError{})) // check for no wrapping in type ioError without cleanup error

	err = newIOError(nil, "File.Close", barError{})
	assert.ErrEqual(t, err, "during File.Close(): bar")
	assert.Equal(t, errors.Is(err, fooError{}), false)
	assert.Equal(t, errors.Is(err, barError{}), true)
	assert.Equal(t, errors.Is(err, bazError{}), false)

	err = newIOError(fooError{}, "File.Close", barError{})
	assert.ErrEqual(t, err, "foo (additional error during File.Close(): bar)")
	assert.Equal(t, errors.Is(err, fooError{}), true)
	assert.Equal(t, errors.Is(err, barError{}), true)
	assert.Equal(t, errors.Is(err, bazError{}), false)
}
