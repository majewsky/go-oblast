// SPDX-FileCopyrightText: 2026 Stefan Majewsky <majewsky@gmx.net>
// SPDX-License-Identifier: Apache-2.0

package assert

import (
	"cmp"
	"errors"
	"reflect"
	"testing"
)

// Equal is a test assertion.
func Equal[V comparable](t testing.TB, actual, expected V) {
	t.Helper()
	if actual != expected {
		t.Errorf("expected %#v", expected)
		t.Errorf(" but got %#v", actual)
	}
}

// DeepEqual is a test assertion.
func DeepEqual[V any](t testing.TB, actual, expected V) {
	t.Helper()
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("expected %#v", expected)
		t.Errorf(" but got %#v", actual)
	}
}

// ErrEqual is a test assertion.
func ErrEqual(t testing.TB, actual error, expected string) {
	t.Helper()
	Equal(t, cmp.Or(actual, errors.New("<success>")).Error(), expected)
}

// SliceEqual is a test assertion.
func SliceEqual[V comparable](t testing.TB, actual []V, expected ...V) {
	t.Helper()
	if len(actual) != len(expected) {
		t.Errorf("length mismatch: expected %#v, but got %#v", expected, actual)
	}
	for idx := range actual {
		if actual[idx] != expected[idx] {
			t.Errorf("element %d: expected %#v", idx, expected[idx])
			t.Errorf("element %d:  but got %#v", idx, actual[idx])
		}
	}
}

// SliceDeepEqual is a test assertion.
func SliceDeepEqual[V any](t testing.TB, actual []V, expected ...V) {
	t.Helper()
	if len(actual) != len(expected) {
		t.Errorf("length mismatch: expected %#v, but got %#v", expected, actual)
	}
	for idx := range actual {
		if !reflect.DeepEqual(actual[idx], expected[idx]) {
			t.Errorf("element %d: expected %#v", idx, expected[idx])
			t.Errorf("element %d:  but got %#v", idx, actual[idx])
		}
	}
}
