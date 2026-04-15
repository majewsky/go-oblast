// SPDX-FileCopyrightText: 2026 Stefan Majewsky <majewsky@gmx.net>
// SPDX-License-Identifier: Apache-2.0

package assert

import (
	"reflect"
	"testing"
)

// Equal is a test assertion.
func Equal[V comparable](t testing.TB, actual, expected V) {
	t.Helper()
	if actual != expected {
		t.Errorf("expected %#v, but got %#v", expected, actual)
	}
}

// DeepEqual is a test assertion.
func DeepEqual[V any](t testing.TB, actual, expected V) {
	t.Helper()
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("expected %#v, but got %#v", expected, actual)
	}
}

// SliceEqual is a test assertion.
func SliceEqual[V comparable](t testing.TB, actual []V, expected ...V) {
	t.Helper()
	if len(actual) != len(expected) {
		t.Errorf("length mismatch: expected %#v, but got %#v", expected, actual)
	}
	for idx := range actual {
		if actual[idx] != expected[idx] {
			t.Errorf("element %d: expected %#v, but got %#v", idx, expected[idx], actual[idx])
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
			t.Errorf("element %d: expected %#v, but got %#v", idx, expected[idx], actual[idx])
		}
	}
}
