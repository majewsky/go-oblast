// SPDX-FileCopyrightText: 2026 Stefan Majewsky <majewsky@gmx.net>
// SPDX-License-Identifier: Apache-2.0

package oblast_test

import (
	"testing"
	"time"

	"go.xyrillian.de/oblast"
)

func TestPlan(t *testing.T) {
	type Log struct {
		oblast.TableInfo      `db:"log_entries"`
		oblast.PrimaryKeyInfo `db:"id"`
		ID                    int64     `db:"id,auto"`
		CreatedAt             time.Time `db:"created_at"`
		Message               string    `db:"message"`
		private1              bool      `db:"private1"`
	}

	type record struct {
		Log
		Keks     bool `db:"keks"`
		private2 bool `db:"private2"`
	}

	db := oblast.NewDB(nil, oblast.PostgresDialect())
	err := oblast.Keks[record](t.Context(), db)
	if err != nil {
		t.Error(err)
	}
	err = oblast.Keks[Log](t.Context(), db)
	if err != nil {
		t.Error(err)
	}
}
