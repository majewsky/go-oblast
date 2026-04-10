// SPDX-FileCopyrightText: 2026 Stefan Majewsky <majewsky@gmx.net>
// SPDX-License-Identifier: Apache-2.0

package internal

import (
	"fmt"
	"reflect"
	"slices"
	"strings"

	"go.xyrillian.de/oblast/info"
)

// Plan holds all information that we can derive from reflecting on a given type.
// The queries held within are only valid within the context of a given SQL dialect.
type Plan struct {
	// Information extracted from applicable marker types (if any).
	TableName         string
	PrimaryKeyColumns []string

	// Argument for reflect.Value.FieldByIndex() for each column name.
	IndexByColumnName map[string][]int
	// Which columns will be filled automatically by the DB during insert.
	// This corresponds to having a tag like `db:"foo,auto"`.
	// In DB dialects that use LastInsertID(), this list may have at most one element.
	AutoColumns []string

	// Prepared queries (or empty strings if the respective query types are not
	// supported for lack of the respective markers).
	InsertQuery string
	UpdateQuery string
	DeleteQuery string

	// Arguments for reflect.Value.FieldByIndex() in the required order for p.InsertQuery.
	InsertFieldOrder [][]int
}

var (
	tableNameMarkerType  = reflect.TypeFor[info.TableNameIs]()
	primaryKeyMarkerType = reflect.TypeFor[info.PrimaryKeyIs]()
)

func BuildPlan(t reflect.Type, dialect Dialect) (Plan, error) {
	if t.Kind() != reflect.Struct {
		return Plan{}, fmt.Errorf("expected record type to be a struct, but got kind %s (full type: %s.%s)",
			t.Kind(), t.PkgPath(), t.Name())
	}

	var p = Plan{
		IndexByColumnName: make(map[string][]int),
	}

	// discover addressable fields in this type,
	// collect information from markers and tags
	for _, index := range getAllAddressableFieldIndexes(t) {
		field := t.FieldByIndex(index)
		fullTag := strings.TrimSpace(field.Tag.Get("db"))
		if fullTag == "" || fullTag == "-" {
			continue
		}
		tags := strings.Split(fullTag, ",")

		switch field.Type {
		case tableNameMarkerType:
			// only consider this marker when directly on `t` itself, not within embedded fields
			if len(index) == 1 {
				if len(tags) > 1 {
					return Plan{}, fmt.Errorf("invalid table name %q (may not contain commas)", fullTag)
				}
				p.TableName = tags[0]
			}
		case primaryKeyMarkerType:
			// only consider this marker when directly on `t` itself, not within embedded fields
			if len(index) == 1 {
				p.PrimaryKeyColumns = tags
			}
		default:
			columnName, extraTags := tags[0], tags[1:]
			if otherIndex := p.IndexByColumnName[columnName]; otherIndex != nil {
				return Plan{}, fmt.Errorf(
					"duplicate tag `db:%q` on field index %v, but also on field index %v",
					columnName, otherIndex, index,
				)
			}
			p.IndexByColumnName[columnName] = index

			for _, tag := range extraTags {
				switch tag {
				case "auto":
					p.AutoColumns = append(p.AutoColumns, columnName)
				default:
					return Plan{}, fmt.Errorf("unknown tag `db:%q` on field index %v", ","+tag, index)
				}
			}
		}
	}

	// validation: oblast.PrimaryKeyInfo must refer to columns that exist
	for _, columnName := range p.PrimaryKeyColumns {
		_, ok := p.IndexByColumnName[columnName]
		if !ok {
			return Plan{}, fmt.Errorf("PrimaryKeyInfo refers to column %[1]q, but no field has tag `db:%[1]q`", columnName)
		}
	}

	// validation: LastInsertID() only works if at most one column is auto-filled
	if dialect.UsesLastInsertID() && len(p.AutoColumns) > 1 {
		return Plan{}, fmt.Errorf(
			"multiple columns are marked as auto-filled (%s), but this SQL dialect only supports at most one per table",
			strings.Join(p.AutoColumns, ", "),
		)
	}

	// TODO: build INSERT query if possible
	// TODO: build UPDATE query if possible
	// TODO: build DELETE query if possible

	return p, nil
}

// WARNING: Panics if t.Kind() != reflect.Struct.
func getAllAddressableFieldIndexes(t reflect.Type) (result [][]int) {
	for field := range t.Fields() {
		// recurse into embedded fields
		if field.Anonymous && field.Type.Kind() == reflect.Struct {
			for _, subindex := range getAllAddressableFieldIndexes(field.Type) {
				result = append(result, append(slices.Clone(field.Index), subindex...))
			}
		}

		// only fields are addressable (otherwise reflect.Value.Interface() on the field would panic)
		if field.PkgPath == "" {
			result = append(result, field.Index)
		}
	}
	return result
}
