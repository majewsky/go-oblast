// SPDX-FileCopyrightText: 2026 Stefan Majewsky <majewsky@gmx.net>
// SPDX-License-Identifier: Apache-2.0

package internal

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
)

// Plan holds all information that we can derive from reflecting on a given type.
// The queries held within are only valid within the context of a given SQL dialect.
type Plan struct {
	TableName             string   // from info.TableNameIs marker (if any)
	AllColumnNames        []string // in order of struct fields
	PrimaryKeyColumnNames []string // from info.PrimaryKeyIs marker (if any)
	AutoColumnNames       []string // subset of AllColumnNames where field has `,auto` marker

	// Argument for reflect.Value.FieldByIndex() for each column name.
	IndexByColumnName map[string][]int

	// Planned queries.
	Insert PlannedQuery
	Update PlannedQuery
	Delete PlannedQuery
}

// PlannedQuery appears in type Plan.
type PlannedQuery struct {
	// Empty if the respective query type is not supported by this Plan
	// for lack of the required marker types.
	Query string
	// Arguments for reflect.Value.FieldByIndex() in the correct order
	// for the query arguments of the above query.
	ArgumentIndexes [][]int
}

// PlanOpts holds additional arguments to BuildPlan().
type PlanOpts struct {
	TableName             string
	PrimaryKeyColumnNames []string
}

// BuildPlan creates a new plan for the given struct type.
func BuildPlan(t reflect.Type, dialect Dialect, opts PlanOpts) (Plan, error) {
	p, err := buildPlan(t, dialect, opts)
	if err != nil {
		return Plan{}, fmt.Errorf("cannot use type %s.%s for queries: %w", t.PkgPath(), t.Name(), err)
	}
	return p, nil
}

func buildPlan(t reflect.Type, dialect Dialect, opts PlanOpts) (Plan, error) {
	if t.Kind() != reflect.Struct {
		return Plan{}, fmt.Errorf("expected struct type, but got kind %s", t.Kind().String())
	}

	var p = Plan{
		TableName:             opts.TableName,
		PrimaryKeyColumnNames: opts.PrimaryKeyColumnNames,
		IndexByColumnName:     make(map[string][]int),
	}

	// discover addressable fields in this type,
	// collect information from markers and tags
	for _, field := range reflect.VisibleFields(t) {
		tags := strings.Split(strings.TrimSpace(field.Tag.Get("db")), ",")

		switch {
		case field.PkgPath != "":
			// ignore unexported fields (otherwise reflect.Value.Interface() on the field would panic)
			continue
		case field.Anonymous && field.Type.Kind() == reflect.Struct:
			// for embedded struct fields, only consider their members, not the type itself, as a potential column
			continue
		default:
			columnName, extraTags := tags[0], tags[1:]
			if columnName == "-" {
				continue
			}
			if columnName == "" {
				columnName = field.Name
			}
			if otherIndex := p.IndexByColumnName[columnName]; otherIndex != nil {
				return Plan{}, fmt.Errorf(
					"duplicate tag `db:%q` on field index %v, but also on field index %v",
					columnName, otherIndex, field.Index,
				)
			}
			p.IndexByColumnName[columnName] = field.Index
			p.AllColumnNames = append(p.AllColumnNames, columnName)

			for _, tag := range extraTags {
				switch tag {
				case "auto":
					p.AutoColumnNames = append(p.AutoColumnNames, columnName)
				default:
					return Plan{}, fmt.Errorf("unknown tag `db:%q` on field index %v", ","+tag, field.Index)
				}
			}
		}
	}

	// validation: defining a primary key only makes sense for records that map onto a single table
	if len(p.PrimaryKeyColumnNames) > 0 && p.TableName == "" {
		return Plan{}, errors.New("cannot declare a primary key without also providing the TableNameIs option")
	}

	// validation: oblast.PrimaryKeyInfo must refer to columns that exist
	for _, columnName := range p.PrimaryKeyColumnNames {
		_, ok := p.IndexByColumnName[columnName]
		if !ok {
			return Plan{}, fmt.Errorf("no field has tag `db:%q`, but a field of this name was declared in the primary key", columnName)
		}
	}

	// validation: LastInsertID() only works if at most one column is auto-filled
	if dialect.UsesLastInsertID() && len(p.AutoColumnNames) > 1 {
		return Plan{}, fmt.Errorf(
			"multiple columns are marked as auto-filled (%s), but this SQL dialect only supports at most one per table",
			strings.Join(p.AutoColumnNames, ", "),
		)
	}

	// prepare query strings
	p.Insert = p.buildInsertQueryIfPossible(dialect)
	p.Update = p.buildUpdateQueryIfPossible(dialect)
	p.Delete = p.buildDeleteQueryIfPossible(dialect)

	return p, nil
}

func (p Plan) getNonAutoColumnNames() []string {
	result := make([]string, 0, len(p.AllColumnNames)-len(p.AutoColumnNames))
	for _, columnName := range p.AllColumnNames {
		if !slices.Contains(p.AutoColumnNames, columnName) {
			result = append(result, columnName)
		}
	}
	return result
}

func (p Plan) getNonPrimaryKeyColumnNames() []string {
	result := make([]string, 0, len(p.AllColumnNames)-len(p.PrimaryKeyColumnNames))
	for _, columnName := range p.AllColumnNames {
		if !slices.Contains(p.PrimaryKeyColumnNames, columnName) {
			result = append(result, columnName)
		}
	}
	return result
}

func (p Plan) buildInsertQueryIfPossible(dialect Dialect) PlannedQuery {
	if p.TableName == "" || len(p.AllColumnNames) == 0 {
		return PlannedQuery{Query: ""}
	}
	nonAutoColumnNames := p.getNonAutoColumnNames()
	if len(nonAutoColumnNames) == 0 {
		return PlannedQuery{Query: ""}
	}

	var (
		argumentIndexes    = make([][]int, len(nonAutoColumnNames))
		quotedColumnNames  = make([]string, len(nonAutoColumnNames))
		quotedPlaceholders = make([]string, len(nonAutoColumnNames))
	)
	for idx, columnName := range nonAutoColumnNames {
		argumentIndexes[idx] = p.IndexByColumnName[columnName]
		quotedColumnNames[idx] = dialect.QuoteIdentifier(columnName)
		quotedPlaceholders[idx] = dialect.Placeholder(idx)
	}

	query := fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES (%s)`,
		dialect.QuoteIdentifier(p.TableName),
		strings.Join(quotedColumnNames, ", "),
		strings.Join(quotedPlaceholders, ", "),
	)
	if len(p.AutoColumnNames) > 0 {
		query += dialect.InsertSuffixForAutoColumns(p.AutoColumnNames)
	}
	return PlannedQuery{query, argumentIndexes}
}

func (p Plan) buildUpdateQueryIfPossible(dialect Dialect) PlannedQuery {
	if p.TableName == "" || len(p.PrimaryKeyColumnNames) == 0 {
		return PlannedQuery{Query: ""}
	}
	nonPrimaryKeyColumnNames := p.getNonPrimaryKeyColumnNames()
	if len(nonPrimaryKeyColumnNames) == 0 {
		return PlannedQuery{Query: ""}
	}

	var (
		setArgumentIndexes = make([][]int, len(nonPrimaryKeyColumnNames))
		setClauses         = make([]string, len(nonPrimaryKeyColumnNames))
	)
	for idx, columnName := range nonPrimaryKeyColumnNames {
		setArgumentIndexes[idx] = p.IndexByColumnName[columnName]
		setClauses[idx] = fmt.Sprintf("%s = %s", dialect.QuoteIdentifier(columnName), dialect.Placeholder(idx))
	}

	var (
		whereArgumentIndexes = make([][]int, len(p.PrimaryKeyColumnNames))
		whereClauses         = make([]string, len(p.PrimaryKeyColumnNames))
	)
	for idx, columnName := range p.PrimaryKeyColumnNames {
		whereArgumentIndexes[idx] = p.IndexByColumnName[columnName]
		whereClauses[idx] = fmt.Sprintf("%s = %s", dialect.QuoteIdentifier(columnName), dialect.Placeholder(idx+len(setClauses)))
	}

	query := fmt.Sprintf(
		`UPDATE %s SET %s WHERE %s`,
		dialect.QuoteIdentifier(p.TableName),
		strings.Join(setClauses, ", "),
		strings.Join(whereClauses, " AND "),
	)
	return PlannedQuery{query, slices.Concat(setArgumentIndexes, whereArgumentIndexes)}
}

func (p Plan) buildDeleteQueryIfPossible(dialect Dialect) PlannedQuery {
	if p.TableName == "" || len(p.PrimaryKeyColumnNames) == 0 {
		return PlannedQuery{Query: ""}
	}

	var (
		argumentIndexes = make([][]int, len(p.PrimaryKeyColumnNames))
		clauses         = make([]string, len(p.PrimaryKeyColumnNames))
	)
	for idx, columnName := range p.PrimaryKeyColumnNames {
		argumentIndexes[idx] = p.IndexByColumnName[columnName]
		clauses[idx] = fmt.Sprintf("%s = %s", dialect.QuoteIdentifier(columnName), dialect.Placeholder(idx))
	}

	query := fmt.Sprintf(
		`DELETE FROM %s WHERE %s`,
		dialect.QuoteIdentifier(p.TableName),
		strings.Join(clauses, " AND "),
	)
	return PlannedQuery{query, argumentIndexes}
}
