// SPDX-FileCopyrightText: 2026 Stefan Majewsky <majewsky@gmx.net>
// SPDX-License-Identifier: Apache-2.0

package oblast

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
)

// plan holds all information that we can derive from reflecting on a given type.
// The queries held within are only valid within the context of a given SQL dialect.
type plan struct {
	TypeName              string   // for use in error messages
	TableName             string   // from info.TableNameIs marker (if any)
	AllColumnNames        []string // in order of struct fields
	PrimaryKeyColumnNames []string // from info.PrimaryKeyIs marker (if any)
	AutoColumnNames       []string // subset of AllColumnNames where field has `,auto` marker

	// Argument for reflect.Value.FieldByIndex() for each column name.
	IndexByColumnName map[string][]int

	// In dialects with UsesLastInsertID() == true, whether the ID column must be written with reflect.Value.SetInt() or reflect.Value.SetUint().
	FillIDWithSetUint bool
	FillIDWithSetInt  bool

	// Planned queries.
	Select plannedQuery // only `SELECT ... FROM ... WHERE `; user supplies the rest during Select{,One}Where()
	Insert plannedQuery
	Update plannedQuery
	Delete plannedQuery
}

// plannedQuery appears in type plan.
type plannedQuery struct {
	// Empty if the respective query type is not supported by this plan for lack of the required marker types.
	Query string
	// Arguments for reflect.Value.FieldByIndex() in the correct order for the query arguments of the above query.
	ArgumentIndexes [][]int
	// Arguments for reflect.Value.FieldByIndex() in the correct order for the Scan() arguments of the above query.
	ScanIndexes [][]int
}

// planOpts holds additional arguments to buildPlan().
type planOpts struct {
	TableName             string
	PrimaryKeyColumnNames []string
}

// buildPlan creates a new plan for the given struct type.
func buildPlan(t reflect.Type, dialect Dialect, opts planOpts) (plan, error) {
	if t.Kind() != reflect.Struct {
		return plan{}, fmt.Errorf("expected struct type, but got kind %s", t.Kind().String())
	}

	var p = plan{
		TypeName:              t.Name(),
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
				return plan{}, fmt.Errorf(
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
					return plan{}, fmt.Errorf("unknown tag `db:%q` on field index %v", ","+tag, field.Index)
				}
			}
		}
	}

	// validation: defining a primary key only makes sense for records that map onto a single table
	if len(p.PrimaryKeyColumnNames) > 0 && p.TableName == "" {
		return plan{}, errors.New("cannot declare a primary key without also providing the TableNameIs option")
	}

	// validation: oblast.PrimaryKeyInfo must refer to columns that exist
	for _, columnName := range p.PrimaryKeyColumnNames {
		_, ok := p.IndexByColumnName[columnName]
		if !ok {
			return plan{}, fmt.Errorf("no field has tag `db:%q`, but a field of this name was declared in the primary key", columnName)
		}
	}

	// validation: LastInsertID() only works if at most one column is auto-filled, and if that column holds an integer type
	if dialect.UsesLastInsertID() {
		switch len(p.AutoColumnNames) {
		case 0:
			// nothing to check
		case 1:
			columnName := p.AutoColumnNames[0]
			field := t.FieldByIndex(p.IndexByColumnName[columnName])
			switch field.Type.Kind() { //nolint:exhaustive // false positive
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				p.FillIDWithSetInt = true
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				p.FillIDWithSetUint = true
			default:
				return plan{}, fmt.Errorf(
					"column is marked as auto-filled (%s), but this SQL dialect only supports auto-filling struct fields with integer types",
					strings.Join(p.AutoColumnNames, ", "),
				)
			}
		default:
			return plan{}, fmt.Errorf(
				"multiple columns are marked as auto-filled (%s), but this SQL dialect only supports at most one per table",
				strings.Join(p.AutoColumnNames, ", "),
			)
		}
	}

	// prepare query strings
	p.Select = p.buildSelectQueryIfPossible(dialect)
	p.Insert = p.buildInsertQueryIfPossible(dialect)
	p.Update = p.buildUpdateQueryIfPossible(dialect)
	p.Delete = p.buildDeleteQueryIfPossible(dialect)

	return p, nil
}

func (p plan) getNonAutoColumnNames() []string {
	result := make([]string, 0, len(p.AllColumnNames)-len(p.AutoColumnNames))
	for _, columnName := range p.AllColumnNames {
		if !slices.Contains(p.AutoColumnNames, columnName) {
			result = append(result, columnName)
		}
	}
	return result
}

func (p plan) getNonPrimaryKeyColumnNames() []string {
	result := make([]string, 0, len(p.AllColumnNames)-len(p.PrimaryKeyColumnNames))
	for _, columnName := range p.AllColumnNames {
		if !slices.Contains(p.PrimaryKeyColumnNames, columnName) {
			result = append(result, columnName)
		}
	}
	return result
}

func (p plan) buildSelectQueryIfPossible(dialect Dialect) plannedQuery {
	if p.TableName == "" {
		return plannedQuery{Query: ""}
	}

	var (
		scanIndexes       = make([][]int, len(p.AllColumnNames))
		quotedColumnNames = make([]string, len(p.AllColumnNames))
	)
	for idx, columnName := range p.AllColumnNames {
		scanIndexes[idx] = p.IndexByColumnName[columnName]
		quotedColumnNames[idx] = dialect.QuoteIdentifier(columnName)
	}

	query := fmt.Sprintf(
		`SELECT %s FROM %s WHERE `,
		strings.Join(quotedColumnNames, ", "),
		dialect.QuoteIdentifier(p.TableName),
	)
	return plannedQuery{query, nil, scanIndexes}
}

func (p plan) buildInsertQueryIfPossible(dialect Dialect) plannedQuery {
	if p.TableName == "" || len(p.AllColumnNames) == 0 {
		return plannedQuery{Query: ""}
	}
	nonAutoColumnNames := p.getNonAutoColumnNames()
	if len(nonAutoColumnNames) == 0 {
		return plannedQuery{Query: ""}
	}

	var (
		argumentIndexes    = make([][]int, len(nonAutoColumnNames))
		scanIndexes        [][]int
		quotedColumnNames  = make([]string, len(nonAutoColumnNames))
		quotedPlaceholders = make([]string, len(nonAutoColumnNames))
	)
	for idx, columnName := range nonAutoColumnNames {
		argumentIndexes[idx] = p.IndexByColumnName[columnName]
		quotedColumnNames[idx] = dialect.QuoteIdentifier(columnName)
		quotedPlaceholders[idx] = dialect.Placeholder(idx)
	}
	if len(p.AutoColumnNames) > 0 {
		// NOTE: This is filled even if dialect.UsesLastInsertID() is false.
		// We need this index to find the right value on which to run SetInt() or SetUint().
		scanIndexes = make([][]int, len(p.AutoColumnNames))
		for idx, columnName := range p.AutoColumnNames {
			scanIndexes[idx] = p.IndexByColumnName[columnName]
		}
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
	return plannedQuery{query, argumentIndexes, scanIndexes}
}

func (p plan) buildUpdateQueryIfPossible(dialect Dialect) plannedQuery {
	if p.TableName == "" || len(p.PrimaryKeyColumnNames) == 0 {
		return plannedQuery{Query: ""}
	}
	nonPrimaryKeyColumnNames := p.getNonPrimaryKeyColumnNames()
	if len(nonPrimaryKeyColumnNames) == 0 {
		return plannedQuery{Query: ""}
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
	return plannedQuery{query, slices.Concat(setArgumentIndexes, whereArgumentIndexes), nil}
}

func (p plan) buildDeleteQueryIfPossible(dialect Dialect) plannedQuery {
	if p.TableName == "" || len(p.PrimaryKeyColumnNames) == 0 {
		return plannedQuery{Query: ""}
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
	return plannedQuery{query, argumentIndexes, nil}
}
