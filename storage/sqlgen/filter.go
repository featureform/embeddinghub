// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package sqlgen

import (
	"fmt"
	"strings"

	"github.com/featureform/fferr"
	"github.com/featureform/storage/query"
)

func compileFilters(filters []query.Query) (string, []any, error) {
	if len(filters) == 0 {
		return "", []any{}, nil
	}
	predicates := make([]string, len(filters))
	args := make([]any, 0, len(filters))
	argNum := 1
	for i, filter := range filters {
		// i starts at 0, but args start at 1 in postgres.
		qry, qryArgs, err := compileFilter(filter, argNum)
		if err != nil {
			return "", nil, err
		}
		args = append(args, qryArgs...)
		argNum += len(qryArgs)
		predicates[i] = qry
	}
	predicateStr := strings.Join(predicates, " AND ")
	return "WHERE " + predicateStr, args, nil
}

func compileFilter(filter query.Query, argNum int) (string, []any, error) {
	switch casted := filter.(type) {
	case query.KeyPrefix:
		return compileKeyPrefix(casted, argNum)
	case query.ValueEquals:
		return compileValueEquals(casted, argNum)
	case query.ValueIn:
		return compileValueIn(casted, argNum)
	case query.ArrayContains:
		return compileArrayContains(casted, argNum)
	case query.ObjectArrayContains:
		return compileObjectArrayContains(casted, argNum)
	case query.ValueLike:
		return compileValueLike(casted, argNum)
	case query.ConditionalOR:
		return compileConditionalOR(casted, argNum)
	default:
		return "", nil, fferr.NewInternalErrorf("Unsupported filter type in SQL storage: %T", casted)
	}
}

func compileKeyPrefix(filter query.KeyPrefix, argNum int) (string, []any, error) {
	argStr, err := compileArgNum(argNum)
	if err != nil {
		return "", nil, err
	}
	var operation string
	if filter.Not {
		operation = "NOT LIKE"
	} else {
		operation = "LIKE"
	}
	return fmt.Sprintf("key %s %s", operation, argStr), []any{filter.Prefix + "%"}, nil
}

func compileValueEquals(qry query.ValueEquals, argNum int) (string, []any, error) {
	argStr, err := compileArgNum(argNum)
	if err != nil {
		return "", nil, err
	}
	if qry.Column == nil {
		return "", nil, fferr.NewInternalErrorf("Column not set in ValueEquals")
	}
	clmStr, err := compileColumn(qry.Column)
	if err != nil {
		return "", nil, err
	}

	// need special handling for NULL; can't use in args nor works with =, != in psql
	if qry.Value == "NULL" || qry.Value == nil {
		if qry.Not {
			return fmt.Sprintf("%s IS NOT NULL", clmStr), nil, nil
		}
		return fmt.Sprintf("%s IS NULL", clmStr), nil, nil
	}

	operation := "="
	if qry.Not {
		operation = "IS NOT"
	}

	return fmt.Sprintf("%s %s %s", clmStr, operation, argStr), []any{qry.Value}, nil
}

func compileValueIn(qry query.ValueIn, argNum int) (string, []any, error) {
	if len(qry.Values) == 0 {
		return "", nil, fferr.NewInternalErrorf("Cannot query ValueIn to an empty array in SQL")
	}
	if qry.Column == nil {
		return "", nil, fferr.NewInternalErrorf("Column not set in ValueIn")
	}
	clmStr, err := compileColumn(qry.Column)
	if err != nil {
		return "", nil, err
	}
	argStrs := make([]string, len(qry.Values))
	for i := range qry.Values {
		argStr, err := compileArgNum(argNum)
		if err != nil {
			return "", nil, err
		}
		argNum++
		argStrs[i] = argStr
	}
	argList := fmt.Sprintf("(%s)", strings.Join(argStrs, ","))
	return fmt.Sprintf("%s IN %s", clmStr, argList), qry.Values, nil
}

func compileArrayContains(qry query.ArrayContains, argNum int) (string, []any, error) {
	if len(qry.Values) == 0 {
		return "", nil, fferr.NewInternalErrorf("Cannot compile Array Contains with an empty values array")
	}
	if qry.Column == nil {
		return "", nil, fferr.NewInternalErrorf("Column not set in Array Contains")
	}

	clmStr, err := compileColumn(qry.Column)
	if err != nil {
		return "", nil, err
	}

	// need to hold all the placeholders
	argPlaceholders := make([]string, len(qry.Values))
	args := make([]any, len(qry.Values))
	for i := range qry.Values {
		argPlaceholders[i] = fmt.Sprintf("$%d", argNum+i)
		args[i] = qry.Values[i]
	}

	// group the placeholders together
	placeholderStr := strings.Join(argPlaceholders, ", ")

	// this results in an array check conditional
	return fmt.Sprintf("(%s)::jsonb ?| array[%s]", clmStr, placeholderStr), args, nil
}

func compileObjectArrayContains(qry query.ObjectArrayContains, argNum int) (string, []any, error) {
	if len(qry.Values) == 0 {
		return "", nil, fferr.NewInternalErrorf("Cannot query ValueIn to an empty array in SQL")
	}
	if qry.Column == nil {
		return "", nil, fferr.NewInternalErrorf("Column not set in Object Array Contains")
	}
	clmStr, err := compileColumn(qry.Column)
	if err != nil {
		return "", nil, err
	}
	argStrs := make([]string, len(qry.Values))
	for i := range qry.Values {
		argStr, err := compileArgNum(argNum)
		if err != nil {
			return "", nil, err
		}
		argNum++
		argStrs[i] = argStr
	}
	argList := fmt.Sprintf("(%s)", strings.Join(argStrs, ","))
	return fmt.Sprintf("EXISTS (SELECT 1 FROM jsonb_array_elements%s AS elem WHERE elem->>'%s' IN %s)", clmStr, qry.SearchField, argList), qry.Values, nil
}

func compileValueLike(qry query.ValueLike, argNum int) (string, []any, error) {
	argStr, err := compileArgNum(argNum)
	if err != nil {
		return "", nil, err
	}
	if qry.Column == nil {
		return "", nil, fferr.NewInternalErrorf("Column not set in ValueLike")
	}
	clmStr, err := compileColumn(qry.Column)
	if err != nil {
		return "", nil, err
	}
	valuePattern := fmt.Sprintf("%%%s%%", qry.Value) // This will result in '%trans%'
	return fmt.Sprintf("%s like %s", clmStr, argStr), []any{valuePattern}, nil
}

func compileConditionalOR(conditionalQry query.ConditionalOR, argNum int) (string, []any, error) {
	if len(conditionalQry.Filters) == 0 {
		return "", nil, fferr.NewInternalErrorf("Cannot compile or with no filters")
	}
	filterNum := len(conditionalQry.Filters)
	predicates := make([]string, filterNum)
	var args []any
	for i, filter := range conditionalQry.Filters {
		if filter.Category() != query.FilterQuery {
			return "", nil, fmt.Errorf("query of type %s is not allowed, only filter queries are allowed", filter.Category())
		}
		if _, ok := filter.(query.ConditionalOR); ok {
			return "", nil, fferr.NewInternalErrorf("Cannot have a conditional OR inside another conditional OR")
		}
		qry, qryArgs, err := compileFilter(filter, argNum)
		if err != nil {
			return "", nil, err
		}
		args = append(args, qryArgs...)
		argNum += len(qryArgs)
		predicates[i] = qry
	}
	predicateStr := strings.Join(predicates, " OR ")
	return fmt.Sprintf("(%s)", predicateStr), args, nil
}

func compileArgNum(argNum int) (string, error) {
	if argNum < 1 {
		return "", fferr.NewInternalErrorf("ArgNum cannot be less than 1")
	}
	return fmt.Sprintf("$%d", argNum), nil
}
