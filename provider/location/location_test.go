// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package location

import (
	"testing"
)

func TestSQLLocation_TableLocation(t *testing.T) {
	tests := []struct {
		name           string
		database       string
		schema         string
		table          string
		expectedResult string
	}{
		{
			name:           "All components present",
			database:       "test_db",
			schema:         "test_schema",
			table:          "test_table",
			expectedResult: "test_db.test_schema.test_table",
		},
		{
			name:           "Schema and table present, database missing",
			database:       "",
			schema:         "test_schema",
			table:          "test_table",
			expectedResult: "test_schema.test_table",
		},
		{
			name:           "Only table present",
			database:       "",
			schema:         "",
			table:          "test_table",
			expectedResult: "test_table",
		},
		{
			name:           "Database and table present, schema missing",
			database:       "test_db",
			schema:         "",
			table:          "test_table",
			expectedResult: "test_db.test_table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			loc := &SQLLocation{
				database: tt.database,
				schema:   tt.schema,
				table:    tt.table,
			}

			result := loc.TableLocation().String()

			if result != tt.expectedResult {
				t.Errorf("TableLocation() = %v, want %v", result, tt.expectedResult)
			}
		})
	}
}
