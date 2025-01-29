package tsquery

import (
	"testing"
)

func TestNewTrainingSetQueryBuilder(t *testing.T) {
	cases := []struct {
		name        string
		lbl         labelTable
		fts         []featureTable
		expectedErr bool
		expectedSQL string
	}{
		{
			name: "All features and label use timestamps",
			lbl: labelTable{
				Entity:             "location_id",
				Value:              "wave_height_ft",
				TS:                 "observed_on",
				SanitizedTableName: "\"DEMO2\".\"CORRECTNESS\".\"wave_height_labels_ts\"",
			},
			fts: []featureTable{
				{
					Entity:             "location_id",
					Values:             []string{"swell_direction"},
					TS:                 "measured_on",
					SanitizedTableName: "\"DEMO2\".\"CORRECTNESS\".\"surf_conditions_features_ts\"",
					ColumnAliases:      []string{"feature__swell_direction__variant"},
				},
				{
					Entity:             "location_id",
					Values:             []string{"wave_power_kj"},
					TS:                 "measured_on",
					SanitizedTableName: "\"DEMO2\".\"CORRECTNESS\".\"surf_conditions_features_ts\"",
					ColumnAliases:      []string{"feature__wave_power_kj__variant"},
				},
				{
					Entity:             "location_id",
					Values:             []string{"swell_period_sec"},
					TS:                 "measured_on",
					SanitizedTableName: "\"DEMO2\".\"CORRECTNESS\".\"surf_conditions_features_ts\"",
					ColumnAliases:      []string{"feature__swell_period_sec__variant"},
				},
			},
			expectedErr: false,
			expectedSQL: `SELECT f1.swell_direction AS "feature__swell_direction__variant", f1.wave_power_kj AS "feature__wave_power_kj__variant", f1.swell_period_sec AS "feature__swell_period_sec__variant", l.wave_height_ft AS label FROM "DEMO2"."CORRECTNESS"."wave_height_labels_ts" l  ASOF JOIN "DEMO2"."CORRECTNESS"."surf_conditions_features_ts" f1 MATCH_CONDITION(l.observed_on >= f1.measured_on) ON(l.location_id = f1.location_id);`,
		},
		{
			name: "Some features don't use timestamps and label use timestamps",
			lbl: labelTable{
				Entity:             "location_id",
				Value:              "wave_height_ft",
				TS:                 "observed_on",
				SanitizedTableName: "\"DEMO2\".\"CORRECTNESS\".\"wave_height_labels_ts\"",
			},
			fts: []featureTable{
				{
					Entity:             "location_id",
					Values:             []string{"swell_direction"},
					SanitizedTableName: "\"DEMO2\".\"CORRECTNESS\".\"surf_conditions_features_ts\"",
					ColumnAliases:      []string{"feature__swell_direction__variant"},
				},
				{
					Entity:             "location_id",
					Values:             []string{"wave_power_kj"},
					TS:                 "measured_on",
					SanitizedTableName: "\"DEMO2\".\"CORRECTNESS\".\"surf_conditions_features_ts\"",
					ColumnAliases:      []string{"feature__wave_power_kj__variant"},
				},
			},
			expectedErr: false,
			expectedSQL: `SELECT f1.swell_direction AS "feature__swell_direction__variant", f2.wave_power_kj AS "feature__wave_power_kj__variant", l.wave_height_ft AS label FROM "DEMO2"."CORRECTNESS"."wave_height_labels_ts" l LEFT JOIN "DEMO2"."CORRECTNESS"."surf_conditions_features_ts" f1 ON l.location_id = f1.location_id ASOF JOIN "DEMO2"."CORRECTNESS"."surf_conditions_features_ts" f2 MATCH_CONDITION(l.observed_on >= f2.measured_on) ON(l.location_id = f2.location_id);`,
		},
		{
			name: "All features use timestamps and label does not",
			lbl: labelTable{
				Entity:             "location_id",
				Value:              "level",
				SanitizedTableName: "\"DEMO2\".\"CORRECTNESS\".\"location_level_labels_no_ts\"",
			},
			fts: []featureTable{
				{
					Entity:             "location_id",
					Values:             []string{"wave_height_ft"},
					TS:                 "measured_on",
					SanitizedTableName: "\"DEMO2\".\"CORRECTNESS\".\"wave_conditions_features_ts\"",
					ColumnAliases:      []string{"feature__wave_height_ft__variant"},
				},
				{
					Entity:             "location_id",
					Values:             []string{"wave_power_kj"},
					TS:                 "measured_on",
					SanitizedTableName: "\"DEMO2\".\"CORRECTNESS\".\"wave_conditions_features_ts\"",
					ColumnAliases:      []string{"feature__wave_power_kj__variant"},
				},
				{
					Entity:             "location_id",
					Values:             []string{"swell_period_sec"},
					TS:                 "measured_on",
					SanitizedTableName: "\"DEMO2\".\"CORRECTNESS\".\"wave_conditions_features_ts\"",
					ColumnAliases:      []string{"feature__swell_period_sec__variant"},
				},
			},
			expectedErr: false,
			expectedSQL: `WITH f1_cte AS (SELECT location_id, wave_height_ft, wave_power_kj, swell_period_sec, measured_on ,ROW_NUMBER() OVER (PARTITION BY location_id ORDER BY measured_on DESC) AS rn  FROM "DEMO2"."CORRECTNESS"."wave_conditions_features_ts" ) SELECT f1_cte.wave_height_ft AS "feature__wave_height_ft__variant", f1_cte.wave_power_kj AS "feature__wave_power_kj__variant", f1_cte.swell_period_sec AS "feature__swell_period_sec__variant", l.level AS label FROM "DEMO2"."CORRECTNESS"."location_level_labels_no_ts" l LEFT JOIN f1_cte ON l.location_id = f1_cte.location_id AND f1_cte.rn = 1;`,
		},
		{
			name: "Neither features nor label use timestamps",
			lbl: labelTable{
				Entity:             "surfer_id",
				Value:              "skill_level",
				SanitizedTableName: "\"DEMO2\".\"CORRECTNESS\".\"surfer_skill_labels_no_ts\"",
			},
			fts: []featureTable{
				{
					Entity:             "surfer_id",
					Values:             []string{"avg_wave_height_m"},
					SanitizedTableName: "\"DEMO2\".\"CORRECTNESS\".\"surfer_success_rates_features_no_ts\"",
					ColumnAliases:      []string{"feature__avg_wave_height_m__variant"},
				},
				{
					Entity:             "surfer_id",
					Values:             []string{"avg_success_rate_perc"},
					SanitizedTableName: "\"DEMO2\".\"CORRECTNESS\".\"surfer_success_rates_features_no_ts\"",
					ColumnAliases:      []string{"feature__avg_success_rate_perc__variant"},
				},
				{
					Entity:             "surfer_id",
					Values:             []string{"favorite_spot"},
					SanitizedTableName: "\"DEMO2\".\"CORRECTNESS\".\"surfer_success_rates_features_no_ts\"",
					ColumnAliases:      []string{"feature__favorite_spot__variant"},
				},
			},
			expectedErr: false,
			expectedSQL: `SELECT f1.avg_wave_height_m AS "feature__avg_wave_height_m__variant", f1.avg_success_rate_perc AS "feature__avg_success_rate_perc__variant", f1.favorite_spot AS "feature__favorite_spot__variant", l.skill_level AS label FROM "DEMO2"."CORRECTNESS"."surfer_skill_labels_no_ts" l LEFT JOIN "DEMO2"."CORRECTNESS"."surfer_success_rates_features_no_ts" f1 ON l.surfer_id = f1.surfer_id;`,
		},
		{
			name: "Features and labels in the same table",
			lbl: labelTable{
				Entity:             "location_id",
				Value:              "wave_power_kj",
				TS:                 "measured_on",
				SanitizedTableName: "\"DEMO2\".\"CORRECTNESS\".\"surf_conditions_features_ts\"",
			},
			fts: []featureTable{
				{
					Entity:             "location_id",
					Values:             []string{"swell_direction"},
					TS:                 "measured_on",
					SanitizedTableName: "\"DEMO2\".\"CORRECTNESS\".\"surf_conditions_features_ts\"",
					ColumnAliases:      []string{"feature__swell_direction__variant"},
				},
				{
					Entity:             "location_id",
					Values:             []string{"wind_speed_kt"},
					TS:                 "measured_on",
					SanitizedTableName: "\"DEMO2\".\"CORRECTNESS\".\"surf_conditions_features_ts\"",
					ColumnAliases:      []string{"feature__wave_height_ft__variant"},
				},
				{
					Entity:             "location_id",
					Values:             []string{"swell_period_sec"},
					TS:                 "measured_on",
					SanitizedTableName: "\"DEMO2\".\"CORRECTNESS\".\"surf_conditions_features_ts\"",
					ColumnAliases:      []string{"feature__swell_period_sec__variant"},
				},
			},
			expectedErr: false,
			expectedSQL: `SELECT f1.swell_direction AS "feature__swell_direction__variant", f1.wind_speed_kt AS "feature__wave_height_ft__variant", f1.swell_period_sec AS "feature__swell_period_sec__variant", l.wave_power_kj AS label FROM "DEMO2"."CORRECTNESS"."surf_conditions_features_ts" l  ASOF JOIN "DEMO2"."CORRECTNESS"."surf_conditions_features_ts" f1 MATCH_CONDITION(l.measured_on >= f1.measured_on) ON(l.location_id = f1.location_id);`,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.lbl.TS != "" {
				builder := &pitTrainingSetQueryBuilder{
					labelTable:      c.lbl,
					featureTableMap: make(map[string]*featureTable),
					config:          QueryConfig{UseAsOfJoin: true, QuoteChar: "\"", QuoteTable: false},
				}
				for _, ft := range c.fts {
					builder.AddFeature(ft)
				}
				if err := builder.Compile(); (err != nil) != c.expectedErr {
					t.Errorf("Expected no error, got %v", err)
				}
				sql := builder.ToSQL()
				if sql != c.expectedSQL {
					t.Errorf("Expected SQL:\n%s\nGot:\n%s", c.expectedSQL, sql)
				}
			} else {
				builder := &trainingSetQueryBuilder{
					labelTable:      c.lbl,
					featureTableMap: make(map[string]*featureTable),
					config:          QueryConfig{UseAsOfJoin: true, QuoteChar: "\"", QuoteTable: false},
				}
				for _, ft := range c.fts {
					builder.AddFeature(ft)
				}
				if err := builder.Compile(); (err != nil) != c.expectedErr {
					t.Errorf("Expected no error, got %v", err)
				}
				sql := builder.ToSQL()
				if sql != c.expectedSQL {
					t.Errorf("Expected SQL:\n%sGot:\n%s", c.expectedSQL, sql)
				}
			}
		})
	}
}
