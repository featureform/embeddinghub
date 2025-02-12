package tsquery

import (
	"fmt"
	"sort"
	"strings"

	"github.com/featureform/fferr"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
)

type joinType string

const (
	joinLeft joinType = "LEFT JOIN"
	joinAsOf joinType = "ASOF JOIN"
)

type QueryConfig struct {
	UseAsOfJoin bool
	QuoteChar   string
	QuoteTable  bool
}

type BuilderParams struct {
	LabelEntityMappings    *metadata.EntityMappings
	SanitizedLabelTable    string
	FeatureColumns         []metadata.ResourceVariantColumns
	SanitizedFeatureTables []string
	FeatureNameVariants    []metadata.ResourceID
	FeatureEntityNames     []string
}

// NewTrainingSet creates a new training set query builder based on the label and feature columns provided.
func NewTrainingSet(config QueryConfig, params BuilderParams) *TrainingSet {
	lbtTable := labelTable{
		SanitizedTableName: params.SanitizedLabelTable,
		EntityMappings:     params.LabelEntityMappings,
	}

	featureTables := make([]featureTable, len(params.FeatureColumns))
	for i, cols := range params.FeatureColumns {
		featureTables[i] = featureTable{
			Entity:             cols.Entity,
			Values:             []string{cols.Value},
			TS:                 cols.TS,
			SanitizedTableName: params.SanitizedFeatureTables[i],
			ColumnAliases:      []string{fmt.Sprintf("feature__%s__%s", params.FeatureNameVariants[i].Name, params.FeatureNameVariants[i].Variant)},
			EntityName:         params.FeatureEntityNames[i],
		}
	}

	return &TrainingSet{
		labelTable:    lbtTable,
		featureTables: featureTables,
		config:        config,
	}
}

// TrainingSet represents a training set query builder.
type TrainingSet struct {
	labelTable    labelTable
	featureTables []featureTable
	config        QueryConfig
}

type featureTable struct {
	Entity             string
	Values             []string
	TS                 string
	SanitizedTableName string
	ColumnAliases      []string
	EntityName         string
}

type labelTable struct {
	SanitizedTableName string
	EntityMappings     *metadata.EntityMappings
}

type featureTableMap map[string]*featureTable

func (ftm featureTableMap) Keys() []string {
	keys := make([]string, len(ftm))
	i := 0
	for k := range ftm {
		keys[i] = k
		i++
	}
	sort.Strings(keys)
	return keys
}

// CompileSQL compiles the label and feature tables into a single SQL query
// that uses LEFT JOINs, ASOF JOINs and/or CTEs to create a training set
// based on the presence or absence of timestamps in the label and feature tables.
func (t TrainingSet) CompileSQL() (string, error) {
	if t.labelTable.EntityMappings.TimestampColumn != "" {
		builder := &pitTrainingSetQueryBuilder{
			labelTable:      t.labelTable,
			featureTableMap: make(map[string]*featureTable),
			config:          t.config,
		}
		for _, ft := range t.featureTables {
			builder.AddFeature(ft)
		}
		if err := builder.Compile(); err != nil {
			return "", err
		}
		return builder.ToSQL(), nil
	}
	builder := &trainingSetQueryBuilder{
		labelTable:      t.labelTable,
		featureTableMap: make(map[string]*featureTable),
		config:          t.config,
	}
	for _, ft := range t.featureTables {
		builder.AddFeature(ft)
	}
	if err := builder.Compile(); err != nil {
		return "", err
	}
	return builder.ToSQL(), nil

}

// ctes represents a list of common table expressions.
type ctes []cte

// ToSQL returns the SQL representation of the CTEs, including the WITH keyword
// and the comma-separated list of CTEs.
func (c ctes) ToSQL(config QueryConfig) string {
	if len(c) == 0 {
		return ""
	}
	var sb strings.Builder
	sb.WriteString("WITH ")
	ctes := make([]string, len(c))
	for i, cte := range c {
		ctes[i] = cte.ToSQL()
	}
	sb.WriteString(strings.Join(ctes, ", "))
	sb.WriteString(" ")
	return sb.String()
}

// leftJoins represents a list of LEFT JOINs.
type leftJoins []leftJoin

// ToSQL returns the SQL representation of the LEFT JOINs, space-separated.
func (j leftJoins) ToSQL(config QueryConfig) string {
	joins := make([]string, len(j))
	for i, join := range j {
		joins[i] = join.ToSQL()
	}
	return strings.Join(joins, " ")
}

// cols represents a list of columns.
type cols []col

// ToSQL returns the SQL representation of the columns, comma-separated.
func (c cols) ToSQL(config QueryConfig) string {
	cols := make([]string, len(c))
	for i, col := range c {
		cols[i] = col.ToSQL(config)
	}
	return strings.Join(cols, ", ")
}

// asOfJoins represents a list of ASOF JOINs.
type asOfJoins []asOfJoin

// ToSQL returns the SQL representation of the ASOF JOINs, space-separated.
func (j asOfJoins) ToSQL(config QueryConfig) string {
	joins := make([]string, len(j))
	for i, join := range j {
		joins[i] = join.ToSQL()
	}
	return strings.Join(joins, " ")
}

// windowJoin represents an ASOF join, just in environments where the ASOF
// join doesn't exist. Instead, we emulate it by manually filtering for entries
// based on their timestamp, and grab the most recent one.
type windowJoin struct {
	entity     string
	ts         string
	label      string
	labelTable string
	ft         *featureTable
}

func (j windowJoin) EntitySQL(config QueryConfig) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("%s",
		j.entity,
	))
	return sb.String()
}

// ToSQL creates an ASOF JOIN between the label and feature tables.
func (j windowJoin) FeatureSQL(index int) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf(
		`feature_%d AS (
  SELECT
    %s as ts,
    l.%s,
    l.label,

`,
		index,
		j.ft.TS,
		j.entity,
	))

	for i := range j.ft.Values {
		sb.WriteString(fmt.Sprintf("    %s,\n", j.ft.Values[i]))
	}

	sb.WriteString(fmt.Sprintf(`
    ROW_NUMBER() OVER (
        PARTITION BY l.%s, l.ts
        ORDER BY f%d.%s DESC
    ) AS rn
    FROM labels l
    LEFT JOIN %s f%d
      ON l.%s = f%d.%s
      AND f%d.%s <= l.ts
),`,
		j.entity,
		index,
		j.ft.TS,
		j.ft.SanitizedTableName,
		index,
		j.entity,
		index,
		j.ft.Entity,
		index,
		j.ft.TS,
	))

	sb.WriteString(fmt.Sprintf(`
feature_%d_filtered AS (
  SELECT
    ts,
    %s,
    label,

`,
		index,
		j.entity,
	))

	joins := make([]string, len(j.ft.Values))
	for i := range j.ft.Values {
		joins[i] = "    " + j.ft.Values[i]
	}
	sb.WriteString(strings.Join(joins, ",\n"))

	sb.WriteString(fmt.Sprintf(`
  FROM feature_%d
  WHERE rn=1
)`,
		index,
	))

	return sb.String()
}

type windowJoins struct {
	ts         string
	label      string
	labelTable string
	windows    []windowJoin
}

func (j windowJoins) HeaderSQL(config QueryConfig) string {
	if len(j.windows) == 0 {
		return ""
	}

	quoteChar := ""
	if config.QuoteTable {
		quoteChar = config.QuoteChar
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf(`WITH labels AS (
  SELECT
    %s as ts,
    %s AS label,
`,
		j.ts,
		j.label,
	))

	joins := make([]string, len(j.windows))
	for i, join := range j.windows {
		joins[i] = "    " + join.EntitySQL(config)
	}
	sb.WriteString(strings.Join(joins, ",\n"))

	sb.WriteString(fmt.Sprintf(`
  FROM %s%s%s
),
`,
		quoteChar,
		j.labelTable,
		quoteChar,
	))

	for i, j := range j.windows {
		joins[i] = j.FeatureSQL(i + 1)
	}
	sb.WriteString(strings.Join(joins, ",\n"))
	sb.WriteString("\n")
	return sb.String()
}

func (j windowJoins) SelectSQL(config QueryConfig) string {
	if len(j.windows) == 0 {
		return ""
	}

	var sb strings.Builder

	for i, window := range j.windows {
		sb.WriteString(fmt.Sprintf(`
LEFT JOIN feature_%d_filtered f%d
  ON f%d.ts = l.%s
  AND f%d.%s = l.%s`,
			i+1,
			i+1,
			i+1,
			window.ts,
			i+1,
			window.entity,
			window.entity,
		))
	}

	return sb.String()
}

// cte represents a common table expression for the case when the label
// does not have a timestamp column, but the feature table does.
type cte struct {
	alias string
	ft    *featureTable
}

// ToSQL creates a row number column (rn) partitioned by the entity column and
// ordered by the timestamp column in descending order.
func (c cte) ToSQL() string {
	selectClause := fmt.Sprintf("SELECT %s, %s, %s", c.ft.Entity, strings.Join(c.ft.Values, ", "), c.ft.TS)
	partitionStmt := fmt.Sprintf(",ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s DESC) AS rn ", c.ft.Entity, c.ft.TS)
	fromStmt := fmt.Sprintf("FROM %s ", c.ft.SanitizedTableName)
	cte := fmt.Sprintf("%s AS (%s %s %s)", c.alias, selectClause, partitionStmt, fromStmt)
	return cte
}

// leftJoin represents a LEFT JOIN between the label and feature tables.
type leftJoin struct {
	alias     string
	ft        *featureTable
	lblEntity string
}

// ToSQL creates a LEFT JOIN between the label and feature tables or feature CTEs.
func (j leftJoin) ToSQL() string {
	// CTE
	if j.ft.TS != "" {
		return fmt.Sprintf("LEFT JOIN %s ON l.%s = %s.%s AND %s.rn = 1", j.alias, j.lblEntity, j.alias, j.ft.Entity, j.alias)
	}
	return fmt.Sprintf("LEFT JOIN %s %s ON l.%s = %s.%s", j.ft.SanitizedTableName, j.alias, j.lblEntity, j.alias, j.ft.Entity)
}

// asOfJoin represents an ASOF JOIN between the label and feature tables.
type asOfJoin struct {
	alias     string
	ft        *featureTable
	lblEntity string
	lblTS     string
}

// ToSQL creates an ASOF JOIN between the label and feature tables.
func (j asOfJoin) ToSQL() string {
	joinClause := fmt.Sprintf("%s %s %s", joinAsOf, j.ft.SanitizedTableName, j.alias)
	matchCondition := fmt.Sprintf("MATCH_CONDITION(l.%s >= %s.%s)", j.lblTS, j.alias, j.ft.TS)
	onClause := fmt.Sprintf("ON(l.%s = %s.%s)", j.lblEntity, j.alias, j.ft.Entity)
	return fmt.Sprintf("%s %s %s", joinClause, matchCondition, onClause)
}

// col represents a column in the SELECT clause, with a table alias, column name, and column alias.
type col struct {
	tableAlias string
	val        string
	colAlias   string
}

// ToSQL returns the SQL representation of the column, with the table alias, column name, and column alias.
func (c col) ToSQL(config QueryConfig) string {
	return fmt.Sprintf("%s.%s AS %s%s%s", c.tableAlias, c.val, config.QuoteChar, c.colAlias, config.QuoteChar)
}

// pitTrainingSetQueryBuilder represents a point-in-time training set query builder.
type pitTrainingSetQueryBuilder struct {
	labelTable      labelTable
	featureTableMap featureTableMap
	columns         cols
	leftJoins       leftJoins
	asOfJoins       asOfJoins
	windowJoins     windowJoins
	config          QueryConfig
}

// addFeature adds a feature table to the point-in-time training set query builder; if a feature uses
// the same table, entity, and timestamp column as another feature, then the values and column aliases
// are combined into a single feature table so that the query uses a single join for all.
func (b *pitTrainingSetQueryBuilder) AddFeature(tbl featureTable) {
	key := createTableKey(tbl.SanitizedTableName, tbl.EntityName, tbl.Entity, tbl.TS)
	existing, exists := b.featureTableMap[key]
	if exists {
		existing.Values = append(existing.Values, tbl.Values...)
		existing.ColumnAliases = append(existing.ColumnAliases, tbl.ColumnAliases...)
	} else {
		b.featureTableMap[key] = &tbl
	}
}

// Compile compiles the point-in-time training set query builder by creating columns, LEFT JOINs, and ASOF JOINs structs.
func (b *pitTrainingSetQueryBuilder) Compile() error {
	if err := validateLabelTable(b.labelTable); err != nil {
		return err
	}

	b.windowJoins = windowJoins{
		ts:         b.labelTable.EntityMappings.TimestampColumn,
		label:      b.labelTable.EntityMappings.ValueColumn,
		labelTable: b.labelTable.SanitizedTableName,
	}

	for i, k := range b.featureTableMap.Keys() {
		ft := b.featureTableMap[k]
		ftAlias := fmt.Sprintf("f%d", i+1)
		// COLUMNS

		for i, val := range ft.Values {
			if err := validateFeatureTable(*ft); err != nil {
				return err
			}
			b.columns = append(b.columns, col{tableAlias: ftAlias, val: val, colAlias: ft.ColumnAliases[i]})
		}
		// JOINS
		for _, m := range b.labelTable.EntityMappings.Mappings {
			if m.Name == ft.EntityName {
				if ft.TS != "" {
					if b.config.UseAsOfJoin {
						b.asOfJoins = append(b.asOfJoins, asOfJoin{alias: ftAlias, ft: ft, lblEntity: m.EntityColumn, lblTS: b.labelTable.EntityMappings.TimestampColumn})
					} else {
						b.windowJoins.windows = append(b.windowJoins.windows, windowJoin{
							entity:     m.EntityColumn,
							ft:         ft,
							ts:         b.labelTable.EntityMappings.TimestampColumn,
							label:      b.labelTable.EntityMappings.ValueColumn,
							labelTable: b.labelTable.SanitizedTableName,
						})
					}
				} else {
					b.leftJoins = append(b.leftJoins, leftJoin{alias: ftAlias, ft: ft, lblEntity: m.EntityColumn})
				}
				break
			}
		}
	}
	return nil
}

// ToSQL returns the SQL representation of the point-in-time training set query builder.
func (b *pitTrainingSetQueryBuilder) ToSQL() string {
	quoteChar := ""
	if b.config.QuoteTable {
		quoteChar = b.config.QuoteChar
	}

	var sb strings.Builder
	// WINDOW (Alternative to ASOF on platforms that don't support it)
	sb.WriteString(b.windowJoins.HeaderSQL(b.config))
	// SELECT
	sb.WriteString(fmt.Sprintf("SELECT %s, l.%s AS label", b.columns.ToSQL(b.config), b.labelTable.EntityMappings.ValueColumn))
	// FROM
	sb.WriteString(fmt.Sprintf(" FROM %s%s%s l ", quoteChar, b.labelTable.SanitizedTableName, quoteChar))
	// JOIN(s)
	sb.WriteString(b.leftJoins.ToSQL(b.config))
	sb.WriteString(" ")
	sb.WriteString(b.asOfJoins.ToSQL(b.config))
	if len(b.windowJoins.windows) > 0 {
		sb.WriteString(" ")
		sb.WriteString(b.windowJoins.SelectSQL(b.config))
	}
	sb.WriteString(";")
	return sb.String()
}

// trainingSetQueryBuilder represents a training set query builder for the case when the label
// does not have a timestamp column, and the feature tables may or may not have a timestamp column.
type trainingSetQueryBuilder struct {
	labelTable      labelTable
	featureTableMap featureTableMap
	ctes            ctes
	columns         cols
	joins           leftJoins
	config          QueryConfig
}

// AddFeature adds a feature table to the training set query builder; if a feature uses the same table,
// entity, and timestamp column as another feature, then the values and column aliases are combined into
// a single feature table so that the query uses a single join and/or CTE for all.
func (b *trainingSetQueryBuilder) AddFeature(tbl featureTable) {
	key := createTableKey(tbl.SanitizedTableName, tbl.EntityName, tbl.Entity, tbl.TS)
	existing, exists := b.featureTableMap[key]
	if exists {
		existing.Values = append(existing.Values, tbl.Values...)
		existing.ColumnAliases = append(existing.ColumnAliases, tbl.ColumnAliases...)
	} else {
		b.featureTableMap[key] = &tbl
	}
}

// Compile compiles the training set query builder by creating CTEs, columns, and LEFT JOINs structs.
func (b *trainingSetQueryBuilder) Compile() error {
	if err := validateLabelTable(b.labelTable); err != nil {
		return err
	}
	for i, k := range b.featureTableMap.Keys() {
		ft := b.featureTableMap[k]
		if err := validateFeatureTable(*ft); err != nil {
			return err
		}
		ftAlias := fmt.Sprintf("f%d", i+1)
		usesCTE := ft.TS != ""
		// CTE
		if usesCTE {
			ftAlias = fmt.Sprintf("%s_cte", ftAlias)
			b.ctes = append(b.ctes, cte{alias: ftAlias, ft: ft})
		}
		// JOIN
		for _, m := range b.labelTable.EntityMappings.Mappings {
			if m.Name == ft.EntityName {
				b.joins = append(b.joins, leftJoin{alias: ftAlias, ft: ft, lblEntity: m.EntityColumn})
				break
			}
		}
		// COLUMNS
		for i, val := range ft.Values {
			b.columns = append(b.columns, col{tableAlias: ftAlias, val: val, colAlias: ft.ColumnAliases[i]})
		}
	}
	return nil
}

// ToSQL returns the SQL representation of the training set query builder.
func (b *trainingSetQueryBuilder) ToSQL() string {
	quoteChar := ""
	if b.config.QuoteTable {
		quoteChar = b.config.QuoteChar
	}

	var sb strings.Builder
	// CTE(s)
	sb.WriteString(b.ctes.ToSQL(b.config))
	// SELECT
	sb.WriteString(fmt.Sprintf("SELECT %s, l.%s AS label ", b.columns.ToSQL(b.config), b.labelTable.EntityMappings.ValueColumn))
	// FROM
	sb.WriteString(fmt.Sprintf("FROM %s%s%s l ", quoteChar, b.labelTable.SanitizedTableName, quoteChar))
	// JOIN(s)
	sb.WriteString(b.joins.ToSQL(b.config))
	sb.WriteString(";")
	return sb.String()
}

// validateFeatureTable validates the feature table.
func validateFeatureTable(ft featureTable) error {
	if ft.Entity == "" {
		logging.GlobalLogger.Errorw("feature entity cannot be empty", "feature_table", ft)
		return fferr.NewInternalErrorf("feature entity cannot be empty: %v", ft)
	}
	if len(ft.Values) == 0 {
		logging.GlobalLogger.Errorw("values cannot be empty", "feature_table", ft)
		return fferr.NewInternalErrorf("values cannot be empty")
	}
	if ft.SanitizedTableName == "" {
		logging.GlobalLogger.Errorw("sanitized table name cannot be empty", "feature_table", ft)
		return fferr.NewInternalErrorf("sanitized table name cannot be empty")
	}
	if len(ft.ColumnAliases) == 0 {
		logging.GlobalLogger.Errorw("column aliases cannot be empty", "feature_table", ft)
		return fferr.NewInternalErrorf("column aliases cannot be empty")
	}
	if len(ft.Values) != len(ft.ColumnAliases) {
		logging.GlobalLogger.Errorw("values and column aliases must be the same length", "feature_table", ft)
		return fferr.NewInternalErrorf("values and column aliases must be the same length")
	}
	return nil
}

// validateLabelTable validates the label table.
func validateLabelTable(lbl labelTable) error {
	if lbl.EntityMappings == nil || len(lbl.EntityMappings.Mappings) == 0 {
		logging.GlobalLogger.Errorw("entity mappings cannot be empty", "label_table", lbl)
		return fferr.NewInternalErrorf("entity mappings cannot be empty")
	}
	if lbl.EntityMappings.ValueColumn == "" {
		logging.GlobalLogger.Errorw("value column cannot be empty", "label_table", lbl)
		return fferr.NewInternalErrorf("value column cannot be empty")
	}
	for _, m := range lbl.EntityMappings.Mappings {
		if m.EntityColumn == "" {
			logging.GlobalLogger.Errorw("entity column cannot be empty", "label_table", lbl)
			return fferr.NewInternalErrorf("entity column cannot be empty")
		}
		if m.Name == "" {
			logging.GlobalLogger.Errorw("name cannot be empty", "label_table", lbl)
			return fferr.NewInternalErrorf("name cannot be empty")
		}
	}
	if lbl.SanitizedTableName == "" {
		logging.GlobalLogger.Errorw("sanitized table name cannot be empty", "label_table", lbl)
		return fferr.NewInternalErrorf("sanitized table name cannot be empty")
	}
	return nil
}

func createTableKey(tableName, entityName, entityCol, ts string) string {
	return fmt.Sprintf("%s_%s_%s_%s", tableName, entityName, entityCol, ts)
}
