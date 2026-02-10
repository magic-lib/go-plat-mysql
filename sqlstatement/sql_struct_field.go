package sqlstatement

import (
	"database/sql"
	"fmt"
	cmap "github.com/orcaman/concurrent-map/v2"
)

var (
	// 通过表名获取列信息
	columnListMap = cmap.New[[]*ColumnInfo]()
)

type ColumnInfo struct {
	TableSchema     string
	TableName       string
	ColumnName      string
	OrdinalPosition int
	ColumnDefault   sql.NullString
	IsNullable      bool
	DataType        string
	ColumnType      string
	ColumnKey       string
	Extra           string
	ColumnComment   string
}

func getTableColumns(db *sql.DB, tableSchema, tableName string) (string, string, []*ColumnInfo, error) {
	newTableSchema := removeCodeForOneColumn(tableSchema)
	newTableName := removeCodeForOneColumn(tableName)

	if newTableName == "" {
		return "", "", nil, fmt.Errorf("tableName is empty")
	}

	cacheKey := newTableSchema + "." + newTableName
	if columnListMap.Has(cacheKey) {
		if columnList, ok := columnListMap.Get(cacheKey); ok {
			return newTableSchema, newTableName, columnList, nil
		}
	}
	var columns []*ColumnInfo
	query := `SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, COLUMN_TYPE, COLUMN_KEY, EXTRA, COLUMN_COMMENT FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME =?`
	rows, err := db.Query(query, newTableName)
	if err != nil {
		return newTableSchema, newTableName, nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		var col ColumnInfo
		var isNullable string
		var columnComment sql.NullString
		err = rows.Scan(&col.TableSchema, &col.TableName, &col.ColumnName, &col.OrdinalPosition, &col.ColumnDefault, &isNullable, &col.DataType, &col.ColumnType, &col.ColumnKey, &col.Extra, &columnComment)
		if col.TableSchema != "" {
			newTableSchema = col.TableSchema
		}
		if col.TableName != "" {
			newTableName = col.TableName
		}

		if err != nil {
			return newTableSchema, newTableName, nil, err
		}
		if isNullable == "YES" {
			col.IsNullable = true
		} else {
			col.IsNullable = false
		}
		if columnComment.Valid {
			col.ColumnComment = columnComment.String
		}
		columns = append(columns, &col)
	}
	if err := rows.Err(); err != nil {
		return newTableSchema, newTableName, nil, err
	}
	cacheKey = newTableSchema + "." + newTableName
	columnListMap.Set(cacheKey, columns)

	return newTableSchema, newTableName, columns, nil
}
