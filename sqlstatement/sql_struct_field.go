package sqlstatement

import (
	"database/sql"
	cmap "github.com/orcaman/concurrent-map/v2"
	"strings"
)

var (
	// 通过表名获取列信息
	columnListMap = cmap.New[[]*ColumnInfo]()
)

type ColumnInfo struct {
	ColumnName string
	IsNullable string
	DataType   string
	ColumnKey  string
	ColumnType string
	Extra      string
}

func getTableColumns(db *sql.DB, tableName string) ([]*ColumnInfo, error) {
	if columnListMap.Has(tableName) {
		if columnList, ok := columnListMap.Get(tableName); ok {
			return columnList, nil
		}
	}
	var columns []*ColumnInfo
	query := `SELECT TABLE_SCHEMA, COLUMN_NAME, IS_NULLABLE, DATA_TYPE, COLUMN_KEY, COLUMN_TYPE, EXTRA FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME =?`
	newTableName := strings.ReplaceAll(tableName, "`", "")
	newTableName = strings.TrimSpace(newTableName)
	rows, err := db.Query(query, newTableName)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	dataBaseName := ""
	for rows.Next() {
		var col ColumnInfo
		err = rows.Scan(&dataBaseName, &col.ColumnName, &col.IsNullable, &col.DataType, &col.ColumnKey, &col.ColumnType, &col.Extra)
		if err != nil {
			return nil, err
		}
		columns = append(columns, &col)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	columnListMap.Set(tableName, columns)

	return columns, nil
}
