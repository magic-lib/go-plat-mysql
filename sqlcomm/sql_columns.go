package sqlcomm

import (
	"database/sql"
	"fmt"
	"github.com/magic-lib/go-plat-utils/cond"
	"github.com/magic-lib/go-plat-utils/conv"
	"strings"
	"time"
	"xorm.io/xorm/schemas"
)

type MysqlColumn struct {
	TableCatalog           string         `json:"table_catalog"`
	TableSchema            string         `json:"table_schema"`
	TableName              string         `json:"table_name"`
	ColumnName             string         `json:"column_name"`
	OrdinalPosition        int            `json:"ordinal_position"`
	ColumnDefault          sql.NullString `json:"column_default"` // 可为 NULL
	IsNullable             bool           `json:"is_nullable"`    // YES / NO
	DataType               string         `json:"data_type"`
	CharacterMaximumLength sql.NullInt64  `json:"character_maximum_length"`
	CharacterOctetLength   sql.NullInt64  `json:"character_octet_length"`
	NumericPrecision       sql.NullInt64  `json:"numeric_precision"`
	NumericScale           sql.NullInt64  `json:"numeric_scale"`
	DateTimePrecision      sql.NullInt64  `json:"datetime_precision"`
	CharacterSetName       sql.NullString `json:"character_set_name"`
	CollationName          sql.NullString `json:"collation_name"`
	ColumnType             string         `json:"column_type"`
	ColumnKey              string         `json:"column_key"`
	Extra                  string         `json:"extra"`
	Privileges             string         `json:"privileges"`
	ColumnComment          string         `json:"column_comment"`
	GenerationExpression   string         `json:"generation_expression"`
	SrsId                  sql.NullInt64  `json:"srs_id"`
}

// MysqlTableColumns 查询表字段信息
func MysqlTableColumns(dbConn *sql.DB, tableName string) ([]*MysqlColumn, error) {
	rows, err := dbConn.Query(`
		SELECT * FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE()
		  AND TABLE_NAME = ?
	`, tableName)
	if err != nil {
		return nil, fmt.Errorf("查询字段空值属性失败: %w", err)
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			fmt.Println("关闭查询结果集失败: ", err)
		}
	}(rows)

	columns := make([]*MysqlColumn, 0)
	for rows.Next() {
		var (
			tableCatalog           string
			tableSchema            string
			tableName              string
			columnName             string
			ordinalPosition        int
			columnDefault          sql.NullString
			isNullable             string
			dataType               string
			characterMaximumLength sql.NullInt64
			characterOctetLength   sql.NullInt64
			numericPrecision       sql.NullInt64
			numericScale           sql.NullInt64
			dateTimePrecision      sql.NullInt64
			characterSetName       sql.NullString
			collationName          sql.NullString
			columnType             string
			columnKey              string
			extra                  string
			privileges             string
			columnComment          string
			generationExpression   string
			srsId                  sql.NullInt64
		)

		if err := rows.Scan(
			&tableCatalog,
			&tableSchema,
			&tableName,
			&columnName,
			&ordinalPosition,
			&columnDefault,
			&isNullable,
			&dataType,
			&characterMaximumLength,
			&characterOctetLength,
			&numericPrecision,
			&numericScale,
			&dateTimePrecision,
			&characterSetName,
			&collationName,
			&columnType,
			&columnKey,
			&extra,
			&privileges,
			&columnComment,
			&generationExpression,
			&srsId,
		); err != nil {
			return nil, err
		}

		oneColumn := &MysqlColumn{
			TableCatalog:           tableCatalog,
			TableSchema:            tableSchema,
			TableName:              tableName,
			ColumnName:             columnName,
			OrdinalPosition:        ordinalPosition,
			ColumnDefault:          columnDefault,
			IsNullable:             isNullable == "YES",
			DataType:               dataType,
			CharacterMaximumLength: characterMaximumLength,
			CharacterOctetLength:   characterOctetLength,
			NumericPrecision:       numericPrecision,
			NumericScale:           numericScale,
			DateTimePrecision:      dateTimePrecision,
			CharacterSetName:       characterSetName,
			CollationName:          collationName,
			ColumnType:             columnType,
			ColumnKey:              columnKey,
			Extra:                  extra,
			Privileges:             privileges,
			ColumnComment:          columnComment,
			GenerationExpression:   generationExpression,
			SrsId:                  srsId,
		}
		columns = append(columns, oneColumn)
	}
	return columns, nil
}

// mysqlColumnDefaultValue 获取字段默认值
func mysqlColumnDefaultValue(oneColumn *MysqlColumn) any {
	if oneColumn.ColumnDefault.Valid { // 有默认值，先取默认值
		if oneColumn.ColumnDefault.String == "CURRENT_TIMESTAMP" {
			columnType := strings.ToUpper(oneColumn.ColumnType)
			if ok, _ := cond.Contains([]string{schemas.Date, schemas.Time, schemas.TimeStamp, schemas.DateTime}, columnType); ok {
				return time.Now()
			}
		}
		return oneColumn.ColumnDefault.String
	}

	if oneColumn.IsNullable { //是否允许为空，返回空值
		return nil
	}

	timeStr, ok := getDefaultTime(oneColumn)
	if ok {
		return timeStr
	}

	dataType := strings.ToUpper(oneColumn.DataType)

	switch dataType {
	case schemas.Int, schemas.BigInt, schemas.SmallInt, schemas.TinyInt:
		return 0
	case schemas.Float, schemas.Double, schemas.Decimal:
		return 0.0
	case schemas.Char, schemas.Varchar, schemas.Text, schemas.LongText:
		return ""
	case schemas.Blob, schemas.VarBinary:
		return ""
	case schemas.Boolean, schemas.Bool:
		return 0
	default:
		return "" // 兜底处理
	}
}

func getDefaultTime(oneColumn *MysqlColumn) (string, bool) {
	dataType := strings.ToUpper(oneColumn.DataType)
	if dataType == schemas.Date {
		return "1000-01-01", true // MySQL 支持的最小合法日期
	}
	if dataType == schemas.Time {
		return "00:00:00", true
	}
	if dataType == schemas.TimeStamp {
		return "1970-01-01 00:00:01", true
	}
	if dataType == schemas.DateTime {
		return "1000-01-01 00:00:00", true
	}
	return "", false
}

// MysqlSchema 获取当前数据库名
func MysqlSchema(db *sql.DB) (string, error) {
	var schema string
	err := db.QueryRow("SELECT SCHEMA()").Scan(&schema)
	return schema, err
}

// MysqlColumnValidValue 获取mysql合法字符
func MysqlColumnValidValue(v any, oneColumn *MysqlColumn) any {
	if v == nil {
		return mysqlColumnDefaultValue(oneColumn)
	}
	//对时间进行特殊处理
	defaultTime, ok := getDefaultTime(oneColumn)
	if ok {
		timeDay := conv.String(v)
		if strings.HasPrefix(timeDay, "000") ||
			timeDay == "" {
			if oneColumn.IsNullable {
				return nil
			} else {
				return defaultTime
			}
		}
		return v
	}

	return v
}

// MysqlColumnAutoIncrement 查询自增列及最大值
func MysqlColumnAutoIncrement(dbConn *sql.DB, tableName string) (columnName string, maxValue int64, err error) {
	// 查询自增列名
	err = dbConn.QueryRow(`
		SELECT COLUMN_NAME 
		FROM information_schema.COLUMNS 
		WHERE TABLE_SCHEMA = DATABASE() 
		  AND TABLE_NAME = ? 
		  AND EXTRA LIKE '%auto_increment%';
	`, tableName).Scan(&columnName)

	if err != nil {
		return "", 0, fmt.Errorf("查询自增列失败: %w", err)
	}

	if columnName == "" {
		return "", 0, fmt.Errorf("未找到自增列")
	}

	var autoInc sql.NullInt64
	err = dbConn.QueryRow(`
		SELECT AUTO_INCREMENT
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = DATABASE()
		  AND TABLE_NAME = ?;
	`, tableName).Scan(&autoInc)

	if err != nil {
		return columnName, 0, fmt.Errorf("查询自增列最大值失败: %w", err)
	}

	return columnName, autoInc.Int64, nil
}

// MysqlColumnRowsToMaps 将sql.Rows转换为[]map[string]any
func MysqlColumnRowsToMaps(rows *sql.Rows) ([]map[string]any, error) {
	// 获取列名
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	columnsType, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	// 创建结果切片
	var result []map[string]any

	// 创建与列数相同长度的interface{}切片，用于存储每一行的值
	values := make([]any, len(columns))
	// 创建指向values中每个元素的指针切片
	valuePtr := make([]any, len(columns))

	// 遍历结果集的每一行
	for rows.Next() {
		// 初始化指针切片，每个元素指向values中对应位置的指针
		for i := range columns {
			valuePtr[i] = &values[i]
		}

		// 扫描当前行的数据到values中
		if err := rows.Scan(valuePtr...); err != nil {
			return nil, err
		}

		// 创建当前行的map
		row := make(map[string]any)
		for i, colName := range columns {
			// 获取值的指针
			val := values[i]
			// 处理空值
			if val == nil {
				row[colName] = nil
				continue
			}

			valType := columnsType[i]

			// 处理不同类型的值
			switch v := val.(type) {
			case []byte:
				// 字节切片通常是字符串或二进制数据
				typeName := valType.DatabaseTypeName()

				if ok, _ := cond.Contains([]string{schemas.Char, schemas.Varchar, schemas.Text, schemas.LongText}, typeName); ok {
					row[colName] = string(v)
				} else if ok, _ = cond.Contains([]string{schemas.Bit, schemas.UnsignedBit}, typeName); ok {
					if len(v) == 1 {
						isAscII := isPrintableASCII(v)
						if isAscII {
							row[colName] = string(v)
						} else {
							row[colName] = bitToBool(v)
						}
					} else {
						row[colName] = bitToInt(v)
					}
				} else {
					row[colName] = string(v)
				}

			case time.Time:
				// 时间类型，可根据需要格式化为字符串
				row[colName] = v.Format(time.RFC3339)
			default:
				// 其他类型直接使用
				row[colName] = v
			}
		}

		// 将当前行添加到结果中
		result = append(result, row)
	}

	// 检查遍历过程中是否有错误
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// 处理 BIT(1) 类型（布尔值）
func bitToBool(b []byte) bool {
	if len(b) == 0 {
		return false
	}
	return b[0] != 0
}

// 处理多字节 BIT 类型（如 BIT(8), BIT(16) 等）
func bitToInt(b []byte) uint64 {
	var result uint64
	for _, v := range b {
		result = (result << 8) | uint64(v)
	}
	return result
}

func isPrintableASCII(data []uint8) bool {
	for _, b := range data {
		if b < 32 || b > 126 {
			return false
		}
	}
	return true
}
