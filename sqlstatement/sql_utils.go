package sqlstatement

import (
	"database/sql"
	"github.com/magic-lib/go-plat-utils/conv"
	"github.com/magic-lib/go-plat-utils/utils"
	"reflect"
	"strings"
	"time"
)

// StructToColumnsAndValues 将结构体转换为 SQL 对应的列名列表和值列表
// convertType 默认的转换方式，如果没有获取到tag，则默认的转换方式。
// 支持的类型有：snake 蛇形命名，camel 驼峰命名，lower 小写命名, upper 大写命名
func StructToColumnsAndValues(in any, convertType string, tagNames ...string) (tableName string, columnsMap map[string]any, err error) {
	structName, columnMap, err := utils.GetStructInfoByTag(in, func(s string) string {
		return convertToByType(s, convertType)
	}, tagNames...)

	if err != nil {
		return "", nil, err
	}

	tableName = convertToByType(structName, convertType)

	columnsMap = make(map[string]any)
	//需要过滤出nil的项目
	for key, value := range columnMap {
		if value == nil {
			continue
		}
		ti := reflect.TypeOf(value)
		if ti.Kind() == reflect.Ptr {
			vi := reflect.ValueOf(value)
			if vi.IsNil() {
				continue
			}
		}
		if ti.Kind() == reflect.Struct {
			if nullVal, ok := value.(sql.NullTime); ok {
				columnsMap[key] = conv.String(nullVal.Time)
				continue
			}
			if nullVal, ok := value.(sql.NullString); ok {
				columnsMap[key] = nullVal.String
				continue
			}
			if nullVal, ok := value.(sql.NullBool); ok {
				columnsMap[key] = nullVal.Bool
				continue
			}
		}

		columnsMap[key] = value
	}

	return tableName, columnsMap, nil
}

func convertToByType(in string, convertType string) string {
	if convertType == "snake" {
		return utils.ChangeVariableName(in, "lower")
	}

	if convertType == "camel" {
		return utils.ChangeVariableName(in, "upper")
	}
	if convertType == "lower" {
		return strings.ToLower(in)
	}
	if convertType == "upper" {
		return strings.ToUpper(in)
	}
	return in
}
func getSliceByMap(columnsMap map[string]any) ([]string, []any) {
	columns := make([]string, 0)
	dataList := make([]any, 0)

	for k, v := range columnsMap {
		columns = append(columns, k)
		dataList = append(dataList, getSqlValues(k, v))
	}
	return columns, dataList
}

func getSqlValues(columnName string, oneValue any) any {
	if t, ok := oneValue.(time.Time); ok {
		return conv.String(t)
	}
	return oneValue
}

// addCodeForColumns 为column添加`符号，避免冲突
func addCodeForColumns(columns []string) []string {
	newColumns := make([]string, 0)
	for _, column := range columns {
		newColumn := addCodeForOneColumn(column)
		newColumns = append(newColumns, newColumn)
	}
	return newColumns
}
func addCodeForOneColumn(column string) string {
	column = strings.ReplaceAll(column, "`", "")
	return "`" + column + "`"
}
