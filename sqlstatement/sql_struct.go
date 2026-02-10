package sqlstatement

import (
	"database/sql"
	"fmt"
	"github.com/Masterminds/squirrel"
	"github.com/magic-lib/go-plat-utils/cond"
	"github.com/magic-lib/go-plat-utils/conv"
	"github.com/magic-lib/go-plat-utils/utils"
	"github.com/samber/lo"
	"strings"
	"time"
)

type SqlStruct struct {
	structData                any           //通过这个可以直接获得mysql数据结构
	tableSchema               string        //库名
	tableName                 string        //表名
	columnList                []*ColumnInfo //表字段信息
	convertTableAndColumnType utils.VariableType
	columnTagName             string
}

type Option func(*SqlStruct)

// NewSqlStruct 新建一个对象
func NewSqlStruct(opts ...Option) *SqlStruct {
	var s = &SqlStruct{
		convertTableAndColumnType: utils.Snake,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// SetTableName 设置表名
func SetTableName(tableName string) Option {
	return func(s *SqlStruct) {
		s.tableName = tableName
	}
}

// SetColumnTagName 设置获取字段的Tag名
func SetColumnTagName(tag string) Option {
	return func(s *SqlStruct) {
		s.columnTagName = tag
	}
}

// SetStructData 设置获取字段的Tag名，这个设置表明后续会操作这一个表
func SetStructData(d any) Option {
	return func(s *SqlStruct) {
		s.structData = d
	}
}

// SetColumnList 设置获取字段的类型
func SetColumnList(db *sql.DB, tableName string) Option {
	return func(s *SqlStruct) {
		if db == nil {
			return
		}
		if s.tableName != "" {
			tableName = s.tableName
		}

		newTableSchema, newTableName, columns, err := getTableColumns(db, s.tableSchema, tableName)
		if err == nil && len(columns) > 0 {
			s.columnList = columns
			if newTableSchema != "" {
				s.tableSchema = newTableSchema
			}
			if newTableName != "" {
				s.tableName = newTableName
			}
		}
	}
}

func (s *SqlStruct) getColumnData(columnName string) *ColumnInfo {
	if len(s.columnList) == 0 || columnName == "" {
		return nil
	}
	columnData, ok := lo.Find(s.columnList, func(item *ColumnInfo) bool {
		if item.ColumnName == columnName {
			return true
		}
		return false
	})
	if ok && columnData != nil {
		return columnData
	}
	return nil
}

// buildMapForInsertOrUpdate 处理insert和update 默认处理json类型的问题
func (s *SqlStruct) buildMapForInsertOrUpdate(columnsMap map[string]any) map[string]any {
	for k, v := range columnsMap {
		columnData := s.getColumnData(k)
		if columnData == nil {
			continue
		}
		dataType := strings.ToLower(columnData.DataType)
		if dataType == "json" {
			if conv.String(v) == "" {
				columnsMap[k] = s.getDefaultValutForJson(columnData)
			}
			continue
		}
		if dataType == "datetime" {
			columnsMap[k] = s.getDefaultValutForDateTime(v, columnData)
			continue
		}
	}
	return columnsMap
}

func (s *SqlStruct) getDefaultValutForJson(columnData *ColumnInfo) string {
	return "{}"
}
func (s *SqlStruct) getDefaultValutForDateTime(v any, columnData *ColumnInfo) any {
	shouldUseCurrentTime := false
	if columnData.ColumnDefault.Valid && columnData.ColumnDefault.String == "CURRENT_TIMESTAMP" {
		shouldUseCurrentTime = true
	}
	useNow := false
	vStr := conv.String(v)
	if columnData.IsNullable {
		if !cond.IsTime(vStr) {
			if shouldUseCurrentTime {
				useNow = true
			}
		}
	} else {
		if !cond.IsTime(vStr) {
			useNow = true
		} else {
			oneTime, ok := conv.Time(vStr)
			if !ok || cond.IsTimeEmpty(oneTime) {
				useNow = true
			}
		}
	}

	if useNow {
		return time.Now() //当前时间
	}
	return v
}

func (s *SqlStruct) commGetTableNameAndColumns(in any) (string, []string, map[string]any, error) {
	if in == nil {
		return "", nil, nil, fmt.Errorf("please use SetStructData func")
	}

	tagNames := make([]string, 0)
	if s.columnTagName != "" {
		tagNames = append(tagNames, s.columnTagName)
	}
	tableName, columnsMap, err := StructToColumnsAndValues(in, s.convertTableAndColumnType, tagNames...)
	if err != nil {
		return "", nil, nil, err
	}
	if s.tableName != "" {
		tableName = s.tableName
	}

	//设置默认值
	if s.structData == nil {
		s.structData = in
	}

	_, columnsList, err := StructToColumns(in, s.convertTableAndColumnType, tagNames...)
	if err != nil {
		return "", nil, nil, err
	}

	return tableName, columnsList, columnsMap, nil
}

// InsertSql 插入的sql语句
func (s *SqlStruct) InsertSql(in any) (string, []any, error) {
	if cond.IsArray(in) {
		inList, err := conv.Convert[[]any](in)
		if err == nil {
			return s.insertSqlList(inList)
		}
	}
	tableName, columnList, columnMap, err := s.commGetTableNameAndColumns(in)
	if err != nil {
		return "", nil, err
	}
	columnMap = s.buildMapForInsertOrUpdate(columnMap)
	columns, values := getSliceByMap(columnList, columnMap)
	columns = addCodeForColumns(columns)
	return squirrel.Insert(tableName).Columns(columns...).Values(values...).ToSql()
}

// insertSqlList 插入的sql语句
func (s *SqlStruct) insertSqlList(inList []any) (string, []any, error) {
	if len(inList) == 0 {
		return "", nil, fmt.Errorf("inList is empty")
	}
	tableName, columns, _, err := s.commGetTableNameAndColumns(inList[0])
	if err != nil {
		return "", nil, err
	}
	columns = addCodeForColumns(columns)

	query := squirrel.Insert(tableName).Columns(columns...)
	for _, in := range inList {
		_, columnList, oneColumnMap, err := s.commGetTableNameAndColumns(in)
		if err != nil {
			return "", nil, err
		}
		oneColumnMap = s.buildMapForInsertOrUpdate(oneColumnMap)
		oneColumns, oneValues := getSliceByMap(columnList, oneColumnMap)
		values, err := reOrderValues(oneColumns, oneValues, columns)
		if err != nil {
			return "", nil, err
		}
		query = query.Values(values...)
	}
	return query.ToSql()
}

func reOrderValues(keys []string, values []any, newOrderKeys []string) ([]any, error) {
	if len(keys) != len(values) || len(newOrderKeys) != len(values) {
		return nil, fmt.Errorf("keys and values length not equal")
	}
	keys = addCodeForColumns(keys)
	keyValueMap := make(map[string]any)
	for i := range keys {
		keyValueMap[keys[i]] = values[i]
	}

	var result []any
	for _, key := range newOrderKeys {
		key = addCodeForOneColumn(key)
		if value, exists := keyValueMap[key]; exists {
			result = append(result, value)
		}
	}
	return result, nil
}

// InsertSqlByMap 插入的sql语句
func (s *SqlStruct) InsertSqlByMap(inMap map[string]any) (string, []any, error) {
	tableName, columnList, columnMap, err := s.commGetTableNameAndColumns(s.structData)
	if err != nil {
		return "", nil, err
	}
	columns, _ := getSliceByMap(columnList, columnMap)
	st := new(Statement)
	sqlStr, values := st.InsertSql(tableName, columns, inMap)
	return sqlStr, values, nil
}

// DeleteSql 删除的sql语句
func (s *SqlStruct) DeleteSql(whereCondition LogicCondition) (string, []any, error) {
	tableName, _, _, err := s.commGetTableNameAndColumns(s.structData)
	if err != nil {
		return "", nil, err
	}
	sqlStr, list := new(Statement).GenerateWhereClause(whereCondition)
	sqlState := squirrel.Delete(tableName)
	if sqlStr == "" {
		return sqlState.ToSql()
	}
	return sqlState.Where(sqlStr, list...).ToSql()
}

// DeleteSqlByMap 删除的sql语句，map里的关系是And关系
func (s *SqlStruct) DeleteSqlByMap(whereMap map[string]any) (string, []any, error) {
	tableName, columnList, columnMap, err := s.commGetTableNameAndColumns(s.structData)
	if err != nil {
		return "", nil, err
	}
	columns, _ := getSliceByMap(columnList, columnMap)
	st := new(Statement)
	sqlStr, values := st.DeleteSql(tableName, columns, whereMap)
	return sqlStr, values, nil
}

// UpdateSql 修改的sql语句
func (s *SqlStruct) UpdateSql(in any, columns []string, whereCondition LogicCondition) (string, []any, error) {
	tableName, _, allColumnMap, err := s.commGetTableNameAndColumns(in)
	if err != nil {
		return "", nil, err
	}

	st := new(Statement)
	columns = st.buildFieldNames(columns)

	updateMap := make(map[string]any)
	if len(columns) == 0 {
		updateMap = allColumnMap
	} else {
		lo.ForEach(columns, func(item string, i int) {
			if val, ok := allColumnMap[item]; ok {
				updateMap[item] = val
			}
		})
	}
	updateMap = s.buildMapForInsertOrUpdate(updateMap)
	newUpdateMap := make(map[string]any)
	for k, v := range updateMap {
		newUpdateMap[addCodeForOneColumn(k)] = v
	}

	sqlStr, list := new(Statement).GenerateWhereClause(whereCondition)
	sqlState := squirrel.Update(tableName).SetMap(newUpdateMap)
	if sqlStr == "" {
		return sqlState.ToSql()
	}
	return sqlState.Where(sqlStr, list...).ToSql()
}

// UpdateSqlByMap 修改的sql语句
func (s *SqlStruct) UpdateSqlByMap(in any, columns []string, whereMap map[string]any) (string, []any, error) {
	tableName, columnList, allColumnMap, err := s.commGetTableNameAndColumns(in)
	if err != nil {
		return "", nil, err
	}

	updateMap := make(map[string]any)
	if len(columns) == 0 {
		updateMap = allColumnMap
	} else {
		lo.ForEach(columns, func(item string, i int) {
			if val, ok := allColumnMap[item]; ok {
				updateMap[item] = val
			}
		})
	}

	st := new(Statement)
	allColumns, _ := getSliceByMap(columnList, allColumnMap)
	sqlStr, values := st.UpdateSql(tableName, allColumns, updateMap, whereMap)
	return sqlStr, values, nil
}

// UpdateSqlWithUpdateMap 更新的sql语句，map里的关系是And关系
func (s *SqlStruct) UpdateSqlWithUpdateMap(updateMap map[string]any, whereMap map[string]any) (string, []any, error) {
	tableName, columnList, columnMap, err := s.commGetTableNameAndColumns(s.structData)
	if err != nil {
		return "", nil, err
	}
	allColumns, _ := getSliceByMap(columnList, columnMap)
	st := new(Statement)
	sqlStr, values := st.UpdateSql(tableName, allColumns, updateMap, whereMap)
	return sqlStr, values, nil
}

// SelectSql 查询的sql语句
func (s *SqlStruct) SelectSql(selectStr string, whereCondition LogicCondition, offset, limit int) (string, []any, error) {
	tableName, _, _, err := s.commGetTableNameAndColumns(s.structData)
	if err != nil {
		return "", nil, err
	}

	if selectStr == "" {
		selectStr = "*"
	}

	sqlStr, list := new(Statement).GenerateWhereClause(whereCondition)
	sqlState := squirrel.Select(selectStr).From(tableName)
	if sqlStr != "" {
		sqlState = sqlState.Where(sqlStr, list...)
	}
	if offset >= 0 && limit > 0 {
		sqlState = sqlState.Offset(uint64(offset)).Limit(uint64(limit))
	}
	return sqlState.ToSql()
}

// SelectSqlByMap 查询的sql语句
func (s *SqlStruct) SelectSqlByMap(selectStr string, whereMap map[string]any, offset, limit int) (string, []any, error) {
	tableName, columnList, columnMap, err := s.commGetTableNameAndColumns(s.structData)
	if err != nil {
		return "", nil, err
	}
	columns, _ := getSliceByMap(columnList, columnMap)
	st := new(Statement)
	sqlStr, values := st.SelectSql(tableName, columns, selectStr, whereMap, offset, limit)
	return sqlStr, values, nil
}

// InsertOnDuplicateUpdateSql 插入重复进行更新
func (s *SqlStruct) InsertOnDuplicateUpdateSql(in any, updateMap map[string]any) (string, []any, error) {
	if len(updateMap) == 0 {
		return "", nil, fmt.Errorf("updateMap is empty")
	}

	insertStr, insertDataList, err := s.InsertSql(in)
	if err != nil {
		return "", nil, err
	}
	columnList := lo.Keys(updateMap)
	updateColumnList := make([]string, 0)
	updateDataList := make([]any, 0)
	updateMap = s.buildMapForInsertOrUpdate(updateMap)
	lo.ForEach(columnList, func(item string, i int) {
		if val, ok := updateMap[item]; ok {
			updateColumnList = append(updateColumnList, addCodeForOneColumn(item))
			updateDataList = append(updateDataList, getSqlValues(item, val))
		}
	})

	updateStr := strings.Join(updateColumnList, "=?,") + "=?"

	allSqlStr := fmt.Sprintf("%s ON DUPLICATE KEY UPDATE %s", insertStr, updateStr)
	allDataList := make([]any, 0)
	allDataList = append(allDataList, insertDataList...)
	allDataList = append(allDataList, updateDataList...)
	return allSqlStr, allDataList, nil
}
