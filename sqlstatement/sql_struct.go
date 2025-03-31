package sqlstatement

import (
	"fmt"
	"github.com/Masterminds/squirrel"
	"github.com/samber/lo"
)

type SqlStruct struct {
	structData                any    //通过这个可以直接获得mysql数据结构
	tableName                 string //表名
	convertTableAndColumnType string
	columnTagName             string
}

type Option func(*SqlStruct)

// NewSqlStruct 新建一个对象
func NewSqlStruct(opts ...Option) *SqlStruct {
	var s = &SqlStruct{
		convertTableAndColumnType: "snake",
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

func (s *SqlStruct) commGetTableNameAndColumns(in any) (string, map[string]any, error) {
	if in == nil {
		return "", nil, fmt.Errorf("please use SetStructData func")
	}

	tagNames := make([]string, 0)
	if s.columnTagName != "" {
		tagNames = append(tagNames, s.columnTagName)
	}
	tableName, columnsMap, err := StructToColumnsAndValues(in, s.convertTableAndColumnType, tagNames...)
	if err != nil {
		return "", nil, err
	}
	if s.tableName != "" {
		tableName = s.tableName
	}

	//设置默认值
	if s.structData == nil {
		s.structData = in
	}

	return tableName, columnsMap, nil
}

// InsertSql 插入的sql语句
func (s *SqlStruct) InsertSql(in any) (string, []any, error) {
	tableName, columnMap, err := s.commGetTableNameAndColumns(in)
	if err != nil {
		return "", nil, err
	}
	columns, values := getSliceByMap(columnMap)
	columns = addCodeForColumns(columns)
	return squirrel.Insert(tableName).Columns(columns...).Values(values...).ToSql()
}

// InsertSqlByMap 插入的sql语句
func (s *SqlStruct) InsertSqlByMap(inMap map[string]any) (string, []any, error) {
	tableName, columnMap, err := s.commGetTableNameAndColumns(s.structData)
	if err != nil {
		return "", nil, err
	}
	columns, _ := getSliceByMap(columnMap)
	st := new(Statement)
	sqlStr, values := st.InsertSql(tableName, columns, inMap)
	return sqlStr, values, nil
}

// DeleteSql 删除的sql语句
func (s *SqlStruct) DeleteSql(whereCondition LogicCondition) (string, []any, error) {
	tableName, _, err := s.commGetTableNameAndColumns(s.structData)
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
	tableName, columnMap, err := s.commGetTableNameAndColumns(s.structData)
	if err != nil {
		return "", nil, err
	}
	columns, _ := getSliceByMap(columnMap)
	st := new(Statement)
	sqlStr, values := st.DeleteSql(tableName, columns, whereMap)
	return sqlStr, values, nil
}

// UpdateSql 修改的sql语句
func (s *SqlStruct) UpdateSql(in any, columns []string, whereCondition LogicCondition) (string, []any, error) {
	tableName, allColumnMap, err := s.commGetTableNameAndColumns(in)
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
	tableName, allColumnMap, err := s.commGetTableNameAndColumns(in)
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
	allColumns, _ := getSliceByMap(allColumnMap)
	sqlStr, values := st.UpdateSql(tableName, allColumns, updateMap, whereMap)
	return sqlStr, values, nil
}

// UpdateSqlWithUpdateMap 更新的sql语句，map里的关系是And关系
func (s *SqlStruct) UpdateSqlWithUpdateMap(updateMap map[string]any, whereMap map[string]any) (string, []any, error) {
	tableName, columnMap, err := s.commGetTableNameAndColumns(s.structData)
	if err != nil {
		return "", nil, err
	}
	allColumns, _ := getSliceByMap(columnMap)
	st := new(Statement)
	sqlStr, values := st.UpdateSql(tableName, allColumns, updateMap, whereMap)
	return sqlStr, values, nil
}

// SelectSql 查询的sql语句
func (s *SqlStruct) SelectSql(selectStr string, whereCondition LogicCondition, offset, limit int) (string, []any, error) {
	tableName, _, err := s.commGetTableNameAndColumns(s.structData)
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
	tableName, columnMap, err := s.commGetTableNameAndColumns(s.structData)
	if err != nil {
		return "", nil, err
	}
	columns, _ := getSliceByMap(columnMap)
	st := new(Statement)
	sqlStr, values := st.SelectSql(tableName, columns, selectStr, whereMap, offset, limit)
	return sqlStr, values, nil
}
