package sqlstatement

import (
	"fmt"
	"github.com/Masterminds/squirrel"
	"github.com/magic-lib/go-plat-utils/cond"
	"github.com/magic-lib/go-plat-utils/conv"
	"github.com/magic-lib/go-plat-utils/utils"
	"github.com/samber/lo"
	"log"
	"reflect"
	"regexp"
	"strings"
)

// Condition 表示单个查询条件
type Condition struct {
	Field    string
	Operator OperatorType
	Value    any
}

// LogicCondition 表示逻辑分组
type LogicCondition struct {
	Conditions []any        // 可以是 Condition 或 LogicCondition
	Operator   OperatorType // "AND" 或 "OR"
}

type OperatorType string

const (
	OperatorAnd          OperatorType = "AND"
	OperatorOr           OperatorType = "OR"
	OperatorLike         OperatorType = "LIKE"
	OperatorNotLike      OperatorType = "NOT LIKE"
	OperatorIn           OperatorType = "IN"
	OperatorNotIn        OperatorType = "NOT IN"
	OperatorIs           OperatorType = "IS"
	OperatorIsNot        OperatorType = "IS NOT"
	OperatorBetween      OperatorType = "BETWEEN"
	OperatorNotBetween   OperatorType = "NOT BETWEEN"
	OperatorLess         OperatorType = "<"
	OperatorLessEqual    OperatorType = "<="
	OperatorGreater      OperatorType = ">"
	OperatorGreaterEqual OperatorType = ">="
	OperatorNotEqual     OperatorType = "!="
	OperatorEqual        OperatorType = "="
)

var (
	operatorList = []OperatorType{
		OperatorLike,
		OperatorNotLike,
		OperatorIn,
		OperatorNotIn,
		OperatorIs,
		OperatorIsNot,
		OperatorBetween,
		OperatorNotBetween,
		OperatorEqual,
		OperatorNotEqual,
		OperatorGreaterEqual,
		OperatorGreater,
		OperatorLessEqual,
		OperatorLess,
	} // 数据库支持的类型
	logicOperatorList = []OperatorType{
		OperatorAnd,
		OperatorOr,
	}
	likeUseReplaceList   = []string{"%", "_"}                          //like需要替换的字符
	likeUseEscapeList    = []string{"/", "&", "#", "@", "^", "$", "!"} //定义可以使用的escape列表
	defaultLogicOperator = OperatorAnd
)

type Statement struct {
}

func (s *Statement) getColumnLikeSql(oldValue string, replaceList []string, escapeList []string) (retValLike string, retEscape string, retSuccess bool) {
	isFind := false
	for _, one := range replaceList {
		if in := strings.IndexAny(oldValue, one); in >= 0 {
			isFind = true
		}
	}

	if !isFind {
		return oldValue, "", true
	}

	oneEscapeStr := ""
	for _, one := range escapeList {
		if in := strings.IndexAny(oldValue, one); in < 0 {
			//不存在，则可作为转义符
			oneEscapeStr = one
			break
		}
	}

	if oneEscapeStr == "" {
		return oldValue, "", false
	}

	for _, one := range replaceList {
		oldValue = strings.ReplaceAll(oldValue, one, oneEscapeStr+one)
	}

	return oldValue, oneEscapeStr, true
}

// GetSqlColumnForLike 获取列名转义sql
func (s *Statement) GetSqlColumnForLike(oldValue string) (retValLike string, retParam string) {
	newValue, escape, retTrue := s.getColumnLikeSql(oldValue, likeUseReplaceList, likeUseEscapeList)
	if retTrue {
		if escape != "" {
			return "? ESCAPE '" + escape + "'", newValue
		}
	}

	return "?", newValue
}

// GenerateWhereClauseByMap 通过Map获取where语句
func (s *Statement) GenerateWhereClauseByMap(whereMap map[string]any) (string, []any) {
	oneLogicCondition := LogicCondition{
		Conditions: make([]any, 0),
		Operator:   defaultLogicOperator,
	}
	for key, val := range whereMap {
		oneCondition := Condition{
			Field:    key,
			Operator: s.getFieldOperator(val),
			Value:    val,
		}

		s := reflect.ValueOf(val)
		if one, ok := s.Interface().(Condition); ok {
			oneCondition.Operator = one.Operator
			oneCondition.Value = one.Value
		}
		oneLogicCondition.Conditions = append(oneLogicCondition.Conditions, oneCondition)
	}
	return s.GenerateWhereClause(oneLogicCondition)
}

func (s *Statement) getFieldOperator(val any) OperatorType {
	if reflect.TypeOf(val).Kind() == reflect.Slice {
		return OperatorIn
	}
	return OperatorEqual
}

func (s *Statement) ConvOperator(op any) OperatorType {
	opStr := conv.String(op)
	opStr = strings.TrimSpace(opStr)
	opStr = strings.ToUpper(opStr)
	return OperatorType(opStr)
}
func (s *Statement) getLogicOperator(op any) OperatorType {
	opStr := conv.String(op)
	if opStr != "" {
		opType := s.ConvOperator(opStr)
		if ok, _ := cond.Contains(logicOperatorList, opType); ok {
			return opType
		}
	}
	return defaultLogicOperator
}

// GenerateWhereClause 生成 WHERE 语句
func (s *Statement) GenerateWhereClause(group LogicCondition) (string, []any) {
	group.Operator = s.getLogicOperator(group.Operator)

	var parts []string
	dataList := make([]any, 0)
	for _, condTemp := range group.Conditions {
		switch c := condTemp.(type) {
		case Condition:
			sqlStr, tempDataList, err := s.generateWhereFromCondition(c)
			if err != nil {
				continue
			}
			if sqlStr != "" {
				parts = append(parts, fmt.Sprintf("(%s)", sqlStr))
				dataList = append(dataList, tempDataList...)
			}
			continue
		case LogicCondition:
			sqlStr, tempDataList := s.GenerateWhereClause(c)
			if sqlStr != "" {
				parts = append(parts, fmt.Sprintf("(%s)", sqlStr))
				dataList = append(dataList, tempDataList...)
			}
			continue
		}
	}
	if len(parts) == 0 {
		return "", dataList
	}
	return strings.Join(parts, fmt.Sprintf(" %s ", group.Operator)), dataList
}

// isValidFieldName 是不是合法的字段名
func isValidFieldName(name string) bool {
	pattern := `^[a-zA-Z_][a-zA-Z0-9_]*$`
	match, _ := regexp.MatchString(pattern, name)
	return match
}

// generateWhereClause 生成 WHERE 语句
func (s *Statement) generateWhereFromCondition(con Condition) (string, []any, error) {
	con.Operator = s.ConvOperator(con.Operator)

	if con.Field == "" {
		//如果value是一个复杂结构
		if con.Value != nil {
			if val, ok := con.Value.(squirrel.Sqlizer); ok {
				sqlStr, tempDataList, err := val.ToSql()
				if err == nil {
					return sqlStr, tempDataList, nil
				}
			}
		}
		return "", []any{}, nil
	}

	// 如果Field不含空格表示是字段，则加上`
	fieldStr := con.Field
	if !isValidFieldName(fieldStr) {
		if con.Value == nil {
			return fieldStr, []any{}, nil
		}
	} else {
		fieldStr = fmt.Sprintf("`%s`", con.Field)
		if con.Value == nil {
			return "", []any{}, nil
		}
	}

	// 判断是否为null字段
	if con.Operator == OperatorIs || con.Operator == OperatorIsNot {
		return fmt.Sprintf("%s %s NULL", fieldStr, con.Operator), []any{}, nil
	}
	if con.Operator == OperatorBetween || con.Operator == OperatorNotBetween {
		//时间是前闭后开，其它都是前闭后闭
		paramList, dataList := s.buildFieldList(con.Value, false)
		if len(paramList) != 2 && len(dataList) != 2 {
			return "", []any{}, fmt.Errorf("between param is not 2 value")
		}
		return fmt.Sprintf("%s %s %s", fieldStr, con.Operator, strings.Join(paramList, " AND ")), dataList, nil
	}

	//如果val是数组，则operator只能是in
	if reflect.TypeOf(con.Value).Kind() == reflect.Slice {
		paramList, dataList := s.buildFieldList(con.Value, true)
		if len(dataList) > 0 {
			//只能为IN，NOT IN
			opt := OperatorIn
			if con.Operator == OperatorNotIn {
				opt = con.Operator
			}
			return fmt.Sprintf("%s %s (%s)", fieldStr, opt, strings.Join(paramList, ",")), dataList, nil
		}
		return "", []any{}, fmt.Errorf("list is empty")
	}

	if con.Operator == "" || con.Operator == "==" {
		con.Operator = OperatorEqual
	}

	//必须是支持的类型，乱传不支持的类型则报错，不阻止了
	if ok, _ := cond.Contains(operatorList, con.Operator); !ok {
		log.Printf("Error operator not support: %s", con.Operator)
		//return "", []any{}, fmt.Errorf("operator not support: %s", con.Operator)
	}

	//如果是某一个函数,子查询，则直接返回
	if val, ok := con.Value.(squirrel.Sqlizer); ok {
		sqlStr, tempDataList, err := val.ToSql()
		if err == nil {
			return fmt.Sprintf("%s %s (%s)", fieldStr, con.Operator, sqlStr), tempDataList, nil
		}
	}

	return fmt.Sprintf("%s %s ?", fieldStr, con.Operator), []any{con.Value}, nil
}

func (s *Statement) buildFieldList(value any, isUniq bool) ([]string, []any) {
	vKind := reflect.TypeOf(value).Kind()
	if vKind != reflect.Slice && vKind != reflect.Array {
		return []string{}, []any{}
	}
	vValue := reflect.ValueOf(value)
	//需要去重处理
	paramList := make([]string, 0)
	dataList := make([]any, 0)
	onlyArray := make([]string, 0)
	for i := 0; i < vValue.Len(); i++ {
		ele := vValue.Index(i).Interface()
		tempOne := conv.String(ele)
		if isUniq {
			if ret, _ := cond.Contains(onlyArray, tempOne); !ret {
				onlyArray = utils.AppendUniq(onlyArray, tempOne)
				paramList = append(paramList, "?")
				dataList = append(dataList, ele)
			}
		} else {
			paramList = append(paramList, "?")
			dataList = append(dataList, ele)
		}
	}
	return paramList, dataList
}

// buildFieldNames 需要将 `name` 转为 name
func (s *Statement) buildFieldNames(canUpdateFieldNames []string) []string {
	canUpdateFieldNamesTemp := make([]string, 0)
	lo.ForEach(canUpdateFieldNames, func(item string, index int) {
		canUpdateFieldNamesTemp = append(canUpdateFieldNamesTemp, strings.ReplaceAll(item, "`", ""))
	})
	return canUpdateFieldNamesTemp
}
func (s *Statement) buildOneFieldName(oneField string) string {
	oneField = strings.ReplaceAll(oneField, "`", "")
	return oneField
}

// getColumnListAndDataList 获取数据库的字段列表与数据
func (s *Statement) getColumnListAndDataList(fieldNames []string, columnMap map[string]any) ([]string, []any) {
	fieldNamesTemp := s.buildFieldNames(fieldNames)

	columnList := make([]string, 0)
	// 必须检查是数据库的字段名，避免传错的名字
	columns := lo.Keys(columnMap)
	lo.ForEach(columns, func(column string, index int) {
		if lo.IndexOf(fieldNamesTemp, column) >= 0 {
			columnList = append(columnList, column)
		}
	})
	columnDataList := make([]any, 0, len(columnList))

	if len(columnList) == 0 {
		return columnList, columnDataList
	}
	lo.ForEach(columnList, func(column string, index int) {
		columnDataList = append(columnDataList, columnMap[column])
	})
	return columnList, columnDataList
}

// InsertSql 插入的sql语句
func (s *Statement) InsertSql(tableName string, allColumns []string, insertMap map[string]any) (string, []any) {
	allColumns = s.buildFieldNames(allColumns)
	columnList, columnDataList := s.getColumnListAndDataList(allColumns, insertMap)
	if len(columnList) == 0 {
		return "", columnDataList
	}
	tableName = addCodeForOneColumn(tableName)
	columnList = lo.Map(columnList, func(item string, index int) string {
		return addCodeForOneColumn(item)
	})
	query := fmt.Sprintf("INSERT INTO %s SET %s", tableName, strings.Join(columnList, "=?,")+"=?")
	return query, columnDataList
}

// UpdateSql 更新的sql语句
func (s *Statement) UpdateSql(tableName string, allColumns []string, updateMap map[string]any, whereMap map[string]any) (string, []any) {
	allColumns = s.buildFieldNames(allColumns)

	columnList, columnDataList := s.getColumnListAndDataList(allColumns, updateMap)
	if len(columnList) == 0 {
		return "", columnDataList
	}

	//过滤key
	whereNewMap := make(map[string]any)
	for k, v := range whereMap {
		if lo.IndexOf(allColumns, k) >= 0 {
			whereNewMap[k] = v
		}
	}

	for i, column := range columnList {
		columnList[i] = addCodeForOneColumn(column)
	}

	tableName = addCodeForOneColumn(tableName)
	columnList = lo.Map(columnList, func(item string, index int) string {
		return addCodeForOneColumn(item)
	})

	whereString, whereDataList := s.GenerateWhereClauseByMap(whereNewMap)
	if len(whereString) == 0 {
		//没有where语句
		query := fmt.Sprintf("UPDATE %s SET %s", tableName, strings.Join(columnList, "=?,")+"=?")
		return query, columnDataList
	}
	columnDataList = append(columnDataList, whereDataList...)
	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s", tableName, strings.Join(columnList, "=?,")+"=?", whereString)
	return query, columnDataList
}

// UpdateSqlByWhereCondition 更新的sql语句
func (s *Statement) UpdateSqlByWhereCondition(tableName string, allColumns []string, updateMap map[string]any, whereCondition LogicCondition) (string, []any) {
	query, updateColumnDataList := s.UpdateSql(tableName, allColumns, updateMap, map[string]any{})
	whereStr, whereDataList := s.GenerateWhereClause(whereCondition)
	if whereStr == "" {
		return query, updateColumnDataList
	}
	query = fmt.Sprintf("%s WHERE %s", query, whereStr)
	updateColumnDataList = append(updateColumnDataList, whereDataList...)
	return query, updateColumnDataList
}

// SelectSql 查询的sql语句
func (s *Statement) SelectSql(tableName string, allColumns []string, selectStr string, whereMap map[string]any, offset, limit int) (string, []any) {
	allColumns = s.buildFieldNames(allColumns)

	if selectStr == "" {
		selectStr = "*"
	} else {
		selectList := strings.Split(selectStr, ",")
		newSelectList := make([]string, 0)
		lo.ForEach(selectList, func(item string, index int) {
			item = strings.TrimSpace(item)
			if lo.IndexOf(allColumns, item) >= 0 {
				item = s.buildOneFieldName(item)
				newSelectList = append(newSelectList, item)
			}
		})
		if len(newSelectList) > 0 {
			selectStr = strings.Join(newSelectList, "`, `")
			selectStr = fmt.Sprintf("`%s`", selectStr)
		} else {
			//表示要查询 count(*) 等，就不用管了
		}
	}

	//过滤key
	whereNewMap := make(map[string]any)
	for k, v := range whereMap {
		if lo.IndexOf(allColumns, k) >= 0 {
			whereNewMap[k] = v
		}
	}
	tableName = addCodeForOneColumn(tableName)

	whereString, whereDataList := s.GenerateWhereClauseByMap(whereNewMap)
	query := fmt.Sprintf("SELECT %s FROM %s", selectStr, tableName)
	if whereString != "" {
		query = fmt.Sprintf("%s WHERE %s", query, whereString)
	}
	if offset >= 0 && limit > 0 {
		query = fmt.Sprintf("%s LIMIT %d, %d", query, offset, limit)
	}

	return query, whereDataList
}

// SelectSqlByWhereCondition 查询的sql语句
func (s *Statement) SelectSqlByWhereCondition(tableName string, allColumns []string, selectStr string, whereCondition LogicCondition, offset, num int) (string, []any) {
	query, selectDataList := s.SelectSql(tableName, allColumns, selectStr, map[string]any{}, 0, 0)
	whereStr, whereDataList := s.GenerateWhereClause(whereCondition)
	if whereStr == "" {
		return query, selectDataList
	}
	query = fmt.Sprintf("%s WHERE %s", query, whereStr)
	if offset >= 0 && num > 0 {
		query = fmt.Sprintf("%s LIMIT %d, %d", query, offset, num)
	}
	selectDataList = append(selectDataList, whereDataList...)
	return query, selectDataList
}

// DeleteSql 删除的sql语句
func (s *Statement) DeleteSql(tableName string, allColumns []string, whereMap map[string]any) (string, []any) {
	allColumns = s.buildFieldNames(allColumns)

	//过滤key
	whereNewMap := make(map[string]any)
	for k, v := range whereMap {
		if lo.IndexOf(allColumns, k) >= 0 {
			whereNewMap[k] = v
		}
	}
	whereString, whereDataList := s.GenerateWhereClauseByMap(whereNewMap)
	tableName = addCodeForOneColumn(tableName)
	query := fmt.Sprintf("DELETE FROM %s", tableName)
	if whereString != "" {
		query = fmt.Sprintf("%s WHERE %s", query, whereString)
	}
	return query, whereDataList
}

// DeleteSqlByWhereCondition 删除的sql语句
func (s *Statement) DeleteSqlByWhereCondition(tableName string, allColumns []string, whereCondition LogicCondition) (string, []any) {
	query, deleteDataList := s.DeleteSql(tableName, allColumns, map[string]any{})
	whereStr, whereDataList := s.GenerateWhereClause(whereCondition)
	if whereStr == "" {
		return query, deleteDataList
	}
	query = fmt.Sprintf("%s WHERE %s", query, whereStr)
	deleteDataList = append(deleteDataList, whereDataList...)
	return query, deleteDataList
}
