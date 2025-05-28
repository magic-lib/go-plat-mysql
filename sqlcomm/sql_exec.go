package sqlcomm

import (
	"database/sql"
	"fmt"
)

// MysqlQuery 执行查询语句
func MysqlQuery(dbConn *sql.DB, sqlQuery string, params ...any) ([]map[string]any, error) {
	if sqlQuery == "" {
		return nil, fmt.Errorf("查询语句不能为空")
	}

	rows, err := dbConn.Query(sqlQuery, params...)
	if err != nil {
		return nil, fmt.Errorf("执行查询失败: %w", err)
	}
	defer func(rows *sql.Rows) {
		err = rows.Close()
		if err != nil {
			fmt.Printf("关闭查询结果集失败: %v", err)
		}
	}(rows)

	result, err := MysqlColumnRowsToMaps(rows)
	if err != nil {
		return nil, fmt.Errorf("将查询结果转换为map失败: %w", err)
	}
	return result, nil
}

// MysqlExec 执行变更语句
func MysqlExec(dbConn *sql.DB, sqlQuery string, params ...any) (sql.Result, error) {
	if sqlQuery == "" {
		return nil, fmt.Errorf("执行语句不能为空")
	}
	result, err := dbConn.Exec(sqlQuery, params...)
	if err != nil {
		return nil, fmt.Errorf("sql执行失败: %s, %w", sqlQuery, err)
	}
	return result, nil
}
