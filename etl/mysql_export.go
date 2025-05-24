package etl

import (
	"database/sql"
	"fmt"
	"github.com/Masterminds/squirrel"
	"github.com/magic-lib/go-plat-mysql/sqlcomm"
	"github.com/magic-lib/go-plat-utils/utils/httputil"
	"log"
)

type mysqlExport struct {
	SqlQuery   string
	TableName  string
	PrimaryKey string
	dbConn     *sql.DB
	page       *httputil.PageModel
	pageEnd    int
}

func newMysqlQuery(db *sql.DB, pageNow, pageEnd, pageSize int) (*mysqlExport, error) {
	if db == nil {
		return nil, fmt.Errorf("数据库连接不能为空")
	}

	return &mysqlExport{
		dbConn:  db,
		pageEnd: pageEnd,
		page: &httputil.PageModel{
			PageNow:  pageNow,
			PageSize: pageSize,
		},
	}, nil
}

func (m *mysqlExport) checkFetchDataList() error {
	if m.SqlQuery == "" && m.TableName == "" {
		return fmt.Errorf("必须提供表名或自定义查询")
	}
	if m.PrimaryKey == "" {
		m.PrimaryKey, _, _ = sqlcomm.MysqlColumnAutoIncrement(m.dbConn, m.TableName)
	}
	if m.PrimaryKey == "" && m.page.PageSize > 0 {
		return fmt.Errorf("有分页必须提供排序字段，避免重复查询")
	}
	return nil
}

// fetchDataList 获取数据列表
func (m *mysqlExport) fetchDataList() ([]map[string]any, error) {
	var sqlQuery = ""
	var sqlParam []any
	if m.SqlQuery != "" {
		sqlQuery = m.SqlQuery
	} else {
		sqlBuild := squirrel.Select("*").From(m.TableName)
		if m.PrimaryKey != "" {
			sqlBuild = sqlBuild.OrderBy(m.PrimaryKey)
		}
		sqlQuery, sqlParam, _ = sqlBuild.ToSql()
	}
	if m.page.PageSize > 0 {
		page := &httputil.PageModel{
			PageNow:  m.page.PageNow,
			PageSize: m.page.PageSize,
		}
		page = page.GetPage(page.PageSize)
		sqlQuery = fmt.Sprintf("%s LIMIT %d, %d", sqlQuery, page.PageOffset, page.PageSize)
	}
	rows, err := m.dbConn.Query(sqlQuery, sqlParam...)
	if err != nil {
		return nil, fmt.Errorf("执行查询失败: %w", err)
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			log.Printf("关闭查询结果集失败: %v", err)
		}
	}(rows)

	result, err := sqlcomm.MysqlColumnRowsToMaps(rows)
	if err != nil {
		return nil, fmt.Errorf("将查询结果转换为map失败: %w", err)
	}

	return result, nil
}
