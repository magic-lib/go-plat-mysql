package etl

import (
	"database/sql"
	"fmt"
	"github.com/Masterminds/squirrel"
	"github.com/magic-lib/go-plat-mysql/sqlcomm"
	"github.com/magic-lib/go-plat-utils/templates"
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
	var page *httputil.PageModel
	if m.page.PageSize > 0 {
		page = &httputil.PageModel{
			PageNow:  m.page.PageNow,
			PageSize: m.page.PageSize,
		}
		page = page.GetPage(page.PageSize)
	}

	if m.TableName != "" {
		sqlBuild := squirrel.Select("*").From(m.TableName)
		if m.PrimaryKey != "" {
			sqlBuild = sqlBuild.OrderBy(m.PrimaryKey)
		}
		if page != nil {
			sqlBuild = sqlBuild.Limit(uint64(page.PageSize)).Offset(uint64(page.PageOffset))
		}
		sqlQuery, sqlParam, _ = sqlBuild.ToSql()
	}

	if m.SqlQuery != "" {
		if sqlQuery != "" {
			newSqlQuery := m.SqlQuery
			queryData := map[string]any{
				m.TableName: fmt.Sprintf("(%s) AS %s", sqlQuery, m.TableName),
			}
			if page != nil {
				queryData["offset"] = page.PageOffset
				queryData["limit"] = page.PageSize
			}

			//匹配了分页查询
			newSqlQueryTemp, err := templates.Template(newSqlQuery, queryData)
			if err == nil && newSqlQueryTemp != "" {
				sqlQuery = newSqlQueryTemp
			}
		} else {
			sqlQuery = m.SqlQuery
		}
	}

	if sqlQuery == "" {
		return nil, fmt.Errorf("查询语句不能为空")
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
