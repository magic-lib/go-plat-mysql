package etl

import (
	"database/sql"
	"fmt"
	"github.com/Masterminds/squirrel"
	"github.com/magic-lib/go-plat-mysql/sqlcomm"
	"github.com/magic-lib/go-plat-utils/templates"
	"github.com/magic-lib/go-plat-utils/utils/httputil"
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
func (m *mysqlExport) fetchDataList(lastId int64) ([]map[string]any, error) {
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
			sqlBuild = sqlBuild.OrderBy(m.PrimaryKey + " ASC")
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
			if lastId > 0 && m.PrimaryKey != "" {
				queryData[m.PrimaryKey] = lastId
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

	return sqlcomm.MysqlQuery(m.dbConn, sqlQuery, sqlParam...)
}
