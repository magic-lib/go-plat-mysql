package etl

import (
	"database/sql"
	"fmt"
	"github.com/Masterminds/squirrel"
	_ "github.com/go-sql-driver/mysql"
	"github.com/magic-lib/go-plat-mysql/sqlstatement"
	"github.com/magic-lib/go-plat-utils/conv"
	"github.com/samber/lo"
	"time"
)

type mysqlLogger struct {
	logTableName string
	dbConn       *sql.DB
}

// logRecord 定义请求记录结构体
type logRecord struct {
	Id           int64     `json:"id"`
	DatabaseName string    `json:"database_name"`
	TableName    string    `json:"table_name"`
	PageNow      int       `json:"page_now"`
	PageSize     int       `json:"page_size"`
	SucNum       int       `json:"suc_num"`
	Extend       string    `json:"extend,omitempty"`
	Errors       string    `json:"errors,omitempty"`
	Status       string    `json:"status"`
	CreateTime   time.Time `json:"create_time"`
	UpdateTime   time.Time `json:"update_time"`
}

func newMysqlLogger(db *sql.DB, logTableName string) (*mysqlLogger, error) {
	if db == nil {
		return nil, fmt.Errorf("数据库连接不能为空")
	}
	if logTableName == "" {
		logTableName = "import_log_table"
	}
	logs := &mysqlLogger{
		dbConn:       db,
		logTableName: logTableName,
	}
	err := logs.createLogTable()
	if err != nil {
		return nil, err
	}
	return logs, nil
}

// 创建请求记录表
func (m *mysqlLogger) createLogTable() error {
	creatSql := fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s (
            id bigint AUTO_INCREMENT PRIMARY KEY,
            database_name VARCHAR(50) NOT NULL,
            table_name VARCHAR(50) NOT NULL,
            page_now INT DEFAULT 1,
            page_size INT DEFAULT 0,
            suc_num INT DEFAULT 0,
            extend TEXT,
            errors TEXT,
            status VARCHAR(20) DEFAULT '',
            create_time DATETIME,
            update_time DATETIME
        )
    `, m.logTableName)
	_, err := m.dbConn.Exec(creatSql)
	if err != nil {
		return err
	}
	return nil
}

func (m *mysqlLogger) insertLogRecord(r *logRecord) (int64, error) {
	_, allColumns, err := sqlstatement.StructToColumnsAndValues(logRecord{
		TableName:  r.TableName,
		PageNow:    r.PageNow,
		PageSize:   r.PageSize,
		Errors:     r.Errors,
		Extend:     r.Extend,
		Status:     "start",
		CreateTime: time.Now(),
		UpdateTime: time.Now(),
	}, "snake")
	if err != nil {
		return 0, err
	}

	columns := make([]string, 0)
	values := make([]any, 0)
	for name, col := range allColumns {
		if name == "id" {
			continue
		}
		if name == "database_name" {
			col = squirrel.Expr("DATABASE()")
		}
		columns = append(columns, name)
		values = append(values, col)
	}

	insertSql, data, err := squirrel.Insert(m.logTableName).Columns(columns...).Values(values...).ToSql()
	if err != nil {
		return 0, err
	}
	result, err := m.dbConn.Exec(insertSql, data...)
	if err != nil {
		return 0, err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}
	return id, nil
}
func (m *mysqlLogger) successLogRecord(id int64, sucNum int, remark string) error {
	sqlBuilder := sqlstatement.NewSqlStruct(
		sqlstatement.SetTableName(m.logTableName),
		sqlstatement.SetStructData(logRecord{}))
	updateSql, data, err := sqlBuilder.UpdateSqlWithUpdateMap(map[string]any{
		"status":      "success",
		"suc_num":     sucNum,
		"extend":      remark,
		"update_time": time.Now(),
	}, map[string]any{
		"id": id,
	})

	if err != nil {
		return err
	}
	_, err = m.dbConn.Exec(updateSql, data...)
	if err != nil {
		return err
	}
	return nil
}
func (m *mysqlLogger) getCurrentMaxPageNow(r *logRecord, pageStart, pageEnd int) (int, error) {
	selectSql := fmt.Sprintf(`SELECT MAX(page_now) AS page_now FROM %s where database_name=DATABASE() AND table_name='%s' AND status = 'success' AND page_size=%d AND page_now>=%d`, m.logTableName, r.TableName, r.PageSize, pageStart)
	if pageEnd > 0 {
		selectSql = fmt.Sprintf(selectSql+` AND page_now<=%d`, pageEnd)
	}

	queryService, err := newMysqlQuery(m.dbConn, 1, 0, 0)
	if err != nil {
		return 0, err
	}
	queryService.SqlQuery = selectSql

	mapList, err := queryService.fetchDataList()
	if err != nil {
		return 0, err
	}
	if len(mapList) == 0 {
		return 0, nil
	}
	if mapList[0]["page_now"] == nil {
		//表示没有记录，没有开始
		if pageStart >= 1 {
			return pageStart - 1, nil
		}
		return 0, nil
	}
	pageNow, ok := conv.Int(mapList[0]["page_now"])
	if ok {
		return pageNow, nil
	}
	return 0, nil
}
func (m *mysqlLogger) getAllPageNowErrorLogList(tableName string, pageSize int, pageStart, pageEnd int) ([]int, error) {
	selectSql := fmt.Sprintf(`SELECT * FROM %s where database_name=DATABASE() AND table_name='%s' AND page_size=%d AND page_now>=%d`, m.logTableName, tableName, pageSize, pageStart)
	if pageEnd > 0 {
		selectSql = fmt.Sprintf(selectSql+` AND page_now<=%d`, pageEnd)
	}
	selectSql = fmt.Sprintf(selectSql + ` ORDER BY page_now ASC`)

	queryService, err := newMysqlQuery(m.dbConn, 1, 0, 0)
	if err != nil {
		return nil, err
	}
	queryService.SqlQuery = selectSql

	mapList, err := queryService.fetchDataList()
	if err != nil {
		return nil, err
	}

	retMapList := make([]*logRecord, 0)

	if len(mapList) == 0 {
		return []int{}, nil
	}
	err = conv.Unmarshal(mapList, &retMapList)
	if err != nil {
		return nil, err
	}

	errPageNowList := make([]int, 0)
	for i := pageStart; i <= pageEnd; i++ {
		pageSuccessData := lo.FindOrElse(retMapList, nil, func(one *logRecord) bool {
			if one.PageNow == i && one.Status == "success" {
				return true
			}
			return false
		})
		if pageSuccessData == nil {
			errPageNowList = append(errPageNowList, i)
		}
	}
	return errPageNowList, nil
}

func (m *mysqlLogger) failureLogRecord(id int64, r *logRecord) error {
	sqlBuilder := sqlstatement.NewSqlStruct(
		sqlstatement.SetTableName(m.logTableName),
		sqlstatement.SetStructData(logRecord{}))
	updateSql, data, err := sqlBuilder.UpdateSqlWithUpdateMap(map[string]any{
		"suc_num":     r.SucNum,
		"extend":      r.Extend,
		"errors":      r.Errors,
		"status":      "failure",
		"update_time": time.Now(),
	}, map[string]any{
		"id": id,
	})

	if err != nil {
		return err
	}
	_, err = m.dbConn.Exec(updateSql, data...)
	if err != nil {
		return err
	}
	return nil
}
