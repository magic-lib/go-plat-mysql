package etl

import (
	"database/sql"
	"fmt"
	"github.com/Masterminds/squirrel"
	_ "github.com/go-sql-driver/mysql"
	"github.com/magic-lib/go-plat-mysql/sqlcomm"
	"github.com/magic-lib/go-plat-mysql/sqlstatement"
	"github.com/magic-lib/go-plat-utils/conv"
	"github.com/samber/lo"
	"time"
)

type mysqlLogger struct {
	logTableName string
	dbConn       *sql.DB
}

// MysqlLogRecord 定义请求记录结构体
type MysqlLogRecord struct {
	Id           int64     `json:"id"`
	DatabaseName string    `json:"database_name"`
	TableName    string    `json:"table_name"`
	Method       string    `json:"method"`
	PageNow      int       `json:"page_now"`
	PageSize     int       `json:"page_size"`
	SucNum       int       `json:"suc_num"`
	FromId       string    `json:"from_id"`
	EndId        string    `json:"end_id"`
	SqlWhere     string    `json:"sql_where,omitempty"` //  条件，用于记录pageNow的查询条件模版是什么
	Extend       string    `json:"extend,omitempty"`
	Errors       string    `json:"errors,omitempty"`
	Status       string    `json:"status"`
	CreateTime   time.Time `json:"create_time"`
	UpdateTime   time.Time `json:"update_time"`
}

// NewMysqlLogger 创建日志记录器
func NewMysqlLogger(db *sql.DB, logTableName string) (*mysqlLogger, error) {
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
            method VARCHAR(50) NOT NULL,
            page_now INT DEFAULT 1,
            page_size INT DEFAULT 0,
            suc_num INT DEFAULT 0,
            from_id VARCHAR(100) DEFAULT '',
            end_id VARCHAR(100) DEFAULT '',
            sql_where TEXT,
            extend TEXT,
            errors TEXT,
            status VARCHAR(20) DEFAULT '',
            create_time DATETIME,
            update_time DATETIME
        )
    `, m.logTableName)
	_, err := sqlcomm.MysqlExec(m.dbConn, creatSql)
	if err != nil {
		return err
	}
	return nil
}

// InsertLogRecord 插入请求记录
func (m *mysqlLogger) InsertLogRecord(r *MysqlLogRecord) (int64, error) {
	_, allColumns, err := sqlstatement.StructToColumnsAndValues(MysqlLogRecord{
		TableName:  r.TableName,
		Method:     r.Method,
		PageNow:    r.PageNow,
		PageSize:   r.PageSize,
		Errors:     r.Errors,
		FromId:     r.FromId,
		EndId:      r.EndId,
		SqlWhere:   r.SqlWhere,
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
	result, err := sqlcomm.MysqlExec(m.dbConn, insertSql, data...)
	if err != nil {
		return 0, err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}
	return id, nil
}

// SuccessLogRecord 成功以后的记录
func (m *mysqlLogger) SuccessLogRecord(id int64, r *MysqlLogRecord, sqlWhere *sqlstatement.LogicCondition) error {
	if r == nil {
		return nil
	}
	r.Status = "success"
	r.SqlWhere = conv.String(sqlWhere)
	return m.commUpdateLogRecord(id, r)
}

// FailureLogRecord 失败以后的记录
func (m *mysqlLogger) FailureLogRecord(id int64, r *MysqlLogRecord, sqlWhere *sqlstatement.LogicCondition) error {
	if r == nil {
		return nil
	}
	r.Status = "failure"
	r.SqlWhere = conv.String(sqlWhere)
	return m.commUpdateLogRecord(id, r)
}

func (m *mysqlLogger) commUpdateLogRecord(id int64, r *MysqlLogRecord) error {
	sqlBuilder := sqlstatement.NewSqlStruct(
		sqlstatement.SetTableName(m.logTableName),
		sqlstatement.SetStructData(MysqlLogRecord{}))
	updateSql, data, err := sqlBuilder.UpdateSqlWithUpdateMap(map[string]any{
		"suc_num":     r.SucNum,
		"from_id":     r.FromId,
		"end_id":      r.EndId,
		"sql_where":   r.SqlWhere,
		"extend":      r.Extend,
		"errors":      r.Errors,
		"status":      r.Status,
		"update_time": time.Now(),
	}, map[string]any{
		"id": id,
	})

	if err != nil {
		return err
	}
	_, err = sqlcomm.MysqlExec(m.dbConn, updateSql, data...)
	if err != nil {
		return err
	}
	return nil
}

func (m *mysqlLogger) getCurrentMaxPageWhere(r *MysqlLogRecord, pageStart, pageEnd int) (sqlstatement.LogicCondition, error) {
	if r.TableName == "" || r.Method == "" {
		return sqlstatement.LogicCondition{}, fmt.Errorf("tableName or method is empty")
	}

	whereCond := sqlstatement.LogicCondition{Conditions: []any{
		sqlstatement.Condition{
			Field:    "database_name",
			Operator: "=",
			Value:    squirrel.Expr("DATABASE()"),
		},
		sqlstatement.Condition{
			Field:    "table_name",
			Operator: "=",
			Value:    r.TableName,
		},
		sqlstatement.Condition{
			Field:    "method",
			Operator: "=",
			Value:    r.Method,
		},
		sqlstatement.Condition{
			Field:    "page_size",
			Operator: "=",
			Value:    r.PageSize,
		},
		sqlstatement.Condition{
			Field:    "page_now",
			Operator: ">=",
			Value:    pageStart,
		},
	}, Operator: "AND"}

	if pageEnd > 0 {
		whereCond.Conditions = append(whereCond.Conditions, sqlstatement.Condition{
			Field:    "page_now",
			Operator: "<=",
			Value:    pageEnd,
		})
	}
	return whereCond, nil

}

func (m *mysqlLogger) FindLogSuccessMaxPageNow(r *MysqlLogRecord, pageStart, pageEnd int) (*MysqlLogRecord, error) {
	whereCond, err := m.getCurrentMaxPageWhere(r, pageStart, pageEnd)
	if err != nil {
		return nil, err
	}
	whereCond.Conditions = append(whereCond.Conditions, sqlstatement.Condition{
		Field:    "status",
		Operator: "=",
		Value:    "success",
	})
	st := sqlstatement.Statement{}
	whereStr, params := st.GenerateWhereClause(whereCond)

	selectSql := fmt.Sprintf(`SELECT * FROM %s where %s ORDER BY page_now DESC LIMIT 1`, m.logTableName, whereStr)

	mapList, err := sqlcomm.MysqlQuery(m.dbConn, selectSql, params...)
	if err != nil {
		return nil, err
	}
	if len(mapList) == 0 {
		return nil, nil
	}
	logRecord := &MysqlLogRecord{}
	err = conv.Unmarshal(mapList[0], logRecord)
	if err != nil {
		return nil, err
	}
	return logRecord, nil
}
func (m *mysqlLogger) ListPageNowErrorLog(tableName string, method string, pageSize int, pageStart, pageEnd int) ([]int, error) {
	whereCond, err := m.getCurrentMaxPageWhere(&MysqlLogRecord{
		TableName: tableName,
		Method:    method,
		PageSize:  pageSize,
	}, pageStart, pageEnd)
	if err != nil {
		return nil, err
	}
	st := sqlstatement.Statement{}
	whereStr, params := st.GenerateWhereClause(whereCond)

	selectSql := fmt.Sprintf(`SELECT * FROM %s where %s ORDER BY page_now ASC`, m.logTableName, whereStr)
	mapList, err := sqlcomm.MysqlQuery(m.dbConn, selectSql, params...)
	if err != nil {
		return nil, err
	}

	retMapList := make([]*MysqlLogRecord, 0)

	if len(mapList) == 0 {
		return []int{}, nil
	}
	err = conv.Unmarshal(mapList, &retMapList)
	if err != nil {
		return nil, err
	}

	errPageNowList := make([]int, 0)
	for i := pageStart; i <= pageEnd; i++ {
		pageSuccessData := lo.FindOrElse(retMapList, nil, func(one *MysqlLogRecord) bool {
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
