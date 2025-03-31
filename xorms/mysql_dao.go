package xorms

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/magic-lib/go-plat-mysql/sqlstatement"
	"github.com/magic-lib/go-plat-startupcfg/startupcfg"
	"github.com/magic-lib/go-plat-utils/cond"
	"github.com/magic-lib/go-plat-utils/conv"
	"github.com/magic-lib/go-plat-utils/logs"
	"strings"
	"sync"
	"xorm.io/core"
	"xorm.io/xorm"
)

// Dao 访问数据库的对象
type Dao struct {
	once    sync.Once
	connect *startupcfg.MysqlConfig
	ctx     context.Context
	engine  *xorm.Engine
	//如果有事务，则将使用
	daoSessionLock sync.Mutex
	daoSession     *xorm.Session
}

// TransCallback 事务回调函数
type TransCallback func(*xorm.Session) error

var (
	defaultAllEngines = NewEnginePool() //全局使用
)

// SetTableSuffix 设置表的后缀，table_1, table_2等
func (m *Dao) SetTableSuffix(suffix string) {
	tbMapper := core.NewSuffixMapper(core.GonicMapper{}, suffix)
	m.engine.SetTableMapper(tbMapper)
}

// SetTableTagIdentifier 设置表的tag名称，默认为"xorm"
func (m *Dao) SetTableTagIdentifier(tagName string) {
	m.engine.SetTagIdentifier(tagName)
}

// SetLogger 设置日志
func (m *Dao) SetLogger(loggerOld any) {
	logger := setXormLogger(loggerOld)
	//一个链接只需要执行一次
	if logger != nil {
		m.engine.SetLogger(logger)
		m.engine.ShowSQL(true)
	} else {
		m.engine.ShowSQL(false)
	}
}

// setLogger 强制设置logger
func (m *Dao) setLogger() {
	m.SetLogger(logs.CtxLogger(m.ctx))
}

// initDB 初始化连接，内部
func (m *Dao) initDB(ctx context.Context, co *startupcfg.MysqlConfig) (*Dao, error) {
	engine, err := defaultAllEngines.GetEngine(co)
	if err != nil {
		return nil, err
	}
	if engine == nil {
		return nil, fmt.Errorf("engine get nil: %s", conv.String(co))
	}
	m.engine = engine
	m.connect = co
	m.ctx = ctx

	m.once.Do(func() {
		//默认打印
		m.SetLogger(logs.DefaultLogger())
	})
	return m, nil
}

// GetEngine 动态获取Engine
func (m *Dao) GetEngine() (*xorm.Engine, error) {
	engine, err := defaultAllEngines.GetEngine(m.connect)
	if err != nil {
		return nil, err
	}
	if engine == nil {
		return m.engine, nil
	}
	return engine, nil
}

// Insert 新增，返回影响的条数和错误
func (m *Dao) Insert(info ...any) (int64, error) {
	if m.daoSession != nil {
		return m.daoSession.Insert(info...)
	}
	return m.engine.Insert(info...)
}

// FlagDelete 逻辑删除
func (m *Dao) FlagDelete(id int64, info any) (int64, error) {
	if m.daoSession != nil {
		return m.daoSession.ID(id).Delete(info)
	}
	return m.engine.ID(id).Delete(info)
}

// Delete 删除
func (m *Dao) Delete(id any, info any) (int64, error) {
	if m.daoSession != nil {
		return m.daoSession.ID(id).Unscoped().Delete(info)
	}
	return m.engine.ID(id).Unscoped().Delete(info)
}

// Update 更新
func (m *Dao) Update(id any, info any, columns ...string) (int64, error) {
	if m.daoSession != nil {
		sessionIns := m.daoSession.ID(id)
		if len(columns) > 0 {
			sessionIns = sessionIns.Cols(columns...)
		}
		return sessionIns.Update(info)
	}
	sessionIns := m.engine.ID(id)
	if len(columns) > 0 {
		sessionIns = sessionIns.Cols(columns...)
	}
	return sessionIns.Update(info)
}

// Get 通过主键查询单个
func (m *Dao) Get(id any, info any) (bool, error) {
	if m.daoSession != nil {
		return m.daoSession.ID(id).Get(info)
	}
	return m.engine.ID(id).Get(info)
}

// UpdateWhere 条件更新
func (m *Dao) UpdateWhere(whereStr string, argList []any, info any, columns ...string) (int64, error) {
	if m.daoSession != nil {
		sessionIns := m.daoSession.Where(whereStr, argList...)
		if len(columns) > 0 {
			sessionIns = sessionIns.Cols(columns...)
		}
		return sessionIns.Update(info)
	}
	sessionIns := m.engine.Where(whereStr, argList...)
	if len(columns) > 0 {
		sessionIns = sessionIns.Cols(columns...)
	}
	return sessionIns.Update(info)
}

// DeleteWhere 条件删除
func (m *Dao) DeleteWhere(whereStr string, argList []any, info any) (int64, error) {
	if m.daoSession != nil {
		return m.daoSession.Where(whereStr, argList...).Unscoped().Delete(info)
	}
	return m.engine.Where(whereStr, argList...).Unscoped().Delete(info)
}

// GetWhere 通过where查询单个
func (m *Dao) GetWhere(whereStr string, argList []any, info any) (bool, error) {
	if m.daoSession != nil {
		return m.daoSession.Where(whereStr, argList...).Get(info)
	}
	return m.engine.Where(whereStr, argList...).Get(info)
}

// TransAction 事务
func (m *Dao) TransAction(callback TransCallback) error {
	session := m.engine.NewSession()
	defer func(session *xorm.Session) {
		_ = session.Close()
	}(session)

	if err := session.Begin(); err != nil {
		return fmt.Errorf("fail to session begin：" + err.Error())
	}

	m.daoSessionLock.Lock()
	m.daoSession = session
	err := callback(session)
	m.daoSession = nil
	m.daoSessionLock.Unlock()
	if err != nil {
		_ = session.Rollback()
		return err
	}
	return session.Commit()
}

// GetListByMap 通过对象查询列表
func (m *Dao) GetListByMap(info map[string]any, bean any) ([]map[string]string, error) {
	tableInfo, err := m.engine.TableInfo(bean)
	if err != nil {
		return nil, err
	}
	newInfo := make(map[string]any)
	for name, val := range info {
		isFind := false
		for _, oneColumn := range tableInfo.Columns() {
			if oneColumn.Name == name {
				isFind = true
				break
			}
		}
		if isFind {
			newInfo[name] = val
		}
	}
	tempStatement := m.engine.Table(bean)

	stat := new(sqlstatement.Statement)
	whereString, dataList := stat.GenerateWhereClauseByMap(newInfo)
	tempStatement = tempStatement.Where(whereString, dataList...)

	retMap, err := tempStatement.QueryString()
	if err != nil {
		return nil, err
	}
	if retMap == nil {
		retMap = make([]map[string]string, 0)
	}
	return retMap, nil
}

func (m *Dao) explainSqlHandle(sqlOrArgs ...any) {
	if len(sqlOrArgs) == 0 {
		return
	}
	sType := m.getStatementType(conv.String(sqlOrArgs[0]))
	if sType != "SELECT" {
		//不是查询语句不进行分析
		return
	}

	oldSql := sqlOrArgs[0]

	sqlOrArgs[0] = fmt.Sprintf("%s %s", "EXPLAIN", sqlOrArgs[0])
	retList, err := m.engine.Query(sqlOrArgs...)
	if err != nil {
		logs.DefaultLogger().Error(sqlOrArgs[0], err)
		return
	}
	if len(retList) == 0 {
		return
	}
	for _, one := range retList {
		possibleKeys, ok1 := one["possible_keys"]
		keys, ok2 := one["key"]
		table, ok3 := one["table"]
		if ok1 && ok2 {
			useKey := false //是否使用了索引

			keyStr := string(keys)
			if keyStr != "" {
				possibleKeyList := strings.Split(string(possibleKeys), ",")
				keyList := strings.Split(keyStr, ",")
				if len(keyList) > 0 {
					for _, oneKey := range keyList {
						if oneKey == "" {
							continue
						}
						if ok, _ := cond.Contains(possibleKeyList, oneKey); ok {
							useKey = true
							break
						}
					}
				}
			}

			if !useKey {
				tableName := ""
				if ok3 {
					tableName = string(table)
				}
				logs.DefaultLogger().Error("has no select index:", oldSql, "|",
					tableName, "|", string(possibleKeys), "|", keyStr)
			}
		}
	}

}

func (m *Dao) getStatementType(sql string) string {
	if len(sql) == 0 {
		return ""
	}

	// 去除首尾空格并转换为大写
	sql = strings.TrimSpace(strings.ToUpper(sql))

	if strings.HasPrefix(sql, "SELECT") {
		return "SELECT"
	} else if strings.HasPrefix(sql, "INSERT") {
		return "INSERT"
	} else if strings.HasPrefix(sql, "UPDATE") {
		return "UPDATE"
	} else if strings.HasPrefix(sql, "DELETE") {
		return "DELETE"
	}

	return ""
}

// SqlQuery sql查询
func (m *Dao) SqlQuery(sqlStr string, args ...any) ([]map[string]string, error) {
	queryParam := make([]any, 0)
	queryParam = append(queryParam, sqlStr)
	if args != nil && len(args) > 0 {
		queryParam = append(queryParam, args...)
	}
	retList, err := m.engine.Query(queryParam...)
	if err != nil {
		logs.DefaultLogger().Error("SqlQuery Error:", err, sqlStr, m.engine)
		return nil, err
	}
	if explainSql {
		m.explainSqlHandle(queryParam...)
	}

	if retList == nil {
		return []map[string]string{}, nil
	}
	retData := make([]map[string]string, len(retList))
	for i, one := range retList {
		oneTemp := make(map[string]string)
		for key, val := range one {
			oneTemp[key] = string(val)
		}
		retData[i] = oneTemp
	}
	return retData, nil
}

// SqlExec sql更新
func (m *Dao) SqlExec(sqlStr string, args ...any) (int64, error) {
	queryParam := make([]any, 0)
	queryParam = append(queryParam, sqlStr)
	if args != nil && len(args) > 0 {
		queryParam = append(queryParam, args...)
	}
	var execResult sql.Result
	var err error
	if m.daoSession != nil {
		execResult, err = m.daoSession.Exec(queryParam...)
	} else {
		execResult, err = m.engine.Exec(queryParam...)
	}

	if err != nil {
		return 0, err
	}
	//如果是插入，则返回自增ID，否则返回影响的行数
	//如果插入失败，则影响行数会为0
	num, err := execResult.LastInsertId()
	if err == nil && num > 0 {
		return num, nil
	}

	num, err = execResult.RowsAffected()
	if err != nil {
		return 0, err
	}
	return num, nil
}
