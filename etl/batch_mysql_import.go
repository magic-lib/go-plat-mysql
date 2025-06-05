package etl

import (
	"database/sql"
	"fmt"
	"github.com/magic-lib/go-plat-mysql/sqlcomm"
	"github.com/magic-lib/go-plat-mysql/sqlstatement"
	"github.com/magic-lib/go-plat-utils/cond"
	"github.com/magic-lib/go-plat-utils/conv"
	"github.com/magic-lib/go-plat-utils/templates/ruleengine"
	"github.com/magic-lib/go-plat-utils/utils/httputil"
	"github.com/samber/lo"
)

type batchMySqlTableImport struct {
	srcMysqlDataSource *mysqlDataSource
	toMysqlDataSource  *mysqlDataSource
	srcDb              *sql.DB
	toDb               *sql.DB

	FromSqlQuery   string //自定义查询语句，跨表查询
	FromTableName  string //查询的表名
	ToTableName    string //查询的表名
	DstPrimaryKey  string //查询的表名
	DstInsertType  string //插入方式,是用insert into 还是 replace into
	FromPrimaryKey string //排序字段，避免重复查询，按某一个顺序来进行查询
	PageStart      uint   //从第几页进行查起
	StartId        string //从第行数据开始
	PageEnd        uint   //结束页
	PageLimit      uint   //分页信息，避免一次查询过多

	LogTableName  string
	ErrorFilePath string

	ToColumnMap map[string]string // 目标表字段和源表字段的映射关系

	ExchangeFuncList []ExchangeFunc
}

var exchangeFuncMap = map[string]ExchangeFunc{}

func RegisterExchangeFunc(name string, f ExchangeFunc) {
	exchangeFuncMap[name] = f
}

type ExchangeFunc func([]map[string]any) []map[string]any

func newBatchMySqlTableImport(srcMysqlDataSource *mysqlDataSource, toMysqlDataSource *mysqlDataSource) *batchMySqlTableImport {
	b := &batchMySqlTableImport{
		srcMysqlDataSource: srcMysqlDataSource,
		toMysqlDataSource:  toMysqlDataSource,
	}
	_ = b.mysqlDb()
	return b
}

func (b *batchMySqlTableImport) mysqlDb() error {
	srcMysqlConn, err := newMysqlDataSource(b.srcMysqlDataSource)

	if err != nil {
		return err
	}
	srcDb, err := srcMysqlConn.Connect()
	if err != nil {
		return err
	}

	toMysqlConn, err := newMysqlDataSource(b.toMysqlDataSource)

	if err != nil {
		return err
	}
	toDb, err := toMysqlConn.Connect()
	if err != nil {
		return err
	}
	b.srcDb = srcDb
	b.toDb = toDb
	return nil
}
func (b *batchMySqlTableImport) runWithColumnMap(oneData map[string]any) map[string]any {
	if b.ToColumnMap == nil {
		return oneData
	}
	ruleEngine := ruleengine.NewEngineLogic()
	for k, v := range b.ToColumnMap {
		if v == "" {
			continue
		}
		newVal, err := ruleEngine.RunOneRuleString(v, oneData)
		if err == nil {
			oneData[k] = newVal
		}
	}
	return oneData
}

func (b *batchMySqlTableImport) batchImport() error {
	if b.srcDb == nil || b.toDb == nil {
		err := b.mysqlDb()
		if err != nil {
			return err
		}
	}

	queryData, err := newMysqlQuery(b.srcDb, int(b.PageStart), int(b.PageEnd), int(b.PageLimit))
	if err != nil {
		return err
	}
	queryData.TableName = b.FromTableName
	queryData.SqlQuery = b.FromSqlQuery
	queryData.PrimaryKey = b.FromPrimaryKey

	logService, err := NewMysqlLogger(b.toDb, b.LogTableName)
	if err != nil {
		return err
	}

	importExec, err := newMysqlImport(b.toDb, b.ToTableName, b.DstPrimaryKey)
	if err != nil {
		return err
	}
	importExec.ErrorFilePrefix = b.ErrorFilePath
	importExec.DstInsertType = b.DstInsertType

	var insertLogRecord = func(startId string, pageNow int, pageSize int) (bool, error) {
		// 全量导入
		isEndQuery, logRecord, whereCond, tempErr := b.importOrUpdateOneList(importExec, queryData, startId)
		// 表示要插入日志
		if logRecord != nil {
			id, logErr := logService.InsertLogRecord(&MysqlLogRecord{
				TableName: b.ToTableName,
				Method:    MysqlMethodImport,
				StartId:   startId,
				PageNow:   pageNow,
				PageSize:  pageSize,
			})

			if logErr == nil {
				if tempErr != nil {
					logRecord.Errors = tempErr.Error()
					_ = logService.FailureLogRecord(id, logRecord, whereCond)
					fmt.Println("执行runOneList语句时错误: ", tempErr)
				} else {
					_ = logService.SuccessLogRecord(id, logRecord, whereCond)
				}
			}
		}

		return isEndQuery, tempErr
	}

	if b.PageLimit == 0 {
		err = queryData.checkFetchDataList()
		if err != nil {
			return err
		}
		_, tempErr := insertLogRecord(b.StartId, 1, 0)
		return tempErr
	}

	var isEnd bool
	queryData.page, isEnd = b.getPageModel(logService, b.ToTableName, MysqlMethodImport, b.StartId)
	if isEnd {
		return nil //表示已经查完了
	}

	err = queryData.checkFetchDataList()
	if err != nil {
		return err
	}

	var globalError error
	for {
		if queryData.pageEnd > 0 {
			//表示结束查询了
			if queryData.page.PageNow > queryData.pageEnd {
				break
			}
		}
		isEndQuery, tempErr := insertLogRecord(b.StartId, queryData.page.PageNow, queryData.page.PageSize)

		if tempErr != nil {
			globalError = tempErr
		}
		if isEndQuery { //如果查不到数据了，则直接跳出
			break
		}
		queryData.page.PageNow++ // 下一页
	}

	if globalError != nil {
		return globalError
	}

	return nil
}

// checkAddOrDelete 检查目标库是否需要新增或删除数据
func (b *batchMySqlTableImport) checkAddOrDelete() error {
	if b.srcDb == nil || b.toDb == nil {
		err := b.mysqlDb()
		if err != nil {
			return err
		}
	}

	if b.FromPrimaryKey == "" || b.DstPrimaryKey == "" {
		return fmt.Errorf("主键和目标主键不能为空")
	}
	queryData, err := newMysqlQuery(b.srcDb, int(b.PageStart), int(b.PageEnd), int(b.PageLimit))
	if err != nil {
		return err
	}

	if queryData == nil {
		return nil
	}
	return nil

	//// 创建临时表存储源表ID
	//createTempTableSQL := `
	//		CREATE TEMPORARY TABLE IF NOT EXISTS temp_source_ids (
	//			id BIGINT PRIMARY KEY
	//		) ENGINE=MEMORY
	//	`
	//_, err = sourceDB.Exec(createTempTableSQL)
	//if err != nil {
	//	errChan <- err
	//	return
	//}
	//
	//importExec, err := newMysqlImport(b.toDb, b.ToTableName, b.DstPrimaryKey)
	//if err != nil {
	//	return err
	//}
	//
	//deleteIdList := make([]any, 0)
	//lastSyncedID, err := b.getFirstOneId(queryData)
	//if err != nil {
	//	return err
	//}
	//if lastSyncedID == nil {
	//	return fmt.Errorf("没有数据查询列表")
	//}
	//for {
	//	page := &httputil.PageModel{
	//		PageNow:  int(b.PageStart),
	//		PageSize: int(b.PageLimit),
	//	}
	//	page = page.GetPage(page.PageSize)
	//
	//	sqlStr := fmt.Sprintf("SELECT %s FROM %s WHERE %s > ? ORDER BY %s ASC LIMIT %d", b.FromPrimaryKey, b.FromTableName,
	//		b.FromPrimaryKey, b.FromPrimaryKey, page.PageSize)
	//
	//	mapList, err := queryData.fetchDataListBySql(sqlStr, []any{lastSyncedID})
	//	if err != nil {
	//		return err
	//	}
	//	if len(mapList) == 0 {
	//		return nil
	//	}
	//
	//}
	//
	//queryData.TableName = b.FromTableName
	//queryData.SqlQuery = b.FromSqlQuery
	//queryData.PrimaryKey = b.FromPrimaryKey
	//
	//logService, err := newMysqlLogger(b.toDb, b.LogTableName)
	//if err != nil {
	//	return err
	//}
	//
	//importExec, err := newMysqlImport(b.toDb, b.ToTableName, b.DstPrimaryKey)
	//if err != nil {
	//	return err
	//}
	//importExec.ErrorFilePrefix = b.ErrorFilePath
	//
	//if b.PageLimit == 0 {
	//	err = queryData.checkFetchDataList()
	//	if err != nil {
	//		return err
	//	}
	//	id, err := logService.insertLogRecord(&logRecord{
	//		TableName: b.ToTableName,
	//		PageNow:   1,
	//		PageSize:  int(b.PageLimit),
	//	})
	//	if err != nil {
	//		return err
	//	}
	//	// 全量导入
	//	_, sucNum, remark, err := b.runOneList(importExec, queryData)
	//	if err != nil {
	//		_ = logService.failureLogRecord(id, &logRecord{
	//			SucNum: sucNum,
	//			Errors: err.Error(),
	//			Extend: remark,
	//		})
	//	} else {
	//		_ = logService.successLogRecord(id, sucNum, remark)
	//	}
	//	return err
	//}
	//
	//var isEnd bool
	//queryData.page, isEnd = b.getPageModel(logService, b.ToTableName)
	//if isEnd {
	//	return nil //表示已经查完了
	//}
	//
	//err = queryData.checkFetchDataList()
	//if err != nil {
	//	return err
	//}
	//
	//var globalError error
	//for {
	//	if queryData.pageEnd > 0 {
	//		//表示结束查询了
	//		if queryData.page.PageNow > queryData.pageEnd {
	//			break
	//		}
	//	}
	//
	//	id, logErr := logService.insertLogRecord(&logRecord{
	//		TableName: b.ToTableName,
	//		PageNow:   queryData.page.PageNow,
	//		PageSize:  queryData.page.PageSize,
	//	})
	//	isEndQuery, sucNum, remark, tempErr := b.runOneList(importExec, queryData)
	//	if logErr == nil {
	//		if tempErr != nil {
	//			_ = logService.failureLogRecord(id, &logRecord{
	//				SucNum: sucNum,
	//				Errors: tempErr.Error(),
	//				Extend: remark,
	//			})
	//			fmt.Println("执行runOneList语句时错误: ", tempErr)
	//		} else {
	//			_ = logService.successLogRecord(id, sucNum, remark)
	//		}
	//	}
	//
	//	if tempErr != nil {
	//		globalError = tempErr
	//	}
	//	if isEndQuery { //如果查不到数据了，则直接跳出
	//		break
	//	}
	//	queryData.page.PageNow++ // 下一页
	//}
	//
	//if globalError != nil {
	//	return globalError
	//}
	//
	//return nil
}

func (b *batchMySqlTableImport) batchModify() error {
	if b.srcDb == nil || b.toDb == nil {
		err := b.mysqlDb()
		if err != nil {
			return err
		}
	}

	if b.FromPrimaryKey == "" || b.DstPrimaryKey == "" {
		return fmt.Errorf("主键和目标主键不能为空")
	}
	queryData, err := newMysqlQuery(b.srcDb, int(b.PageStart), int(b.PageEnd), int(b.PageLimit))
	if err != nil {
		return err
	}

	queryData.TableName = b.FromTableName
	queryData.SqlQuery = b.FromSqlQuery
	queryData.PrimaryKey = b.FromPrimaryKey

	logService, err := NewMysqlLogger(b.toDb, b.LogTableName)
	if err != nil {
		return err
	}

	importExec, err := newMysqlImport(b.toDb, b.ToTableName, b.DstPrimaryKey)
	if err != nil {
		return err
	}
	importExec.ErrorFilePrefix = b.ErrorFilePath

	var modifyLogRecord = func(startId string, pageNow int, pageSize int) (bool, error) {
		// 全量导入
		isEndQuery, logRecord, whereCond, tempErr := b.importOrUpdateOneList(importExec, queryData, startId)

		if logRecord != nil {
			id, logErr := logService.InsertLogRecord(&MysqlLogRecord{
				TableName: b.ToTableName,
				Method:    MysqlMethodModify,
				StartId:   startId,
				PageNow:   pageNow,
				PageSize:  pageSize,
			})

			if logErr == nil {
				if tempErr != nil {
					logRecord.Errors = tempErr.Error()
					_ = logService.FailureLogRecord(id, logRecord, whereCond)
					fmt.Println("执行runOneList语句时错误: ", tempErr)
				} else {
					_ = logService.SuccessLogRecord(id, logRecord, whereCond)
				}
			}
		}

		return isEndQuery, tempErr
	}

	if b.PageLimit == 0 {
		err = queryData.checkFetchDataList()
		if err != nil {
			return err
		}
		_, tempErr := modifyLogRecord(b.StartId, 1, 0)
		return tempErr
	}

	var isEnd bool
	queryData.page, isEnd = b.getPageModel(logService, b.ToTableName, MysqlMethodModify, b.StartId)
	if isEnd {
		return nil //表示已经查完了
	}

	err = queryData.checkFetchDataList()
	if err != nil {
		return err
	}

	var globalError error
	for {
		if queryData.pageEnd > 0 {
			//表示结束查询了
			if queryData.page.PageNow > queryData.pageEnd {
				break
			}
		}
		isEndQuery, tempErr := modifyLogRecord(b.StartId, queryData.page.PageNow, queryData.page.PageSize)

		if tempErr != nil {
			globalError = tempErr
		}
		if isEndQuery { //如果查不到数据了，则直接跳出
			break
		}
		queryData.page.PageNow++ // 下一页
	}

	if globalError != nil {
		return globalError
	}

	return nil
}

// getFirstOneId 获取第一个id
func (b *batchMySqlTableImport) getFirstOneId(queryData *mysqlExport) (any, error) {
	sqlQuery := fmt.Sprintf("SELECT %s FROM %s ORDER BY %s ASC LIMIT 1", b.FromPrimaryKey, b.FromTableName,
		b.FromPrimaryKey)

	mapList, err := sqlcomm.MysqlQuery(queryData.dbConn, sqlQuery)
	if err != nil {
		return nil, err
	}
	if len(mapList) == 0 {
		return nil, nil
	}
	if param, ok := mapList[0][b.FromPrimaryKey]; ok {
		return param, nil
	}
	return nil, nil
}

func (b *batchMySqlTableImport) checkComplete(method string, startId string, exec func(b *batchMySqlTableImport) error) error {
	if b.toDb == nil {
		err := b.mysqlDb()
		if err != nil {
			return err
		}
	}

	logService, err := NewMysqlLogger(b.toDb, b.LogTableName)
	if err != nil {
		return err
	}

	page, _ := b.getPageModel(logService, b.ToTableName, method, b.StartId)
	errPageNowList, err := logService.ListPageNowErrorLog(b.ToTableName, method, startId, int(b.PageLimit), page.PageNow, int(b.PageEnd))
	if len(errPageNowList) == 0 {
		return nil
	}

	pageStart := b.PageStart
	pageEnd := b.PageEnd

	for _, pageNow := range errPageNowList {
		if pageNow == 0 {
			continue
		}
		b.PageStart = uint(pageNow)
		b.PageEnd = uint(pageNow)
		err = exec(b)
		if err != nil {
			fmt.Println("检查完成有失败：", err, "当前页:", pageNow)
		}
	}
	b.PageStart = pageStart
	b.PageEnd = pageEnd
	return nil
}

func (b *batchMySqlTableImport) getPageModel(logService *mysqlLogger, toTableName, method string, startId string) (page *httputil.PageModel, isEnd bool) {
	pageNow := 1
	if b.PageStart > 0 {
		pageNow = int(b.PageStart) //手动设置
	}
	//根据日志最后一条，查询最大成功的页码
	last, err := logService.FindLogSuccessMaxPageNow(&MysqlLogRecord{
		TableName: toTableName,
		Method:    method,
		StartId:   startId,
		PageSize:  int(b.PageLimit),
	}, pageNow, int(b.PageEnd))

	if err == nil && last != nil {
		pageNowTemp := uint(last.PageNow)
		//表示数据已经查询完毕了
		if pageNowTemp >= b.PageEnd && b.PageEnd > 0 {
			page = &httputil.PageModel{
				PageNow:  pageNow,
				PageSize: int(b.PageLimit),
			}
			page = page.GetPage(page.PageSize)
			return page, true
		}
		pageNow = last.PageNow + 1 //从下一页开始查询
	}

	page = &httputil.PageModel{
		PageNow:  pageNow,
		PageSize: int(b.PageLimit),
	}
	page = page.GetPage(page.PageSize)

	return page, false
}

func (b *batchMySqlTableImport) importOrUpdateOneList(importExec *mysqlImport, queryData *mysqlExport, startId string) (bool, *MysqlLogRecord, *sqlstatement.LogicCondition, error) {
	return b.commRunOneList(importExec, queryData, startId, func(idList []string, dataList []map[string]any, pageNow int) (int, error) {
		return importExec.importData(idList, pageNow, dataList)
	})
}

func (b *batchMySqlTableImport) commRunOneList(importExec *mysqlImport, queryData *mysqlExport, startId string, f func(idList []string, dataList []map[string]any, pageNow int) (int, error)) (bool, *MysqlLogRecord, *sqlstatement.LogicCondition, error) {
	dataList, err := queryData.fetchDataList(startId)

	logRecord := &MysqlLogRecord{}

	if err != nil {
		return false, logRecord, nil, err
	}

	if len(dataList) == 0 {
		return true, nil, nil, nil
	}

	isEndQuery := false
	if b.PageLimit > 0 {
		if len(dataList) < int(b.PageLimit) { //查询的数据量小于分页大小，则说明已经查完了
			isEndQuery = true
		}
	}

	dataList = importExec.defaultExchangeFunc(dataList)

	//对整个数据做相应处理，这里是通用设置
	if b.ToColumnMap != nil {
		lo.ForEach(dataList, func(one map[string]any, i int) {
			dataList[i] = b.runWithColumnMap(one)
		})
	}

	if len(b.ExchangeFuncList) > 0 {
		for _, exchangeFunc := range b.ExchangeFuncList {
			dataList = exchangeFunc(dataList)
		}
	}
	if len(dataList) == 0 {
		//表示过滤了，不用执行
		return false, nil, nil, nil
	}

	idList := make([]string, len(dataList))
	if importExec.dstPrimaryKey != "" {
		for i, one := range dataList {
			idList[i] = conv.String(one[importExec.dstPrimaryKey])
		}
	} else if queryData.PrimaryKey != "" {
		for i, one := range dataList {
			idList[i] = conv.String(one[queryData.PrimaryKey])
		}
	}

	firstCurrId := ""
	lastCurrId := ""
	if len(idList) > 0 {
		firstCurrId = idList[0]
		lastCurrId = idList[len(idList)-1]

		//如果主键是数字类型，则取第一个和最后一个，有可能顺序会改变，所以如果是数字，则进行排序比较，确保最小值和最大值
		if cond.IsNumeric(firstCurrId) && cond.IsNumeric(lastCurrId) {
			firstCurrIdNum, ok1 := conv.Int64(firstCurrId)
			lastCurrIdNum, ok2 := conv.Int64(lastCurrId)
			if ok1 && ok2 {
				lo.ForEach(idList, func(id string, i int) {
					tempIdNum, ok := conv.Int64(id)
					if ok {
						if tempIdNum < firstCurrIdNum {
							firstCurrIdNum = tempIdNum
						}
						if tempIdNum > lastCurrIdNum {
							lastCurrIdNum = tempIdNum
						}
					}
				})
				firstCurrId = conv.String(firstCurrIdNum)
				lastCurrId = conv.String(lastCurrIdNum)
			}
		}

	}

	sucNum, err := f(idList, dataList, queryData.page.PageNow)
	logRecord = &MysqlLogRecord{
		SucNum: sucNum,
		FromId: firstCurrId,
		EndId:  lastCurrId,
	}

	if err != nil {
		return false, logRecord, nil, err
	}
	return isEndQuery, logRecord, nil, nil
}
