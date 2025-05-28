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
	FromPrimaryKey string //排序字段，避免重复查询，按某一个顺序来进行查询
	PageStart      uint   //从第几页进行查起
	PageEnd        uint   //结束页
	PageLimit      uint   //分页信息，避免一次查询过多

	LogTableName  string
	ErrorFilePath string

	ToColumnMap map[string]string // 目标表字段和源表字段的映射关系

	ExchangeFuncList []ExchangeFunc
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

	var insertLogRecord = func(lastId int64, pageNow int, pageSize int) (bool, *MysqlLogRecord, error) {
		id, logErr := logService.InsertLogRecord(&MysqlLogRecord{
			TableName: b.ToTableName,
			Method:    MysqlMethodImport,
			PageNow:   pageNow,
			PageSize:  pageSize,
		})
		// 全量导入
		isEndQuery, logRecord, whereCond, tempErr := b.runOneList(importExec, queryData, lastId)

		if logErr == nil {
			if tempErr != nil {
				logRecord.Errors = tempErr.Error()
				_ = logService.FailureLogRecord(id, logRecord, whereCond)
				fmt.Println("执行runOneList语句时错误: ", tempErr)
			} else {
				_ = logService.SuccessLogRecord(id, logRecord, whereCond)
			}
		}

		return isEndQuery, logRecord, tempErr
	}

	if b.PageLimit == 0 {
		err = queryData.checkFetchDataList()
		if err != nil {
			return err
		}
		_, _, tempErr := insertLogRecord(0, 1, 0)
		return tempErr
	}

	var isEnd bool
	var lastId int64
	queryData.page, lastId, isEnd = b.getPageModel(logService, b.ToTableName, MysqlMethodImport)
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
		isEndQuery, logRecord, tempErr := insertLogRecord(lastId, queryData.page.PageNow, queryData.page.PageSize)

		if tempErr != nil {
			globalError = tempErr
		}
		if isEndQuery { //如果查不到数据了，则直接跳出
			break
		}
		if logRecord != nil && logRecord.EndId != "" {
			lastId, _ = conv.Int64(logRecord.EndId)
		}
		queryData.page.PageNow++ // 下一页
	}

	if globalError != nil {
		return globalError
	}

	return nil
}

// checkDelete 检查目标库是否需要删除数据
func (b *batchMySqlTableImport) checkDelete() error {
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
func (b *batchMySqlTableImport) checkComplete() error {
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

	page, _, _ := b.getPageModel(logService, b.ToTableName, MysqlMethodImport)
	errPageNowList, err := logService.ListPageNowErrorLog(b.ToTableName, MysqlMethodImport, int(b.PageLimit), page.PageNow, int(b.PageEnd))
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
		err = b.batchImport()
		if err != nil {
			fmt.Println("批量导入有失败：", err, "当前页:", pageNow)
		}
	}
	b.PageStart = pageStart
	b.PageEnd = pageEnd
	return nil
}

func (b *batchMySqlTableImport) getPageModel(logService *mysqlLogger, toTableName, method string) (page *httputil.PageModel, lastId int64, isEnd bool) {
	pageNow := 1
	if b.PageStart > 0 {
		pageNow = int(b.PageStart) //手动设置
	}
	//根据日志最后一条，查询最大成功的页码
	last, err := logService.FindLogSuccessMaxPageNow(&MysqlLogRecord{
		TableName: toTableName,
		PageSize:  int(b.PageLimit),
		Method:    method,
	}, pageNow, int(b.PageEnd))

	if err == nil && last != nil {
		lastId, _ = conv.Int64(last.EndId)
		pageNowTemp := uint(last.PageNow)
		//表示数据已经查询完毕了
		if pageNowTemp > b.PageEnd && b.PageEnd > 0 {
			page = &httputil.PageModel{
				PageNow:  pageNow,
				PageSize: int(b.PageLimit),
			}
			page = page.GetPage(page.PageSize)
			return page, lastId, true
		}
		pageNow = last.PageNow + 1 //从下一页开始查询
	}

	page = &httputil.PageModel{
		PageNow:  pageNow,
		PageSize: int(b.PageLimit),
	}
	page = page.GetPage(page.PageSize)

	return page, lastId, false
}

func (b *batchMySqlTableImport) runOneList(importExec *mysqlImport, queryData *mysqlExport, lastId int64) (bool, *MysqlLogRecord, *sqlstatement.LogicCondition, error) {
	dataList, err := queryData.fetchDataList(lastId)

	logRecord := &MysqlLogRecord{}

	if err != nil {
		return false, logRecord, nil, err
	}

	if len(dataList) == 0 {
		return true, logRecord, nil, nil
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

	sucNum, err := importExec.importData(idList, queryData.page.PageNow, dataList)
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
