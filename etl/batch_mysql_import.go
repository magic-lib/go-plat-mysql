package etl

import (
	"database/sql"
	"fmt"
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

	logService, err := newMysqlLogger(b.toDb, b.LogTableName)
	if err != nil {
		return err
	}

	importExec, err := newMysqlImport(b.toDb, b.ToTableName, b.DstPrimaryKey)
	if err != nil {
		return err
	}
	importExec.ErrorFilePrefix = b.ErrorFilePath

	if b.PageLimit == 0 {
		err = queryData.checkFetchDataList()
		if err != nil {
			return err
		}
		id, err := logService.insertLogRecord(&logRecord{
			TableName: b.ToTableName,
			PageNow:   1,
			PageSize:  int(b.PageLimit),
		})
		if err != nil {
			return err
		}
		// 全量导入
		_, sucNum, remark, err := b.runOneList(importExec, queryData)
		if err != nil {
			_ = logService.failureLogRecord(id, &logRecord{
				SucNum: sucNum,
				Errors: err.Error(),
				Extend: remark,
			})
		} else {
			_ = logService.successLogRecord(id, sucNum, remark)
		}
		return err
	}

	var isEnd bool
	queryData.page, isEnd = b.getPageModel(logService, b.ToTableName)
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

		id, logErr := logService.insertLogRecord(&logRecord{
			TableName: b.ToTableName,
			PageNow:   queryData.page.PageNow,
			PageSize:  queryData.page.PageSize,
		})
		isEndQuery, sucNum, remark, tempErr := b.runOneList(importExec, queryData)
		if logErr == nil {
			if tempErr != nil {
				_ = logService.failureLogRecord(id, &logRecord{
					SucNum: sucNum,
					Errors: tempErr.Error(),
					Extend: remark,
				})
				fmt.Println("执行runOneList语句时错误: ", tempErr)
			} else {
				_ = logService.successLogRecord(id, sucNum, remark)
			}
		}

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
func (b *batchMySqlTableImport) checkComplete() error {
	if b.toDb == nil {
		err := b.mysqlDb()
		if err != nil {
			return err
		}
	}

	logService, err := newMysqlLogger(b.toDb, b.LogTableName)
	if err != nil {
		return err
	}

	page, _ := b.getPageModel(logService, b.ToTableName)
	errPageNowList, err := logService.getAllPageNowErrorLogList(b.ToTableName, int(b.PageLimit), page.PageNow, int(b.PageEnd))
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

func (b *batchMySqlTableImport) getPageModel(logService *mysqlLogger, toTableName string) (page *httputil.PageModel, isEnd bool) {
	pageNow := 1
	if b.PageStart > 0 {
		pageNow = int(b.PageStart) //手动设置
	}
	//根据日志最后一条，查询最大成功的页码
	num, err := logService.getCurrentMaxPageNow(&logRecord{
		TableName: toTableName,
		PageSize:  int(b.PageLimit),
	}, pageNow, int(b.PageEnd))
	if err == nil {
		if num != 0 {
			//表示数据已经查询完毕了
			if uint(num) >= b.PageEnd && b.PageEnd > 0 {
				page = &httputil.PageModel{
					PageNow:  pageNow, // 从第一页开始
					PageSize: int(b.PageLimit),
				}
				page = page.GetPage(page.PageSize)
				return page, true
			}
		}
		pageNow = num + 1 //从下一页开始查询
	}

	page = &httputil.PageModel{
		PageNow:  pageNow, // 从第一页开始
		PageSize: int(b.PageLimit),
	}
	page = page.GetPage(page.PageSize)

	return page, false
}

func (b *batchMySqlTableImport) runOneList(importExec *mysqlImport, queryData *mysqlExport) (bool, int, string, error) {
	dataList, err := queryData.fetchDataList()
	if err != nil {
		return false, 0, "", err
	}

	if len(dataList) == 0 {
		return true, 0, "", nil
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
	}
	remark := fmt.Sprintf("%s - %s", firstCurrId, lastCurrId)

	sucNum, err := importExec.importData(idList, queryData.page.PageNow, dataList)
	if err != nil {
		return false, sucNum, remark, err
	}
	return isEndQuery, sucNum, remark, nil
}
