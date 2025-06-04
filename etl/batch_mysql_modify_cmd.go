package etl

import (
	"fmt"
	"github.com/magic-lib/go-plat-utils/goroutines"
	"time"
)

func NewMySqlBatchCheckModify(data *MySqlImportData) *batchMySqlTableImportCmd {
	return &batchMySqlTableImportCmd{
		batchMySqlImportData: data,
	}
}

// ModifyData 根据查询到的方法，更改新的数据
func (b *batchMySqlTableImportCmd) ModifyData() {
	complete, err := goroutines.AsyncForEachWhile(b.batchMySqlImportData.TableList, func(oneImportTable oneImportTable, index int) (bool, error) {

		batchExecutor := newBatchMySqlTableImport(&mysqlDataSource{
			ConnCfg: &b.batchMySqlImportData.SrcMysqlConfig,
		}, &mysqlDataSource{
			ConnCfg: &b.batchMySqlImportData.DstMysqlConfig,
		})
		batchExecutor.LogTableName = b.batchMySqlImportData.LogTableName
		batchExecutor.ErrorFilePath = b.batchMySqlImportData.ErrorFilePath
		batchExecutor.PageLimit = b.batchMySqlImportData.PageLimit

		batchExecutor.FromPrimaryKey = oneImportTable.SrcPrimaryKey
		batchExecutor.FromTableName = oneImportTable.SrcTableName
		batchExecutor.FromSqlQuery = oneImportTable.SrcSqlQuery
		batchExecutor.StartId = oneImportTable.SrcStartId
		batchExecutor.PageStart = oneImportTable.SrcPageStart
		batchExecutor.PageEnd = oneImportTable.SrcPageEnd
		batchExecutor.ToTableName = oneImportTable.DstTableName
		batchExecutor.DstPrimaryKey = oneImportTable.DstPrimaryKey
		batchExecutor.ToColumnMap = oneImportTable.DstColumnMap

		if len(oneImportTable.DstExchangeFuncKeyList) > 0 {
			batchExecutor.ExchangeFuncList = make([]ExchangeFunc, 0)
			for _, key := range oneImportTable.DstExchangeFuncKeyList {
				if oneFunc, ok := exchangeFuncMap[key]; ok {
					batchExecutor.ExchangeFuncList = append(batchExecutor.ExchangeFuncList, oneFunc)
				}
			}
		}

		err := batchExecutor.batchModify()
		if err != nil {
			fmt.Println("批量修改有失败：", err)
		}

		{ // 检查是否有失败的记录，重新进行导入
			err = batchExecutor.checkComplete(MysqlMethodModify, batchExecutor.StartId, func(b *batchMySqlTableImport) error {
				return b.batchModify()
			})
			if err != nil {
				fmt.Println("检查是否有失败的记录，重新进行导入有失败：", err)
			}
		}

		return true, err
	}, goroutines.AsyncForEachWhileOptions{
		TotalTimeout:   24 * time.Hour,
		MaxConcurrency: 2,
	})
	if err != nil {
		fmt.Println("批量修改有失败：", err)
	}
	if complete {
		fmt.Println("修改完成了")
	} else {
		fmt.Println("修改完成，有部分未成功，检查日志")
	}
}
