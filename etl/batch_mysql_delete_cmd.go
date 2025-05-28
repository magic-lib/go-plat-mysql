package etl

import (
	"fmt"
	"github.com/magic-lib/go-plat-utils/goroutines"
	"time"
)

func NewMySqlBatchCheckDelete(data *MySqlImportData) *batchMySqlTableImportCmd {
	return &batchMySqlTableImportCmd{
		batchMySqlImportData: data,
	}
}

// CheckDelete 原始数据删除了，新表同样也需要删除
func (b *batchMySqlTableImportCmd) CheckDelete() {
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
		batchExecutor.PageStart = oneImportTable.SrcPageStart
		batchExecutor.PageEnd = oneImportTable.SrcPageEnd
		batchExecutor.ToTableName = oneImportTable.DstTableName
		batchExecutor.DstPrimaryKey = oneImportTable.DstPrimaryKey
		batchExecutor.ToColumnMap = oneImportTable.DstColumnMap

		err := batchExecutor.checkDelete()
		if err != nil {
			fmt.Println("批量删除有失败：", err)
		}

		return true, err
	}, goroutines.AsyncForEachWhileOptions{
		TotalTimeout:   24 * time.Hour,
		MaxConcurrency: 2,
	})
	if err != nil {
		fmt.Println("批量导入有失败：", err)
	}
	if complete {
		fmt.Println("导入完成了")
	} else {
		fmt.Println("导入完成，有部分未成功，检查日志")
	}
}
