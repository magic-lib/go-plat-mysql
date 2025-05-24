package etl

import (
	"fmt"
	"github.com/magic-lib/go-plat-startupcfg/startupcfg"
	"github.com/magic-lib/go-plat-utils/goroutines"
	"time"
)

type MySqlImportData struct {
	SrcMysqlConfig startupcfg.MysqlConfig `json:"src_mysql_config"`
	DstMysqlConfig startupcfg.MysqlConfig `json:"dst_mysql_config"`
	LogTableName   string                 `json:"log_table_name"`
	PageLimit      uint                   `json:"page_limit"`
	TableList      []oneImportTable       `json:"table_list"`
}

type oneImportTable struct {
	SrcTableName  string            `json:"src_table_name"`
	SrcSqlQuery   string            `json:"src_sql_query"`   //自定义查询语句，跨表查询 string `json:"from_table_name"`
	SrcPrimaryKey string            `json:"src_primary_key"` //唯一排序字段，主键，避免重复查询，按某一个顺序来进行查询，目前不支持联合主键
	SrcPageStart  uint              `json:"src_page_start"`  //从第几页进行查起
	SrcPageEnd    uint              `json:"src_page_end"`    //并发执行的结束页
	DstTableName  string            `json:"dst_table_name"`
	DstColumnMap  map[string]string `json:"dst_column_map"` //需要同步的字段，key为目标表字段名，value为表达式
}

type batchMySqlTableImportCmd struct {
	batchMySqlImportData *MySqlImportData
}

func NewMySqlBatchImportTable(data *MySqlImportData) *batchMySqlTableImportCmd {
	return &batchMySqlTableImportCmd{
		batchMySqlImportData: data,
	}
}
func (b *batchMySqlTableImportCmd) Start() {
	complete, err := goroutines.AsyncForEachWhile(b.batchMySqlImportData.TableList, func(oneImportTable oneImportTable, index int) (bool, error) {

		batchExecutor := newBatchMySqlTableImport(&mysqlDataSource{
			ConnCfg: &b.batchMySqlImportData.SrcMysqlConfig,
		}, &mysqlDataSource{
			ConnCfg: &b.batchMySqlImportData.DstMysqlConfig,
		})
		batchExecutor.LogTableName = b.batchMySqlImportData.LogTableName
		batchExecutor.PageLimit = b.batchMySqlImportData.PageLimit

		batchExecutor.FromPrimaryKey = oneImportTable.SrcPrimaryKey
		batchExecutor.FromTableName = oneImportTable.SrcTableName
		batchExecutor.FromSqlQuery = oneImportTable.SrcSqlQuery
		batchExecutor.PageStart = oneImportTable.SrcPageStart
		batchExecutor.PageEnd = oneImportTable.SrcPageEnd
		batchExecutor.ToTableName = oneImportTable.DstTableName
		batchExecutor.ToColumnMap = oneImportTable.DstColumnMap

		err := batchExecutor.batchImport()
		if err != nil {
			fmt.Println("批量导入有失败：", err)
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
