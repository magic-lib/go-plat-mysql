package etl

import (
	"database/sql"
	"fmt"
	"github.com/Masterminds/squirrel"
	"github.com/magic-lib/go-plat-mysql/sqlcomm"
	"github.com/magic-lib/go-plat-utils/conv"
	"github.com/samber/lo"
	"os"
	"time"
)

type mysqlImport struct {
	ErrorFilePrefix string `json:"error_file_prefix"`
	ErrorFileSuffix string `json:"error_file_suffix"`
	dbConn          *sql.DB
	tableName       string
	columnMap       map[string]*sqlcomm.MysqlColumn
	columns         []string
	dstPrimaryKey   string
}

func newMysqlImport(db *sql.DB, tableName string, dstPrimaryKey string) (*mysqlImport, error) {
	if db == nil {
		return nil, fmt.Errorf("数据库连接不能为空")
	}
	columns, err := sqlcomm.MysqlTableColumns(db, tableName)
	if err != nil {
		return nil, fmt.Errorf("获取列失败: %w", err)
	}
	columnMap := make(map[string]*sqlcomm.MysqlColumn)
	lo.ForEach(columns, func(column *sqlcomm.MysqlColumn, i int) {
		columnMap[column.ColumnName] = column
	})

	columnNames := make([]string, 0)
	for name, _ := range columnMap {
		columnNames = append(columnNames, name)
	}

	if dstPrimaryKey == "" {
		_ = lo.FindOrElse(columns, nil, func(column *sqlcomm.MysqlColumn) bool {
			if column.ColumnKey == "PRI" {
				dstPrimaryKey = column.ColumnName
				return true
			}
			return false
		})
		if dstPrimaryKey == "" {
			fmt.Println("没有找到目标表的主键")
		}
	}

	return &mysqlImport{
		dbConn:          db,
		tableName:       tableName,
		dstPrimaryKey:   dstPrimaryKey,
		columnMap:       columnMap,
		columns:         columnNames,
		ErrorFileSuffix: ".error.log",
	}, nil
}

func (m *mysqlImport) defaultExchangeFunc(dataList []map[string]any) []map[string]any {
	for _, one := range dataList {
		for columnName, v := range one {
			if oneColumn, ok := m.columnMap[columnName]; ok {
				one[columnName] = sqlcomm.MysqlColumnValidValue(v, oneColumn)
			}
		}
	}
	return dataList
}
func (m *mysqlImport) importData(idList []string, pageNow int, dataList []map[string]any) (int, error) {
	if len(dataList) == 0 {
		return 0, fmt.Errorf("数据不能为空")
	}
	if m.tableName == "" {
		return 0, fmt.Errorf("表名不能为空")
	}

	fileName := fmt.Sprintf("%s%s%s", m.ErrorFilePrefix, m.tableName, m.ErrorFileSuffix)

	file, err := m.getErrorFile(fileName)
	if err != nil {
		return 0, fmt.Errorf("打开错误文件失败: %w", err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			fmt.Println("关闭错误文件失败: ", err)
		}
	}(file)

	//这里要进行批量插入
	allValues := make([][]any, 0)
	lo.ForEach(dataList, func(item map[string]any, i int) {
		values := make([]any, 0)
		for _, col := range m.columns {
			if v, ok := item[col]; ok {
				values = append(values, v)
				continue
			}
			//判断如果允许为空则为空，不能为空，则为空字符串
			var oneData any = ""
			if oneColumn, ok := m.columnMap[col]; ok {
				oneData = sqlcomm.MysqlColumnValidValue(nil, oneColumn)
			}
			values = append(values, oneData)
		}
		allValues = append(allValues, values)
	})

	stmt := squirrel.Replace(m.tableName).Columns(m.columns...)
	for _, row := range allValues {
		stmt = stmt.Values(row...)
	}
	sqlString, sqlValue, err := stmt.ToSql()
	if err != nil {
		err = fmt.Errorf("生成sql语句失败: %w", err)
		errTemp := m.writeError(conv.String(idList), file)
		if errTemp != nil {
			fmt.Println("生成sql语句时错误: ", err)
		}
		return 0, err
	}
	firstCurrId := ""
	lastCurrId := ""
	if len(idList) > 0 {
		firstCurrId = idList[0]
		lastCurrId = idList[len(idList)-1]
	}

	ret, err := m.dbConn.Exec(sqlString, sqlValue...)
	if err != nil {
		err = fmt.Errorf("写入数据失败: %w", err)
		errTemp := m.writeError(conv.String(idList)+"\n"+sqlString+"\n"+conv.String(sqlValue), file)
		if errTemp != nil {
			fmt.Println("写入数据错误: ", errTemp, " id:", firstCurrId, "-", lastCurrId)
		}
		if ret != nil {
			num, errNum := ret.RowsAffected()
			if errNum == nil {
				return int(num), err
			}
		}
		return 0, err
	}

	sqlSuccessStr := fmt.Sprintf("写入数据成功, num: %%d, len: %%d, pageNow:%d, id: %s-%s time: %s", pageNow, firstCurrId, lastCurrId, conv.String(time.Now()))

	num, _ := ret.RowsAffected()
	fmt.Println(fmt.Sprintf(sqlSuccessStr, num, len(dataList)))
	return len(dataList), nil
}

func (m *mysqlImport) getErrorFile(filename string) (*os.File, error) {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("打开错误文件失败: %w", err)
	}
	return file, nil
}

func (m *mysqlImport) writeError(data string, file *os.File) error {
	if _, err := file.WriteString(data + "\n"); err != nil {
		return fmt.Errorf("写入错误数据失败: %w", err)
	}
	return nil
}
