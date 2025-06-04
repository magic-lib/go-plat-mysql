package main_test

import (
	"encoding/json"
	"fmt"
	"github.com/magic-lib/go-plat-mysql/etl"
	"os"
	"testing"
)

func TestMysqlExport(t *testing.T) {
	// 打开 JSON 文件
	file, err := os.Open("member.json")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	// 创建一个结构体变量用于存储解析后的数据
	var data etl.MySqlImportData

	// 解码 JSON 文件内容到结构体
	decoder := json.NewDecoder(file)
	if err = decoder.Decode(&data); err != nil {
		fmt.Println("Error decoding JSON:", err)
		return
	}

	importTable := etl.NewMySqlBatchImportTable(&data)
	importTable.Start()
}
