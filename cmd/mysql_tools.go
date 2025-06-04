package main

import (
	"encoding/json"
	"fmt"
	"github.com/magic-lib/go-plat-mysql/etl"
	"github.com/urfave/cli/v2"
	"log"
	"os"
)

var cmdConfig = struct {
	JsonConfig string
	ToolsType  string
}{
	JsonConfig: "",
	ToolsType:  "",
}

func main() {
	app := &cli.App{
		Name:  "mysql-tools",
		Usage: "Tool for importing data into MySQL",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "tool-type",
				Destination: &cmdConfig.ToolsType,
				Required:    true,
				Usage:       "tool-type: import",
			},
			&cli.StringFlag{
				Name:        "json-config",
				Destination: &cmdConfig.JsonConfig,
				Required:    true,
				Usage:       "JsonConfig",
			},
		},
		Action: func(c *cli.Context) error {
			jsonData, err := getToolsConfigFromFile(cmdConfig.JsonConfig)
			if err != nil {
				fmt.Println("getToolsConfigFromFile opening file:", err)
				return err
			}

			if cmdConfig.ToolsType == etl.MysqlMethodImport {
				importTable := etl.NewMySqlBatchImportTable(jsonData)
				importTable.Start()
				return nil
			}

			if cmdConfig.ToolsType == etl.MysqlMethodDelete {
				importTable := etl.NewMySqlBatchCheckDelete(jsonData)
				importTable.CheckNewOrDelete()
				return nil
			}

			if cmdConfig.ToolsType == etl.MysqlMethodModify {
				importTable := etl.NewMySqlBatchCheckModify(jsonData)
				importTable.ModifyData()
				return nil
			}

			return nil
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatalln(err)
	}
}

func getToolsConfigFromFile(jsonConfig string) (*etl.MySqlImportData, error) {
	// 打开 JSON 文件
	file, err := os.Open(jsonConfig)
	if err != nil {
		return nil, err
	}
	defer func(file *os.File) {
		err = file.Close()
		if err != nil {
			fmt.Println("Close file error:", err)
		}
	}(file)

	// 创建一个结构体变量用于存储解析后的数据
	var data etl.MySqlImportData

	// 解码 JSON 文件内容到结构体
	decoder := json.NewDecoder(file)
	if err = decoder.Decode(&data); err != nil {
		return nil, err
	}
	return &data, nil
}
