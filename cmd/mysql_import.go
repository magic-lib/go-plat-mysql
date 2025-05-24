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
			if cmdConfig.ToolsType == "import" {
				// 打开 JSON 文件
				file, err := os.Open(cmdConfig.JsonConfig)
				if err != nil {
					fmt.Println("Error opening file:", err)
					return err
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
					fmt.Println("Error decoding JSON:", err)
					return err
				}

				importTable := etl.NewMySqlBatchImportTable(&data)
				importTable.Start()
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
