package main_test

import (
	"encoding/json"
	"fmt"
	"github.com/magic-lib/go-plat-mysql/etl"
	"os"
	"testing"
)

func TestMysqlImportExport(t *testing.T) {
	// 打开 JSON 文件
	file, err := os.Open("credit.json")
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

	// 然后更新
	// order_type
	// company_no
	// loan_finish_num
	// account_member_id
	// account_user_id
	// account_id
	// member_id
	// member_group
	/*
		UPDATE audit_order JOIN `allinone-order`.orderinfo ON audit_order.order_id = orderinfo.id SET
		audit_order.account_member_id = orderinfo.member_id,
			audit_order.member_id = orderinfo.member_id,
			audit_order.account_user_id = orderinfo.user_id,
			audit_order.account_id = orderinfo.apply_account_id,
			audit_order.member_group = orderinfo.member_group,
			audit_order.company_no = orderinfo.company_no,
			audit_order.loan_finish_num = 0,
			audit_order.order_type = orderinfo.order_type where audit_order.order_id >0;

		//audit_review_order_log
		UPDATE audit_review_order_log log JOIN audit_order ON log.order_id = audit_order.order_id SET log.audit_order_id = audit_order.audit_order_id where log.order_id >0;
	*/
}
