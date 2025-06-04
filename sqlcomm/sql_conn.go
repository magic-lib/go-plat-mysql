package sqlcomm

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// MysqlConnect 建立mysql连接
func MysqlConnect(dsn string, timeout time.Duration) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("连接数据库失败: %w", err)
	}
	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		return db, fmt.Errorf("数据库连接测试失败: %s %w", dsn, err)
	}
	return db, nil
}
