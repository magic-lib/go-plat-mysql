package etl

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/magic-lib/go-plat-mysql/sqlcomm"
	"github.com/magic-lib/go-plat-startupcfg/startupcfg"
	"time"
)

// mysqlDataSource mysql数据源
type mysqlDataSource struct {
	Dsn         string                  `json:"dsn"`
	ConnCfg     *startupcfg.MysqlConfig `json:"conn_cfg"`
	dbConn      *sql.DB
	connTimeout time.Duration
}

func newMysqlDataSource(m *mysqlDataSource) (*mysqlDataSource, error) {
	if m.ConnCfg == nil && m.Dsn == "" {
		return nil, fmt.Errorf("请设置数据库连接信息")
	}
	if m.Dsn == "" {
		m.Dsn = m.ConnCfg.DatasourceName()
	}
	m.connTimeout = 10 * time.Second

	db, err := m.Connect()
	if err != nil {
		return nil, fmt.Errorf("连接数据库失败: %w", err)
	}
	m.dbConn = db

	return m, nil
}

func (m *mysqlDataSource) Connect() (*sql.DB, error) {
	if m.dbConn != nil {
		return m.dbConn, nil
	}
	db, err := sqlcomm.MysqlConnect(m.Dsn, m.connTimeout)
	if err != nil {
		return nil, fmt.Errorf("连接数据库创建失败: %w", err)
	}
	m.dbConn = db
	return m.dbConn, nil
}
