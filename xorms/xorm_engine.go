package xorms

import (
	"fmt"
	"github.com/magic-lib/go-plat-startupcfg/startupcfg"
	"github.com/magic-lib/go-plat-utils/crypto"
	"github.com/magic-lib/go-plat-utils/logs"
	"log"
	"runtime"
	"sync"
	"time"
	"xorm.io/xorm"
	//需要引入默认的mysql数据驱动
	_ "github.com/go-sql-driver/mysql"
	cmap "github.com/orcaman/concurrent-map/v2"
)

type allEngine struct {
	engineList   cmap.ConcurrentMap[string, *xorm.Engine]            //保存了所有engine列表
	connectList  cmap.ConcurrentMap[string, *startupcfg.MysqlConfig] //保存了所有连接
	runCheckOnce sync.Once
	lockMutex    sync.Mutex
	initOnce     sync.Once
}

func (m *allEngine) init() {
	m.initOnce.Do(func() {
		m.engineList = cmap.New[*xorm.Engine]()
		m.connectList = cmap.New[*startupcfg.MysqlConfig]()
	})
}

// NewEnginePool 初始化
func NewEnginePool() *allEngine {
	pool := new(allEngine)
	pool.init()
	return pool
}

// GetEngine 获取一个
func (m *allEngine) GetEngine(con *startupcfg.MysqlConfig) (*xorm.Engine, error) {
	if con == nil {
		return nil, fmt.Errorf("con is nil")
	}
	//初始化
	m.init()

	cacheKey := m.getCacheKey(con)
	//先尝试从缓存中获取
	engineTemp, has := m.engineList.Get(cacheKey)
	if has {
		return engineTemp, nil
	}

	m.lockMutex.Lock()
	defer m.lockMutex.Unlock()
	engine, err := m.getNewEngine(con)
	if err != nil {
		return nil, err
	}

	m.engineList.Set(cacheKey, engine)
	m.connectList.Set(cacheKey, con)

	//运行连接池里的检测，只需要执行一次
	m.runCheckOnce.Do(func() {
		m.monitorEngine()
	})

	return engine, nil
}

func (m *allEngine) getNewEngine(con *startupcfg.MysqlConfig) (*xorm.Engine, error) {
	dsn := con.DatasourceName()
	engine, err := xorm.NewEngine("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("xorm.NewEngine error:%s", err.Error())
	}
	err = engine.Ping()
	if err != nil {
		return nil, fmt.Errorf("engine ping error:%s", err.Error())
	}

	// 设置连接池的最大空闲连接数
	engine.SetMaxIdleConns(10) //默认为2
	// 设置连接池的最大打开连接数
	//engine.SetMaxOpenConns(100) //默认无限
	//engine.SetConnMaxLifetime(time.Minute * 3)
	engine.SetConnMaxLifetime(0) //设置 packets.go:123: closing bad idle connection: EOF
	return engine, nil
}

func (m *allEngine) removeByKey(key string) error {
	if engine, has := m.engineList.Get(key); has {
		err := engine.Close()
		if err != nil {
			return err //关闭出错，不能删除
		}
		m.engineList.Remove(key)
	}
	m.connectList.Remove(key)
	return nil
}

func (m *allEngine) monitorEngine() {
	//当dbInstanceMap销毁时，则需要断开连接
	runtime.SetFinalizer(m.engineList, func(engList cmap.ConcurrentMap[string, *xorm.Engine]) {
		if engList.IsEmpty() {
			return
		}
		for key, value := range engList.Items() {
			err := value.Close()
			if err != nil {
				logs.DefaultLogger().Error("runCheckEngine SetFinalizer close error:", err)
			}
			engList.Remove(key)
		}
	})

	//每7分钟检测一次
	ticker := time.NewTicker(7 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			for key, engine := range m.engineList.Items() {
				if err := engine.Ping(); err != nil { //连接不通的情况
					connTemp, has := m.connectList.Get(key)
					if !has {
						//该配置已经删除了,所以这里也需要删除
						_ = m.removeByKey(key)
						continue
					}
					newEngine, err := m.getNewEngine(connTemp)
					if err != nil {
						log.Printf("Failed to reconnect: %v", err)
						continue
					}
					_ = engine.Close()   //关闭旧的
					*engine = *newEngine //重新赋值
				}
			}

		}
	}
}

func (m *allEngine) getCacheKey(con *startupcfg.MysqlConfig) string {
	return crypto.Md5(con.DatasourceName())
}
