package xorms

import (
	"context"
	"fmt"
	"github.com/magic-lib/go-plat-startupcfg/startupcfg"
	"github.com/magic-lib/go-plat-utils/logs"
	"xorm.io/xorm"
)

func checkInitDB(ctx context.Context, child any, co *startupcfg.MysqlConfig, isPanic bool) *Dao {
	obj, ok := child.(interface {
		initDB(ctx context.Context, co *startupcfg.MysqlConfig) (*Dao, error)
	})
	if !ok { //不包含该方法
		return nil
	}
	ret, err := obj.initDB(ctx, co)
	if ret == nil || err != nil {
		logs.CtxLogger(ctx).Error("InitDatabase error:", co)
		if isPanic {
			panic(any(co))
		}
		return nil
	}
	//设置默认日志
	ret.setLogger()
	return ret
}

// InitXormEngine 对外调用继承Dao的对象，进行数据库连接初始化
func InitXormEngine(ctx context.Context, child any, con *startupcfg.MysqlConfig, isPanic ...bool) (*xorm.Engine, error) {
	isPanicBool := false
	if len(isPanic) >= 1 {
		isPanicBool = isPanic[0]
	}
	ret := checkInitDB(ctx, child, con, isPanicBool)
	if ret == nil {
		logs.CtxLogger(ctx).Error("child error:", con)
		return nil, fmt.Errorf("InitDatabase child error")
	}
	return ret.GetEngine()
}
