package xorms

var (
	explainSql         = false                                       //执行分析索引命中的情况
	operatorList       = []string{"LIKE", "=", ">=", ">", "<=", "<"} // 数据库支持的类型
	likeUseReplaceList = []string{"%", "_"}                          //like需要替换的字符
	likeUseEscapeList  = []string{"/", "&", "#", "@", "^", "$", "!"} //定义可以使用的escape列表
)

// SetExplainSql 设置是否需要调试
func SetExplainSql(explain bool) {
	explainSql = explain
}
