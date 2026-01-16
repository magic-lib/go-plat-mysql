package sqlstatement_test

import (
	"database/sql"
	"fmt"
	"github.com/Masterminds/squirrel"
	"github.com/magic-lib/go-plat-mysql/sqlstatement"
	"github.com/magic-lib/go-plat-utils/conv"
	"testing"
	"time"
)

func TestGenerateWhereClause(t *testing.T) {
	sta := new(sqlstatement.Statement)

	sqlStr, list := sta.GenerateWhereClause(sqlstatement.LogicCondition{
		Conditions: []any{
			sqlstatement.Condition{
				Field:    "name",
				Value:    "test",
				Operator: "=",
			},
			sqlstatement.Condition{
				Field:    "age",
				Value:    18,
				Operator: "=",
			},
			sqlstatement.LogicCondition{
				Conditions: []any{
					sqlstatement.Condition{
						Field:    "name",
						Value:    "test",
						Operator: "=",
					},
					sqlstatement.Condition{
						Field:    "age",
						Value:    18,
						Operator: "=",
					},
				},
				Operator: "or",
			},
		},
		Operator: "and",
	})

	fmt.Println(sqlStr, list)

	sqlStr, list = sta.GenerateWhereClause(sqlstatement.LogicCondition{
		Conditions: []any{
			sqlstatement.LogicCondition{
				Conditions: []any{
					sqlstatement.Condition{
						Field:    "name",
						Value:    "test",
						Operator: "=",
					},
					sqlstatement.Condition{
						Field:    "age",
						Value:    18,
						Operator: "=",
					},
				},
				Operator: "and",
			},
			sqlstatement.LogicCondition{
				Conditions: []any{
					sqlstatement.Condition{
						Field:    "name",
						Value:    "test",
						Operator: "=",
					},
					sqlstatement.Condition{
						Field:    "age",
						Value:    18,
						Operator: "=",
					},
				},
				Operator: "or",
			},
		},
		Operator: "or",
	})

	fmt.Println(sqlStr, list)

	sqlStr, list = sta.GenerateWhereClause(sqlstatement.LogicCondition{
		Conditions: []any{
			sqlstatement.Condition{
				Field:    "name",
				Value:    "test",
				Operator: "=",
			},
		},
		Operator: "or",
	})

	fmt.Println(sqlStr, list)

}

type AgeKey struct {
	Name *string "json:`name`"
	Age  int     "json:`age`"
}

func TestGenerateWhereClause1(t *testing.T) {

	aa := new(AgeKey)
	conv.Unmarshal(map[string]any{
		"name": "bbbb",
	}, aa)

	if aa.Name == nil {
		fmt.Println("3333")
	}

	fmt.Println(conv.String(aa))
}
func TestGetTableName(t *testing.T) {
	aa := AgeKey{}
	a, b, e := sqlstatement.StructToColumnsAndValues(aa, "snake", "json")
	fmt.Println(a, b, e)
}
func TestInsertSql(t *testing.T) {
	sqlObj := sqlstatement.NewSqlStruct(sqlstatement.SetTableName("`kkkk`"))

	mm := "bbbb"
	a, b, e := sqlObj.InsertSql(&AgeKey{
		Age:  12,
		Name: &mm,
	})
	fmt.Println(a, b, e)

	a, b, e = sqlObj.DeleteSql(sqlstatement.LogicCondition{
		Conditions: []any{
			sqlstatement.Condition{
				Field:    "1",
				Operator: "=",
				Value:    "1",
			},
		},
	})
	fmt.Println(a, b, e)
	a, b, e = sqlObj.UpdateSql(&AgeKey{
		Age:  12,
		Name: &mm,
	}, []string{"name"}, sqlstatement.LogicCondition{
		Conditions: []any{
			sqlstatement.Condition{
				Field:    "1",
				Operator: "=",
				Value:    "1",
			},
		},
	})
	fmt.Println(a, b, e)
	a, b, e = sqlObj.SelectSql("", sqlstatement.LogicCondition{
		Conditions: []any{
			sqlstatement.Condition{
				Field:    "1",
				Operator: "=",
				Value:    "1",
			},
		},
	}, 0, 0)
	fmt.Println(a, b, e)
}

type Table1 struct {
	CreateTime sql.NullTime   `db:"create_time" json:"create_time"`
	Name       sql.NullString `db:"name" json:"name"`
}

func TestNullString(t *testing.T) {
	aa := Table1{
		CreateTime: sql.NullTime{Time: time.Now()},
		Name:       sql.NullString{String: "aaaaa"},
	}

	sqlBuilder := sqlstatement.NewSqlStruct(
		sqlstatement.SetColumnTagName("db"),
		sqlstatement.SetStructData(Table1{}),
		sqlstatement.SetTableName("table1"),
	)

	query, columnDataList, err := sqlBuilder.InsertSql(aa)

	fmt.Println(query)
	fmt.Println(columnDataList, err)
}
func TestNullStringList(t *testing.T) {
	aa := []Table1{
		{
			CreateTime: sql.NullTime{
				Time:  time.Now(),
				Valid: true,
			},
			Name: sql.NullString{
				String: "aaaaa",
				Valid:  true,
			},
		},
		{
			CreateTime: sql.NullTime{
				Time:  time.Now(),
				Valid: true,
			},
			Name: sql.NullString{
				String: "bbbbb",
				Valid:  true,
			},
		},
	}

	sqlBuilder := sqlstatement.NewSqlStruct(
		sqlstatement.SetColumnTagName("db"),
		sqlstatement.SetStructData(Table1{}),
		sqlstatement.SetTableName("table1"),
	)

	query, columnDataList, err := sqlBuilder.InsertSql(aa)

	fmt.Println(query)
	fmt.Println(columnDataList, err)
}
func TestInsertIgnore(t *testing.T) {
	allValues := make([][]any, 0)
	allValues = append(allValues, []any{"test", 18})
	allValues = append(allValues, []any{"test2", 20})
	stmt := squirrel.Insert("users").Options("IGNORE").Columns("name", "age")
	for _, row := range allValues {
		stmt = stmt.Values(row...)
	}

	query, columnDataList, err := stmt.ToSql()

	fmt.Println(query)
	fmt.Println(columnDataList, err)
}
