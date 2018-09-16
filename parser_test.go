package opentick

import (
	"github.com/alecthomas/repr"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

var sqlSelectStmt = "select a, b from test where (a > 1.2) and (b < 2 or (b in (1,2))) limit -2"
var sqlInsertStmt = "INSERT into x(x, y) values(1, 2)"
var sqlInsertAst = `
&opentick.Ast{
  Insert: &opentick.AstInsert{
    Table: &opentick.AstTableName{
      A: &"x",
    },
    Cols: []string{
      "x",
      "y",
    },
    Values: []opentick.AstValue{
      opentick.AstValue{
        Number: &1,
      },
      opentick.AstValue{
        Number: &2,
      },
    },
  },
}
`
var sqlSelectAst = `
&opentick.Ast{
  Select: &opentick.AstSelect{
    Selected: &opentick.AstSelectExpression{
      Cols: []string{
        "a",
        "b",
      },
    },
    From: &opentick.AstTableName{
      A: &"test",
    },
    Where: &opentick.AstExpression{
      And: []opentick.AstCondition{
        opentick.AstCondition{
          LHS: &"a",
          ConditionRHS: &opentick.AstConditionRHS{
            Compare: &opentick.AstCompare{
              Operator: &">",
              RHS: &opentick.AstValue{
                Number: &1.2,
              },
            },
          },
        },
        opentick.AstCondition{
          SubExpression: &opentick.AstExpression{
            Or: []opentick.AstCondition{
              opentick.AstCondition{
                LHS: &"b",
                ConditionRHS: &opentick.AstConditionRHS{
                  Compare: &opentick.AstCompare{
                    Operator: &"<",
                    RHS: &opentick.AstValue{
                      Number: &2,
                    },
                  },
                },
              },
              opentick.AstCondition{
                LHS: &"b",
                ConditionRHS: &opentick.AstConditionRHS{
                  In: []opentick.AstValue{
                    opentick.AstValue{
                      Number: &1,
                    },
                    opentick.AstValue{
                      Number: &2,
                    },
                  },
                },
              },
            },
          },
        },
      },
    },
    Limit: &-2,
  },
}
`

func Test_Parse(t *testing.T) {
	expr, err := Parse(sqlSelectStmt)
	expr.Select.Where.Reduce()
	// assert.Equal(t, repr.String(expr, repr.Indent("  "), repr.OmitEmpty(true)), strings.TrimSpace(sqlSelectAst))
	repr.Println(expr, repr.Indent("  "), repr.OmitEmpty(true))
	expr, err = Parse("select * from a where (((a=1 and (b=2.2 and ((c=3 and (d=3))))))) limit -2")
	expr.Select.Where.Reduce()
	expr.Select.Where.Flatten()
	repr.Println(expr, repr.Indent("  "), repr.OmitEmpty(true))
	assert.Equal(t, err, nil)
	expr, err = Parse(sqlInsertStmt)
	assert.Equal(t, repr.String(expr, repr.Indent("  "), repr.OmitEmpty(true)), strings.TrimSpace(sqlInsertAst))
}

func Benchmark_Parse(b *testing.B) {
	b.StartTimer()
	for i := 0; i < b.N; i++ { //use b.N for looping
		_, err := Parse(sqlSelectStmt)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func Test_CreateTableSql(t *testing.T) {
	sqlCreateTable1 := `
	create table test.test(
		symbol_id bigint,
		interval int, 
  	tm timestamp,
		open double,
		high double,
		low double,
		close double,
		volume double,
		primary key (symbol_id, interval, tm)
	)
  `
	_, err := Parse(sqlCreateTable1)
	if err != nil {
		t.Error(err)
	}
}
