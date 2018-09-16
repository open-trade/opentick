package opentick

import (
	"github.com/alecthomas/participle"
	"github.com/alecthomas/participle/lexer"
)

var (
	sqlLexer = lexer.Must(lexer.Regexp(`(\s+)` +
		`|(?P<Keyword>(?i)\b(TIMESTAMP|DATABASE|BOOLEAN|PRIMARY|SMALLINT|TINYINT|BIGINT|DOUBLE|SELECT|INSERT|VALUES|CREATE|DELETE|RENAME|FLOAT|WHERE|LIMIT|TABLE|ALTER|FALSE|TEXT|FROM|TYPE|DROP|TRUE|INTO|ADD|AND|KEY|INT|OR|IN)\b)` +
		`|(?P<Ident>[a-zA-Z][a-zA-Z0-9_]*)` +
		`|(?P<Number>-?\d*\.?\d+([eE][-+]?\d+)?)` +
		`|(?P<String>'[^']*'|"[^"]*")` +
		`|(?P<Operator><>|!=|<=|>=|[-+*/%,.()=<>?])`,
	))
	sqlParser = participle.MustBuild(
		&Ast{},
		participle.Lexer(sqlLexer),
		participle.Unquote(sqlLexer, "String"),
		participle.Upper(sqlLexer, "Keyword"),
	)
)

type AstBoolean bool

func (b *AstBoolean) Capture(values []string) error {
	*b = values[0] == "TRUE"
	return nil
}

type Ast struct {
	Select *AstSelect `"SELECT" @@`
	Insert *AstInsert `| "INSERT" @@`
	Create *AstCreate `| "CREATE" @@`
	Drop   *AstDrop   `| "DROP" @@`
}

type AstDrop struct {
	Table    *AstTableName `"TABLE" @@`
	Database *string       `| "DATABASE" @Ident`
}

type AstCreate struct {
	Table    *AstCreateTable `"TABLE" @@`
	Database *string         `| "DATABASE" @Ident`
}

type AstCreateTable struct {
	Name *AstTableName `@@`
	Cols []AstTypeDef  `"(" @@ {"," @@} ")"`
}

type AstTypeDef struct {
	Key  []string `"PRIMARY" "KEY" "(" @Ident {"," @Ident} ")"`
	Name *string  `| @Ident`
	Type *string  `@{"BIGINT" | "TINYINT" | "SMALLINT" | "INT"  | "DOUBLE" | "FLOAT" | "TIMESTAMP" | "BOOLEAN" | "TEXT"}`
}

type AstInsert struct {
	Table  *AstTableName `"INTO" @@`
	Cols   []string      `"(" @Ident {"," @Ident} ")"`
	Values []AstValue    `"VALUES" "(" @@ {"," @@} ")"`
}

type AstTableName struct {
	A *string `@Ident`
	B *string `["." @Ident]`
}

func (self *AstTableName) TableName() string {
	if self.B == nil {
		return *self.A
	}
	return *self.B
}

func (self *AstTableName) DatabaseName() string {
	if self.B == nil {
		return ""
	}
	return *self.A
}

type AstSelect struct {
	Selected *AstSelectExpression `@@`
	From     *AstTableName        `"FROM" @@`
	Where    *AstExpression       `["WHERE" @@]`
	Limit    *int64               `["LIMIT" @Number]`
}

type AstSelectExpression struct {
	All  *string  `@"*"`
	Cols []string `| @Ident {"," @Ident}`
}

type AstExpression struct {
	OrAnd []AstAndCondition `@@ {"OR" @@}`
	And   []AstCondition
	Or    []AstCondition
}

type AstAndCondition struct {
	And []AstCondition `@@ {"AND" @@}`
}

type AstCondition struct {
	LHS           *string          `@Ident`
	ConditionRHS  *AstConditionRHS `[@@]`
	SubExpression *AstExpression   `| "(" @@ ")"`
}

type AstConditionRHS struct {
	Compare *AstCompare `@@`
	In      []AstValue  `| "IN" "(" @@ {"," @@} ")"`
}

type AstCompare struct {
	Operator *string   `@("<>" | "<=" | ">=" | "=" | "<" | ">" | "!=")`
	RHS      *AstValue `@@`
}

type AstValue struct {
	Number      *float64    `@Number`
	String      *string     `| @String`
	Placeholder *string     `| "?"`
	Boolean     *AstBoolean `| @("TRUE" | "FALSE")`
}

// reduce OrAnd
func (expr *AstExpression) Reduce() {
	for i := 0; i < len(expr.OrAnd); i++ {
		for j := 0; j < len(expr.OrAnd[i].And); j++ {
			c := &expr.OrAnd[i].And[j]
			if c.SubExpression != nil {
				c.SubExpression.Reduce()
			}
		}
	}
	if len(expr.OrAnd) == 1 {
		expr.And = expr.OrAnd[0].And
	} else {
		conds := make([]AstCondition, len(expr.OrAnd))
		for i := 0; i < len(expr.OrAnd); i++ {
			if len(expr.OrAnd[i].And) == 1 {
				conds[i] = expr.OrAnd[i].And[0]
			} else {
				tmp := &AstExpression{}
				tmp.And = expr.OrAnd[i].And
				conds[i].SubExpression = tmp
			}
		}
		expr.Or = conds
	}
	expr.OrAnd = nil
}

func (expr *AstExpression) Flatten() {
	if expr.OrAnd != nil {
		panic("Please Reduce first")
	}

	if expr.And != nil {
		for i := 0; i < len(expr.And); {
			if expr.And[i].SubExpression != nil {
				expr.And[i].SubExpression.Flatten()
				if expr.And[i].SubExpression.And != nil {
					and2 := expr.And[i].SubExpression.And
					expr.And[i] = and2[0]
					expr.And = append(expr.And, and2[1:]...)
					i += len(and2)
					continue
				} else {
				}
			}
			i += 1
		}
	} else {
		for i := 0; i < len(expr.Or); i++ {
			if expr.Or[i].SubExpression != nil {
				expr.Or[i].SubExpression.Flatten()
			}
		}
	}
}

func Parse(sql string) (*Ast, error) {
	expr := &Ast{}
	err := sqlParser.ParseString(sql, expr)
	return expr, err
}
