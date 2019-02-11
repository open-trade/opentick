package opentick

import (
	"github.com/alecthomas/participle"
	"github.com/alecthomas/participle/lexer"
	"strconv"
)

var (
	sqlLexer = lexer.Must(lexer.Regexp(`(\s+)` +
		`|(?P<Keyword>(?i)\b(TIMESTAMP|DATABASE|BOOLEAN|PRIMARY|SMALLINT|TINYINT|BIGINT|DOUBLE|SELECT|INSERT|VALUES|CREATE|DELETE|RENAME|FLOAT|WHERE|LIMIT|TABLE|ALTER|FALSE|TEXT|FROM|TYPE|DROP|TRUE|INTO|ADD|AND|KEY|INT|IF|NOT|EXISTS)\b)` +
		`|(?P<Func>(?i)\b(ADJ)\b)` +
		`|(?P<Ident>[a-zA-Z][a-zA-Z0-9_]*)` +
		`|(?P<Number>-?\d+\.?\d*([eE][-+]?\d+)?)` +
		`|(?P<String>'[^']*'|"[^"]*")` +
		`|(?P<Operator><=|>=|[-+*/%,.()=<>?])`,
	))
	sqlParser = participle.MustBuild(
		&Ast{},
		participle.Lexer(sqlLexer),
		participle.Unquote(sqlLexer, "String"),
		participle.Upper(sqlLexer, "Keyword", "Func"),
	)
)

type AstBoolean bool

func (self *AstBoolean) Capture(values []string) error {
	*self = values[0] == "TRUE"
	return nil
}

type AstNumber struct {
	Float *float64
	Int   *int64
}

func (self *AstNumber) Capture(values []string) error {
	v1, err := strconv.ParseInt(values[0], 10, 64)
	if err == nil {
		self.Int = &v1
	} else {
		v2, _ := strconv.ParseFloat(values[0], 64)
		self.Float = &v2
	}
	return nil
}

type Ast struct {
	Select *AstSelect `"SELECT" @@`
	Insert *AstInsert `| "INSERT" @@`
	Create *AstCreate `| "CREATE" @@`
	Drop   *AstDrop   `| "DROP" @@`
	Delete *AstDelete `| "DELETE" @@`
}

type AstDrop struct {
	Table    *AstTableName `"TABLE" @@`
	Database *string       `| "DATABASE" @Ident`
}

type AstCreate struct {
	Table    *AstCreateTable    `"TABLE" @@`
	Database *AstCreateDatabase `| "DATABASE" @@`
}

type AstDelete struct {
	Table *AstTableName  `"FROM" @@`
	Where *AstExpression `["WHERE" @@]`
}

type AstCreateDatabase struct {
	IfNotExists *string `[@("IF" "NOT" "EXISTS")]`
	Name        *string `@Ident`
}

type AstCreateTable struct {
	IfNotExists *string       `[@("IF" "NOT" "EXISTS")]`
	Name        *AstTableName `@@`
	Cols        []AstTypeDef  `"(" @@ {"," @@} ")"`
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
	Table    *AstTableName        `"FROM" @@`
	Where    *AstExpression       `["WHERE" @@]`
	Limit    *int64               `["LIMIT" @Number]`
}

type AstSelectExpression struct {
	All  *string        `@"*"`
	Cols []AstSelectCol `| @@ {"," @@}`
}

type AstSelectCol struct {
	Name *string        `@Ident`
	Func *AstSelectFunc `| @@`
}

type AstSelectFunc struct {
	Name *string `@Func "("`
	Col  *string `@Ident ")"`
}

type AstExpression struct {
	And []AstCondition `@@ {"AND" @@}`
}

type AstCondition struct {
	LHS      *string   `@Ident`
	Operator *string   `@("<=" | ">=" | "=" | "<" | ">")`
	RHS      *AstValue `@@`
}

type AstValue struct {
	Number      *AstNumber  `@Number`
	String      *string     `| @String`
	Placeholder *string     `| @"?"`
	Boolean     *AstBoolean `| @("TRUE" | "FALSE")`
}

func (self *AstValue) Value() interface{} {
	if self.Number != nil {
		if self.Number.Int != nil {
			return *self.Number.Int
		}
		return *self.Number.Float
	}
	if self.String != nil {
		return *self.String
	}
	if self.Boolean != nil {
		return (bool)(*self.Boolean)
	}
	return nil
}

func Parse(sql string) (*Ast, error) {
	expr := &Ast{}
	err := sqlParser.ParseString(sql, expr)
	return expr, err
}
