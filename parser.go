package opentick

import (
	"github.com/alecthomas/participle"
	"github.com/alecthomas/participle/lexer"
)

var (
	sqlLexer = lexer.Must(lexer.Regexp(`(\s+)` +
		`|(?P<Keyword>(?i)\b(TIMESTAMP|DATABASE|BOOLEAN|PRIMARY|SMALLINT|TINYINT|BIGINT|DOUBLE|SELECT|INSERT|VALUES|CREATE|DELETE|RENAME|FLOAT|WHERE|LIMIT|TABLE|ALTER|FALSE|TEXT|FROM|TYPE|DROP|TRUE|INTO|ADD|AND|KEY|INT)\b)` +
		`|(?P<Ident>[a-zA-Z][a-zA-Z0-9_]*)` +
		`|(?P<Number>-?\d*\.?\d+([eE][-+]?\d+)?)` +
		`|(?P<String>'[^']*'|"[^"]*")` +
		`|(?P<Operator><=|>=|[-+*/%,.()=<>?])`,
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
	Delete *AstDelete `| "DELETE" @@`
}

type AstDrop struct {
	Table    *AstTableName `"TABLE" @@`
	Database *string       `| "DATABASE" @Ident`
}

type AstCreate struct {
	Table    *AstCreateTable `"TABLE" @@`
	Database *string         `| "DATABASE" @Ident`
}

type AstDelete struct {
	Table *AstTableName  `"FROM" @@`
	Where *AstExpression `["WHERE" @@]`
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
	And []AstCondition `@@ {"AND" @@}`
}

type AstCondition struct {
	LHS      *string   `@Ident`
	Operator *string   `@("<=" | ">=" | "=" | "<" | ">")`
	RHS      *AstValue `@@`
}

type AstValue struct {
	Number      *float64    `@Number`
	String      *string     `| @String`
	Placeholder *string     `| "?"`
	Boolean     *AstBoolean `| @("TRUE" | "FALSE")`
}

func Parse(sql string) (*Ast, error) {
	expr := &Ast{}
	err := sqlParser.ParseString(sql, expr)
	return expr, err
}
