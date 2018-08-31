package opentick

import (
	"github.com/alecthomas/participle"
	"github.com/alecthomas/participle/lexer"
)

var (
	sqlLexer = lexer.Must(lexer.Regexp(`(\s+)` +
		`|(?P<Keyword>(?i)TIMESTAMP|DATABASE|BOOLEAN|PRIMARY|SMALLINT|TINYINT|BIGINT|DOUBLE|SELECT|INSERT|VALUES|CREATE|DELETE|RENAME|FLOAT|WHERE|LIMIT|TABLE|ALTER|FALSE|TEXT|FROM|TYPE|DROP|TRUE|INTO|ADD|AND|KEY|INT|OR|IN)` +
		`|(?P<Ident>[a-zA-Z][a-zA-Z0-9_]*)` +
		`|(?P<Number>-?\d*\.?\d+([eE][-+]?\d+)?)` +
		`|(?P<String>'[^']*'|"[^"]*")` +
		`|(?P<Operators><>|!=|<=|>=|[-+*/%,.()=<>?])`,
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
	Name   *AstTableName `@@`
	Fields []AstTypeDef  `"(" @@ {"," @@} ")"`
}

type AstTypeDef struct {
	Key  []string `"PRIMARY" "KEY" "(" @Ident {"," @Ident} ")"`
	Name *string  `| @Ident`
	Type *string  `@{"BIGINT" | "TINYINT" | "SMALLINT" | "INT"  | "DOUBLE" | "FLOAT" | "TIMESTAMP" | "BOOLEAN" | "TEXT"}`
}

type AstInsert struct {
	Table  *AstTableName `"INTO" @@`
	Fields []string      `"(" @Ident {"," @Ident} ")"`
	Values []AstValue    `"VALUES" "(" @@ {"," @@} ")"`
}

type AstTableName struct {
	A *string `@Ident`
	B *string `["." @Ident]`
}

type AstSelect struct {
	Selected *AstSelectExpression `@@`
	From     *AstTableName        `"FROM" @@`
	Where    *AstExpression       `["WHERE" @@]`
	Limit    *int64               `["LIMIT" @Number]`
}

type AstSelectExpression struct {
	All    *string  `@"*"`
	Fields []string `| @Ident {"," @Ident}`
}

type AstExpression struct {
	Or []AstAndCondition `@@ {"OR" @@}`
}

type AstAndCondition struct {
	And []AstConditionOperand `@@ {"AND" @@}`
}

type AstConditionOperand struct {
	LHS          *AstTerm         `@@`
	ConditionRHS *AstConditionRHS `[@@]`
}

type AstConditionRHS struct {
	Compare *AstCompare `@@`
	In      []AstTerm   `| "IN" "(" @@ {"," @@} ")"`
}

type AstCompare struct {
	Operator *string     `@("<>" | "<=" | ">=" | "=" | "<" | ">" | "!=")`
	RHS      *AstSummand `@@`
}

type AstSummand struct {
	LHS *AstFactor `@@`
	Op  *string    `[@("+" | "-")`
	RHS *AstFactor `@@]`
}

type AstFactor struct {
	LHS *AstTerm `@@`
	Op  *string  `[@("*" | "/" | "%")`
	RHS *AstTerm `@@]`
}

type AstTerm struct {
	Symbol        *string        `@Ident`
	Value         *AstValue      `| @@`
	SubExpression *AstExpression `| "(" @@ ")"`
}

type AstValue struct {
	Number  *float64    `@Number`
	String  *string     `| @String`
	Boolean *AstBoolean `| @("TRUE" | "FALSE")`
}

func Parse(sql string) (*Ast, error) {
	expr := &Ast{}
	err := sqlParser.ParseString(sql, expr)
	return expr, err
}
