package opentick

import (
	"github.com/alecthomas/participle"
	"github.com/alecthomas/participle/lexer"
)

var (
	sqlLexer = lexer.Must(lexer.Regexp(`(\s+)` +
		`|(?P<Keyword>(?i)SELECT|FROM|WHERE|LIMIT|AND|OR|IN|TABLE|INSERT|VALUES|INTO|CREATE|DELETE|ALTER|TYPE|ADD|DROP|RENAME|TRUE|FALSE)` +
		`|(?P<Ident>[a-zA-Z][a-zA-Z0-9_]*)` +
		`|(?P<Number>-?\d*\.?\d+([eE][-+]?\d+)?)` +
		`|(?P<String>'[^']*'|"[^"]*")` +
		`|(?P<Operators><>|!=|<=|>=|[-+*/%,.()=<>?])`,
	))
	sqlParser = participle.MustBuild(
		&Select{},
		participle.Lexer(sqlLexer),
		participle.Unquote(sqlLexer, "String"),
		participle.Upper(sqlLexer, "Keyword"),
	)
)

type Boolean bool

func (b *Boolean) Capture(values []string) error {
	*b = values[0] == "TRUE"
	return nil
}

// Select based on http://www.h2database.com/html/grammar.html
type Select struct {
	Selected *SelectExpression `"SELECT" @@`
	From     *string           `"FROM" @Ident`
	Where    *Expression       `[ "WHERE" @@ ]`
	Limit    *int64            `[ "LIMIT" @Number ]`
}

type SelectExpression struct {
	All    *string   `@"*"`
	Fields []*string `| @Ident { "," @Ident }`
}

type Expression struct {
	Or []*AndCondition `@@ { "OR" @@ }`
}

type AndCondition struct {
	And []*ConditionOperand `@@ { "AND" @@ }`
}

type ConditionOperand struct {
	Operand      *Operand      `@@`
	ConditionRHS *ConditionRHS `[ @@ ]`
}

type ConditionRHS struct {
	Compare *Compare   `  @@`
	In      []*Operand `| "IN" "(" @@ { "," @@ } ")"`
}

type Compare struct {
	Operator *string  `@( "<>" | "<=" | ">=" | "=" | "<" | ">" | "!=" )`
	RHS      *Operand `@@`
}

type Operand struct {
	Summand []*Summand `@@ { "|" "|" @@ }`
}

type Summand struct {
	LHS *Factor `@@`
	Op  *string `[ @("+" | "-")`
	RHS *Factor `@@ ]`
}

type Factor struct {
	LHS *Term   `@@`
	Op  *string `[ @("*" | "/" | "%")`
	RHS *Term   `@@ ]`
}

type Term struct {
	Symbol        *string     `@Ident @{ "." Ident }`
	Value         *Value      `| @@`
	SubExpression *Expression `| "(" @@ ")"`
}

type Value struct {
	Wildcard *string  `@"*"`
	Number   *float64 `| @Number`
	String   *string  `| @String`
	Boolean  *Boolean `| @("TRUE" | "FALSE")`
	Null     *string  `| @"NULL"`
}

func Parse(sql string) (*Select, error) {
	expr := &Select{}
	err := sqlParser.ParseString(sql, expr)
	return expr, err
}
