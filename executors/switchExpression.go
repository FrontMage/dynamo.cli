package executors

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/FrontMage/dynamo.cli/sqlparser"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

var doubleQuoteReg = regexp.MustCompile("^\"(.*?)\"$")

func tryParseInt(s string) interface{} {
	if intValue, err := strconv.Atoi(s); err == nil {
		return intValue
	} else if int64Value, err := strconv.ParseInt(s, 10, 64); err == nil {
		return int64Value
	} else if doubleQuoteReg.MatchString(s) {
		return strings.Trim(s, "\"")
	} else {
		return s
	}
}

func SwitchExpression(condition sqlparser.Condition) expression.ConditionBuilder {
	switch condition.Operator {
	case "=":
		return expression.Name(condition.Key).
			Equal(expression.Value(tryParseInt(condition.Value)))
	case ">":
		return expression.Name(condition.Key).
			GreaterThan(expression.Value(tryParseInt(condition.Value)))
	case "<":
		return expression.Name(condition.Key).
			LessThan(expression.Value(tryParseInt(condition.Value)))
	case ">=":
		return expression.Name(condition.Key).
			GreaterThanEqual(expression.Value(tryParseInt(condition.Value)))
	case "<=":
		return expression.Name(condition.Key).
			LessThanEqual(expression.Value(tryParseInt(condition.Value)))
	case "!=":
		return expression.Name(condition.Key).
			NotEqual(expression.Value(tryParseInt(condition.Value)))
	case " LIKE ":
		return expression.Name(condition.Key).Contains(condition.Value)
	default:
		return expression.Name(condition.Key).Equal(expression.Value(condition.Value))
	}
}
