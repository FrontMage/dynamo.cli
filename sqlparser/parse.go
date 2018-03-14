package sqlparser

import (
	"regexp"
	"strconv"
	"strings"
)

var SelectRegexp = regexp.MustCompile(`(?i)^(SELECT) `)
var FromRegexp = regexp.MustCompile("(?i) ?(FROM) ?")
var WhereRegexp = regexp.MustCompile("(?i) ?(WHERE) ?")
var LimitRegexp = regexp.MustCompile("(?i) ?(LIMIT) ?")
var EndRegexp = regexp.MustCompile("(?i) ?(END) ?")
var UpdateRegexp = regexp.MustCompile(" ?(UPDATE) ?")
var setRegexp = regexp.MustCompile("(?i) ?(SET) ?")
var returningRegexp = regexp.MustCompile("(?i) ?(RETRUNING) ?")
var DescRegexp = regexp.MustCompile(`(?i)^(DESC) `)
var TableRegexp = regexp.MustCompile("(?i) ?(TABLE) ?")

var KeywordRegexps = []*regexp.Regexp{
	SelectRegexp, FromRegexp, WhereRegexp, LimitRegexp,
	UpdateRegexp, setRegexp, returningRegexp, EndRegexp,
	TableRegexp,
}

var selectStmtRegexp = regexp.MustCompile("(?i)(SELECT )(.*?)( FROM)")
var FromStmtRegexp = regexp.MustCompile("(?i)(FROM )(.*?)(( WHERE)|( LIMIT)|( END))")
var WhereStmtRegexp = regexp.MustCompile("(?i)(WHERE )(.*?)(( LIMIT)|( END))")
var LimitStmtRegexp = regexp.MustCompile("(?i)(LIMIT )(.*?)( END)")

var updateStmtRegexp = regexp.MustCompile("(?i)(UPDATE )(.*?)( SET)")
var setStmtRegexp = regexp.MustCompile("(?i)(SET )(.*?)(( WHERE)|( RETRUNING)|( END))")
var whereStmtRegexpForUpdate = regexp.MustCompile("(?i)(WHERE )(.*?)(( RETRUNING)|( END))")
var returningStmtRegexp = regexp.MustCompile("(?i)(RETRUNING )(.*?)( END)")

var TableStmtRegexp = regexp.MustCompile("(?i)(TABLE )(.*?)( END)")

const (
	OpEq   = "="
	opGt   = ">"
	opLt   = "<"
	opGtEq = ">="
	opLtEq = "<="
	opNeq  = "!="
	opLike = " LIKE "
	// TODO support between
)

var ops = []string{opGtEq, opLtEq, opNeq, OpEq, opGt, opLt, opLike}

// SelectStatement holds all key information parsed from a sql select statement
// SelectStatement AttributesToGet is the part between SELECT and FROM
// SelectStatement Conditions is the part between WHERE and LIMIT or END
type SelectStatement struct {
	AttributesToGet []string
	TableName       string
	Conditions      []Condition
	Limit           int64
}

// UpdateStatement holds all key information parsed from a sql select statement
// UpdateStatement AttributesToGet is the part between RETRUNING and END
// UpdateStatement Conditions is the part between WHERE and RETRUNING or END
type UpdateStatement struct {
	AttributesToGet   []string
	UpdateExpressions []UpdateExpression
	TableName         string
	Conditions        []Condition
}

type UpdateExpression struct {
	Key   string
	Value string
}

type Condition struct {
	Key                 string
	Operator            string
	Value               string
	NextLogicalOperator string
}

// DescTableStatement holds all key information parsed from a sql describe table statement
// DescTableStatement which is the table name between TALBE and END
type DescTableStatement struct {
	TableName string
}

func killAllKeyWords(s string) string {
	for _, r := range KeywordRegexps {
		s = r.ReplaceAllString(s, "")
	}
	return strings.TrimSpace(s)
}

func parseAttributesToGet(attributesToGetStr string) []string {
	attributesToGet := []string{}
	for _, t := range strings.Split(attributesToGetStr, ",") {
		if strings.Contains(t, ",") {
			for _, s := range strings.Split(t, ",") {
				attributesToGet = append(attributesToGet, strings.TrimSpace(s))
			}
		} else {
			attributesToGet = append(attributesToGet, strings.TrimSpace(t))
		}
	}
	return attributesToGet
}

func switchCondition(condition, nextLogicalOperator string) Condition {
	// TODO support OR
	// TODO last one does not need AND
	for _, op := range ops {
		if strings.Contains(condition, op) {
			tokens := strings.Split(condition, op)
			return Condition{
				Key:                 strings.TrimSpace(tokens[0]),
				Operator:            op,
				Value:               strings.TrimSpace(tokens[1]),
				NextLogicalOperator: nextLogicalOperator,
			}
		}
	}
	return Condition{
		Key:                 "",
		Operator:            "=",
		Value:               "",
		NextLogicalOperator: nextLogicalOperator,
	}
}

// ParseSelect parse a select sql statement to go struct
// ParseSelect can only parse select, other statement will go wrong
func ParseSelect(selectSQL string) SelectStatement {
	// TODO trim all space
	attributesToGetStr := killAllKeyWords(selectStmtRegexp.FindString(selectSQL))
	tableName := killAllKeyWords(FromStmtRegexp.FindString(selectSQL))
	conditionStr := killAllKeyWords(WhereStmtRegexp.FindString(selectSQL))
	limitStr := killAllKeyWords(LimitStmtRegexp.FindString(selectSQL))
	// TODO support OFFSET

	// TODO match OR
	conditions := strings.Split(conditionStr, " AND ")
	stmt := SelectStatement{
		AttributesToGet: parseAttributesToGet(attributesToGetStr),
		TableName:       tableName,
		Conditions:      []Condition{},
	}
	// if there is a limit statement, use it instead default 1
	if limit, err := strconv.Atoi(limitStr); err == nil {
		stmt.Limit = int64(limit)
	} else if limitStr == "ALL" {
		stmt.Limit = -1
	} else {
		stmt.Limit = 1
	}
	if conditionStr != "" {
		for _, c := range conditions {
			stmt.Conditions = append(stmt.Conditions, switchCondition(c, "AND"))
		}
	}
	return stmt
}

// ParseUpdate parse an update SQL string to UpdateStatement, mainly just extract tokens
func ParseUpdate(updateSQL string) UpdateStatement {
	attributesToGetStr := killAllKeyWords(returningStmtRegexp.FindString(updateSQL))
	updateStr := killAllKeyWords(setStmtRegexp.FindString(updateSQL))
	tableName := killAllKeyWords(updateStmtRegexp.FindString(updateSQL))
	conditionStr := killAllKeyWords(whereStmtRegexpForUpdate.FindString(updateSQL))
	stmt := UpdateStatement{
		AttributesToGet:   parseAttributesToGet(attributesToGetStr),
		TableName:         tableName,
		Conditions:        []Condition{},
		UpdateExpressions: []UpdateExpression{},
	}
	if updateStr != "" {
		for _, u := range strings.Split(updateStr, ",") {
			tokens := strings.Split(u, "=")
			stmt.UpdateExpressions = append(stmt.UpdateExpressions, UpdateExpression{
				Key:   tokens[0],
				Value: tokens[1],
			})
		}
	}
	if conditionStr != "" {
		for _, c := range strings.Split(conditionStr, " AND ") {
			stmt.Conditions = append(stmt.Conditions, switchCondition(c, "AND"))
		}
	}
	return stmt
}

// ParseDescTable parse a describe table SQL string to DescTableStatement, extract table name
func ParseDescTable(descTableSQL string) DescTableStatement {
	return DescTableStatement{
		TableName: killAllKeyWords(TableStmtRegexp.FindString(descTableSQL)),
	}
}
