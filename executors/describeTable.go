package executors

import (
	"encoding/json"
	"errors"

	"dynamo.cli/sqlparser"
	"dynamo.cli/tables"
)

// DescribeTable returns the basic info to describe the given table
func DescribeTable(descTableSQL string) (string, error) {
	tableName := sqlparser.ParseDescTable(descTableSQL).TableName
	if tableName == "" {
		return "", errors.New("Can't find table name, check your inputs")
	}
	if tableInfo, err := tables.GetTableDesc(&tableName); err == nil {
		if jsonString, err := json.MarshalIndent(tableInfo, "", "  "); err == nil {
			return string(jsonString), nil
		} else {
			return "", err
		}
	} else {
		return "", err
	}
}
