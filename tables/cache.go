package tables

import (
	"sync"

	"github.com/FrontMage/dynamo.cli/db"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// TODO since there is no support of alter table, maybe a force remote fetch flat is not needed now, but I suppose one day will.

var mutex = sync.Mutex{}

// TableInfoCache keys are table name, values are dynamodb DescribeTableOutput
// TableInfoCache is a cache for table info, reduce request times
var TableInfoCache = map[string]*dynamodb.DescribeTableOutput{}

// GetTableDesc returns the table info and updates the table info cache
func GetTableDesc(tableName *string) (*dynamodb.DescribeTableOutput, error) {
	if TableInfoCache[*tableName] != nil {
		return TableInfoCache[*tableName], nil
	} else {
		if result, err := db.DynamoDB.DescribeTable(&dynamodb.DescribeTableInput{TableName: tableName}); err == nil {
			mutex.Lock()
			TableInfoCache[*tableName] = result
			mutex.Unlock()
			return result, nil
		} else {
			return result, err
		}
	}
}
