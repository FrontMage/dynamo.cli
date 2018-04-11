package executors

import (
	"errors"
	"strings"

	"github.com/FrontMage/dynamo.cli/db"
	"github.com/FrontMage/dynamo.cli/sqlparser"
	"github.com/FrontMage/dynamo.cli/tables"
	"github.com/FrontMage/dynamo.cli/utils"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

type tableBrief struct {
	keySchemas             []string
	globalSecondaryIndexes []tableIndex
	hashKey                string
	rangeKey               string
	itemCount              int64
}

type tableIndex struct {
	name  string
	field string
}

const (
	queryWithGlobalSecondaryIndex = "0"
	queryWithHashKey              = "1"
	unableToQuery                 = "2"
)

func getQueryMethod(globalSecondaryIndexes []tableIndex, hashKey string, conditions []sqlparser.Condition) (string, sqlparser.Condition, string) {
	queryMethod := unableToQuery
	relatedCondition := sqlparser.Condition{}
	relatedIndexName := ""
	for _, index := range globalSecondaryIndexes {
		for _, c := range conditions {
			if c.Key == index.field && c.Operator == sqlparser.OpEq {
				queryMethod = queryWithGlobalSecondaryIndex
				relatedCondition = c
				relatedIndexName = index.name
				break
			}
		}
	}
	for _, c := range conditions {
		if c.Key == hashKey && c.Operator == sqlparser.OpEq {
			queryMethod = queryWithHashKey
			relatedCondition = c
			break
		}

	}
	return queryMethod, relatedCondition, relatedIndexName
}

func buildFilterExpression(conditions []sqlparser.Condition, indexToUse string) expression.ConditionBuilder {
	var filterExpression expression.ConditionBuilder
	for idx, c := range conditions {
		// filter expression can only contains fields that is not primary key or index
		if !strings.HasPrefix(indexToUse, c.Key) {
			if idx == 0 {
				// if the value can be parse to number, use it as number
				filterExpression = SwitchExpression(c)
			} else {
				filterExpression = filterExpression.
					And(SwitchExpression(c))
			}
		}
	}
	return filterExpression
}

func buildProjection(attributesToGet []string) expression.ProjectionBuilder {
	params := []expression.NameBuilder{}
	for _, a := range attributesToGet {
		params = append(params, expression.Name(a))
	}
	if len(params) > 1 {
		return expression.NamesList(params[0], params[1:len(params)]...)
	} else {
		return expression.NamesList(params[0])
	}
}

func briefTable(desc *dynamodb.TableDescription) tableBrief {
	brief := tableBrief{}
	brief.itemCount = *desc.ItemCount
	for _, s := range desc.KeySchema {
		brief.keySchemas = append(brief.keySchemas, *s.AttributeName)
	}
	for _, i := range desc.GlobalSecondaryIndexes {
		brief.globalSecondaryIndexes = append(brief.globalSecondaryIndexes, tableIndex{
			name:  *i.IndexName,
			field: strings.Split(*i.IndexName, "-")[0],
		})
	}
	brief.hashKey = *desc.KeySchema[0].AttributeName
	if len(desc.KeySchema) > 1 {
		brief.rangeKey = *desc.KeySchema[1].AttributeName
	}
	return brief
}

func scan(stmt sqlparser.SelectStatement) (string, error) {
	scanInput := &dynamodb.ScanInput{
		TableName: &stmt.TableName,
		Limit:     &stmt.Limit,
	}
	if stmt.AttributesToGet[0] != "*" {
		scanInput.SetAttributesToGet(aws.StringSlice(stmt.AttributesToGet))
	}
	if result, err := db.DynamoDB.Scan(scanInput); err == nil {
		return utils.FormatPrettyListOfMap(result.Items), nil
	} else {
		return "", err
	}
}

func scanWithFilterUntilLimit(scanInput *dynamodb.ScanInput, limit int64,
	list []map[string]*dynamodb.AttributeValue) (string, error) {
	if result, err := db.DynamoDB.Scan(scanInput); err == nil {
		for _, i := range result.Items {
			if int64(len(list)) < limit {
				list = append(list, i)
			}
		}
		if int64(len(list)) < limit && result.LastEvaluatedKey != nil {
			scanInput.ExclusiveStartKey = result.LastEvaluatedKey
			return scanWithFilterUntilLimit(scanInput, limit, list)
		} else {
			return utils.FormatPrettyListOfMap(list), nil
		}
	} else {
		return "", err
	}
}

// TODO better structure
// Select executes selectSQL string by parsing to dynamodb api
func Select(selectSQL string) (string, error) {
	stmt := sqlparser.ParseSelect(selectSQL)
	if stmt.TableName == "" {
		return "", errors.New("Can't utils.Find table name, check your inputs")
	}
	if len(stmt.Conditions) == 0 {
		return scan(stmt)
	}

	// get table info
	if tableDesc, describeTableErr := tables.GetTableDesc(&stmt.TableName); describeTableErr == nil {

		tableInfo := briefTable(tableDesc.Table)
		conditionKeys := []string{}

		isAbleToGet := true
		for _, c := range stmt.Conditions {
			conditionKeys = append(conditionKeys, c.Key)
		}
		for _, schema := range tableInfo.keySchemas {
			schemaIdx := utils.FindIndex(conditionKeys, schema)
			isAbleToGet = isAbleToGet && schemaIdx != -1 && stmt.Conditions[schemaIdx].Operator == sqlparser.OpEq
		}
		// if key schema is satisfied use get
		if isAbleToGet {
			// TODO unable to use builder for get, checkout on stackoverflow
			key := map[string]*dynamodb.AttributeValue{}
			for _, c := range stmt.Conditions {
				switch tryParseInt(c.Value).(type) {
				case string:
					key[c.Key] = &dynamodb.AttributeValue{
						S: aws.String(c.Value),
					}
				case int:
					key[c.Key] = &dynamodb.AttributeValue{
						N: aws.String(c.Value),
					}
				case int64:
					key[c.Key] = &dynamodb.AttributeValue{
						N: aws.String(c.Value),
					}
				}
			}
			getItemInput := &dynamodb.GetItemInput{
				TableName: &stmt.TableName,
				Key:       key,
			}
			if stmt.AttributesToGet[0] != "*" {
				getItemInput.SetAttributesToGet(aws.StringSlice(stmt.AttributesToGet))
			}
			if result, err := db.DynamoDB.GetItem(getItemInput); err == nil {
				return utils.FormatPrettyMap(result.Item), err
			} else {
				return "", err
			}
			// if key schema is not satisfied, see if it's able to query
		} else {
			// build keyConditionExpression
			queryMethod, relatedCondition, indexToUse := getQueryMethod(tableInfo.globalSecondaryIndexes, tableInfo.hashKey, stmt.Conditions)

			// build filterExpression
			filterExpression := buildFilterExpression(stmt.Conditions, indexToUse)

			// build projection expression
			projectionExpression := buildProjection(stmt.AttributesToGet)

			// if it's able to query with index, use query
			if queryMethod != unableToQuery {
				keyConditionExpression := expression.Key(relatedCondition.Key).Equal(expression.Value(tryParseInt(relatedCondition.Value)))
				builder := expression.NewBuilder().
					WithKeyCondition(keyConditionExpression)
				// try use filter expression, if it's empty do not use it
				if len(stmt.Conditions) > 1 {
					builder = builder.WithFilter(filterExpression)
				}
				if stmt.AttributesToGet[0] != "*" {
					builder = builder.WithProjection(projectionExpression)
				}
				if expr, err := builder.Build(); err == nil {
					queryInput := &dynamodb.QueryInput{
						ExclusiveStartKey:         nil,
						TableName:                 &stmt.TableName,
						ExpressionAttributeNames:  expr.Names(),
						ExpressionAttributeValues: expr.Values(),
						KeyConditionExpression:    expr.KeyCondition(),
						Limit: &stmt.Limit,
					}
					if stmt.AttributesToGet[0] != "*" {
						queryInput.ProjectionExpression = expr.Projection()
					}
					if queryMethod == queryWithGlobalSecondaryIndex {
						queryInput.IndexName = &indexToUse
					}
					if *(expr.Filter()) != "" {
						queryInput.SetFilterExpression(*(expr.Filter()))
					}
					if result, err := db.DynamoDB.Query(queryInput); err == nil {
						return utils.FormatPrettyListOfMap(result.Items), nil
					} else {
						return "", err
					}
				} else {
					return "", err
				}
				// if it's not able to use query, try use scan with filter
			} else {
				builder := expression.NewBuilder().
					WithFilter(filterExpression)
				if stmt.AttributesToGet[0] != "*" {
					builder = builder.WithProjection(projectionExpression)
				}
				if expr, err := builder.Build(); err == nil {
					scanInput := &dynamodb.ScanInput{
						ExclusiveStartKey:         nil,
						TableName:                 &stmt.TableName,
						ExpressionAttributeNames:  expr.Names(),
						ExpressionAttributeValues: expr.Values(),
						FilterExpression:          expr.Filter(),
						Limit:                     aws.Int64(100),
					}
					if stmt.AttributesToGet[0] != "*" {
						scanInput.ProjectionExpression = expr.Projection()
					}
					return scanWithFilterUntilLimit(scanInput, stmt.Limit,
						[]map[string]*dynamodb.AttributeValue{})
				} else {
					return "", err
				}
			}
		}
	} else {
		return "", describeTableErr
	}
}
