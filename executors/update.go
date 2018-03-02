package executors

import (
	"errors"

	"dynamo.cli/db"
	"dynamo.cli/sqlparser"
	"dynamo.cli/utils"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

// Update executes updateSQL string by parsing to dynamodb api
func Update(updateSQL string) (string, error) {
	stmt := sqlparser.ParseUpdate(updateSQL)
	if stmt.TableName == "" {
		return "", errors.New("Can't utils.Find table name, check your inputs")
	}

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

	var updateExpr expression.UpdateBuilder

	for idx, u := range stmt.UpdateExpressions {
		if idx == 0 {
			updateExpr = expression.Set(expression.Name(u.Key), expression.Value(tryParseInt(u.Value)))
		} else {
			updateExpr = updateExpr.Set(expression.Name(u.Key), expression.Value(tryParseInt(u.Value)))
		}
	}

	if expr, err := expression.NewBuilder().WithUpdate(updateExpr).Build(); err == nil {
		updateInput := &dynamodb.UpdateItemInput{
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			UpdateExpression:          expr.Update(),
			Key:                       key,
			TableName:                 &stmt.TableName,
		}

		if len(stmt.AttributesToGet) == 0 {
			updateInput.SetReturnValues("ALL_NEW")
		} else {
			// TODO support return value filter, by default dynamodb does not support that
			updateInput.SetReturnValues("ALL_NEW")
		}

		if result, err := db.DynamoDB.UpdateItem(updateInput); err == nil {
			return utils.FormatPrettyMap(result.Attributes), nil
		} else {
			return "", err
		}
	} else {
		return "", err
	}
}
