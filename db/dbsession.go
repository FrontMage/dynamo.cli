package db

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// DynamoDB is a connected session of dynamoDB
var DynamoDB *dynamodb.DynamoDB

// GetDynamoSession returns a new dynamodb session
func GetDynamoSession(accessKeyID, secretAccessKey, region string) *dynamodb.DynamoDB {
	token := ""
	creds := credentials.NewStaticCredentials(accessKeyID, secretAccessKey, token)
	_, err := creds.Get()
	if err != nil {
		panic(err)
	}

	DynamoDB = dynamodb.New(session.New(&aws.Config{Region: &region}))
	return DynamoDB
}

// ListTables returns all table names from dynamoDB
func ListTable(receiver []*string, lastEvaluatedTableName *string) ([]*string, error) {
	if result, err := DynamoDB.ListTables(&dynamodb.ListTablesInput{
		ExclusiveStartTableName: lastEvaluatedTableName,
		Limit: aws.Int64(100),
	}); err == nil {
		for _, name := range result.TableNames {
			receiver = append(receiver, name)
		}
		if result.LastEvaluatedTableName != nil {
			return ListTable(receiver, result.LastEvaluatedTableName)
		}
		return receiver, nil
	} else {
		return receiver, err
	}
}
