package utils

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/tidwall/pretty"
)

// FormatPrettyMap format and colorize a map[string]*dynamodb.AttributeValue to JSON string
func FormatPrettyMap(input map[string]*dynamodb.AttributeValue) string {
	jsonResult := map[string]interface{}{}
	if err := dynamodbattribute.UnmarshalMap(input, &jsonResult); err == nil {
		if formatedResult, err := json.MarshalIndent(&jsonResult, "", "  "); err == nil {
			return string(pretty.Color(formatedResult, nil))
		} else {
			fmt.Println(err.Error())
			return ""
		}
	} else {
		fmt.Println(err.Error())
		return ""
	}
}

// FormatPrettyListOfMap format and colorize a []map[string]*dynamodb.AttributeValue to JSON string
func FormatPrettyListOfMap(input []map[string]*dynamodb.AttributeValue) string {
	jsonResult := []map[string]interface{}{}
	if err := dynamodbattribute.UnmarshalListOfMaps(input, &jsonResult); err == nil {
		if formatedResult, err := json.MarshalIndent(&jsonResult, "", "  "); err == nil {
			if len(jsonResult) == 1 {
				return fmt.Sprintf("%s\n%d item", string(pretty.Color(formatedResult, nil)), len(jsonResult))
			} else {
				return fmt.Sprintf("%s\n%d items", string(pretty.Color(formatedResult, nil)), len(jsonResult))
			}
		} else {
			fmt.Println(err.Error())
			return ""
		}
	} else {
		fmt.Println(err.Error())
		return ""
	}
}
