package executors

import (
	"reflect"
	"testing"

	"github.com/FrontMage/dynamo.cli/sqlparser"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

func Test_tryParseInt(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name string
		args args
		want interface{}
	}{
		{
			name: "test tryParseInt with parseable string",
			args: args{s: "123"},
			want: 123,
		},
		{
			name: "test tryParseInt with unparseable string",
			args: args{s: "hi"},
			want: "hi",
		},
		{
			name: "test tryParseInt with quoted string",
			args: args{s: `"123"`},
			want: "123",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tryParseInt(tt.args.s); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("tryParseInt() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSwitchExpression(t *testing.T) {
	type args struct {
		condition sqlparser.Condition
	}
	tests := []struct {
		name string
		args args
		want expression.ConditionBuilder
	}{
		{
			name: "test SwitchExpression with =",
			args: args{
				condition: sqlparser.Condition{
					Key:                 "user_id",
					Value:               "9527",
					Operator:            "=",
					NextLogicalOperator: "AND",
				},
			},
			want: expression.Name("user_id").Equal(expression.Value(9527)),
		},
		{
			name: "test SwitchExpression with >",
			args: args{
				condition: sqlparser.Condition{
					Key:                 "user_id",
					Value:               "9527",
					Operator:            ">",
					NextLogicalOperator: "AND",
				},
			},
			want: expression.Name("user_id").GreaterThan(expression.Value(9527)),
		},
		{
			name: "test SwitchExpression with <",
			args: args{
				condition: sqlparser.Condition{
					Key:                 "user_id",
					Value:               "9527",
					Operator:            "<",
					NextLogicalOperator: "AND",
				},
			},
			want: expression.Name("user_id").LessThan(expression.Value(9527)),
		},
		{
			name: "test SwitchExpression with >=",
			args: args{
				condition: sqlparser.Condition{
					Key:                 "user_id",
					Value:               "9527",
					Operator:            ">=",
					NextLogicalOperator: "AND",
				},
			},
			want: expression.Name("user_id").GreaterThanEqual(expression.Value(9527)),
		},
		{
			name: "test SwitchExpression with <=",
			args: args{
				condition: sqlparser.Condition{
					Key:                 "user_id",
					Value:               "9527",
					Operator:            "<=",
					NextLogicalOperator: "AND",
				},
			},
			want: expression.Name("user_id").LessThanEqual(expression.Value(9527)),
		},
		{
			name: "test SwitchExpression with !=",
			args: args{
				condition: sqlparser.Condition{
					Key:                 "user_id",
					Value:               "9527",
					Operator:            "!=",
					NextLogicalOperator: "AND",
				},
			},
			want: expression.Name("user_id").NotEqual(expression.Value(9527)),
		},
		{
			name: "test SwitchExpression with LIKE",
			args: args{
				condition: sqlparser.Condition{
					Key:                 "user_id",
					Value:               "9527",
					Operator:            " LIKE ",
					NextLogicalOperator: "AND",
				},
			},
			want: expression.Name("user_id").Contains("9527"),
		},
		{
			name: "test SwitchExpression with default",
			args: args{
				condition: sqlparser.Condition{
					Key:                 "user_id",
					Value:               "9527",
					Operator:            "<>",
					NextLogicalOperator: "AND",
				},
			},
			want: expression.Name("user_id").Equal(expression.Value("9527")),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SwitchExpression(tt.args.condition); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SwitchExpression() = %v, want %v", got, tt.want)
			}
		})
	}
}
