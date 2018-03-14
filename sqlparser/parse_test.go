package sqlparser

import (
	"reflect"
	"testing"
)

func Test_killAllKeyWords(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "test killAllKeyWords",
			args: args{s: "SELECT * FROM user LIMIT 10 END"},
			want: "*user10",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := killAllKeyWords(tt.args.s); got != tt.want {
				t.Errorf("killAllKeyWords() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_parseAttributesToGet(t *testing.T) {
	type args struct {
		attributesToGetStr string
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "test parseAttributesToGet with ,",
			args: args{attributesToGetStr: "user_id,age,name"},
			want: []string{"user_id", "age", "name"},
		},
		{
			name: "test parseAttributesToGet without ,",
			args: args{attributesToGetStr: "user_id"},
			want: []string{"user_id"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseAttributesToGet(tt.args.attributesToGetStr); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseAttributesToGet() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_switchCondition(t *testing.T) {
	type args struct {
		condition           string
		nextLogicalOperator string
	}
	tests := []struct {
		name string
		args args
		want Condition
	}{
		{
			name: "test switchCondition",
			args: args{
				condition:           "user_id=123",
				nextLogicalOperator: "AND",
			},
			want: Condition{
				Key:                 "user_id",
				Operator:            "=",
				Value:               "123",
				NextLogicalOperator: "AND",
			},
		},
		{
			name: "test switchCondition",
			args: args{
				condition:           "user_id+123",
				nextLogicalOperator: "AND",
			},
			want: Condition{
				Key:                 "",
				Operator:            "=",
				Value:               "",
				NextLogicalOperator: "AND",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := switchCondition(tt.args.condition, tt.args.nextLogicalOperator); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("switchCondition() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseSelect(t *testing.T) {
	type args struct {
		selectSQL string
	}
	tests := []struct {
		name string
		args args
		want SelectStatement
	}{
		{
			name: "test parseSelect basic",
			args: args{selectSQL: "SELECT * FROM user END"},
			want: SelectStatement{
				AttributesToGet: []string{"*"},
				TableName:       "user",
				Conditions:      []Condition{},
				Limit:           1,
			},
		},
		{
			name: "test parseSelect limit number",
			args: args{selectSQL: "SELECT * FROM user LIMIT 10 END"},
			want: SelectStatement{
				AttributesToGet: []string{"*"},
				TableName:       "user",
				Conditions:      []Condition{},
				Limit:           10,
			},
		},
		{
			name: "test parseSelect limit all",
			args: args{selectSQL: "SELECT * FROM user LIMIT ALL END"},
			want: SelectStatement{
				AttributesToGet: []string{"*"},
				TableName:       "user",
				Conditions:      []Condition{},
				Limit:           -1,
			},
		},
		{
			name: "test parseSelect with attributes to get",
			args: args{selectSQL: "SELECT user_id,name,age FROM user LIMIT ALL END"},
			want: SelectStatement{
				AttributesToGet: []string{"user_id", "name", "age"},
				TableName:       "user",
				Conditions:      []Condition{},
				Limit:           -1,
			},
		},
		{
			name: "test parseSelect with condition",
			args: args{selectSQL: "SELECT user_id,name,age FROM user WHERE user_id=9527 LIMIT ALL END"},
			want: SelectStatement{
				AttributesToGet: []string{"user_id", "name", "age"},
				TableName:       "user",
				Conditions: []Condition{
					Condition{
						Key:                 "user_id",
						Value:               "9527",
						Operator:            "=",
						NextLogicalOperator: "AND",
					},
				},
				Limit: -1,
			},
		},
		{
			name: "test parseSelect with multiple condition",
			args: args{selectSQL: `SELECT user_id,name,age FROM user WHERE user_id=9527 AND name=Jason LIMIT ALL END`},
			want: SelectStatement{
				AttributesToGet: []string{"user_id", "name", "age"},
				TableName:       "user",
				Conditions: []Condition{
					Condition{
						Key:                 "user_id",
						Value:               "9527",
						Operator:            "=",
						NextLogicalOperator: "AND",
					},
					Condition{
						Key:                 "name",
						Value:               "Jason",
						Operator:            "=",
						NextLogicalOperator: "AND",
					},
				},
				Limit: -1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ParseSelect(tt.args.selectSQL); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseSelect() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseUpdate(t *testing.T) {
	type args struct {
		updateSQL string
	}
	tests := []struct {
		name string
		args args
		want UpdateStatement
	}{
		{
			name: "test ParseUpdate",
			args: args{updateSQL: "UPDATE user SET user_name=xinbg WHERE user_id=123 RETRUNING user_name,phone END"},
			want: UpdateStatement{
				AttributesToGet: []string{"user_name", "phone"},
				TableName:       "user",
				Conditions: []Condition{Condition{
					Key:                 "user_id",
					Value:               "123",
					Operator:            "=",
					NextLogicalOperator: "AND",
				}},
				UpdateExpressions: []UpdateExpression{UpdateExpression{
					Key:   "user_name",
					Value: "xinbg",
				}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ParseUpdate(tt.args.updateSQL); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseUpdate() = %+v,\n 		------want %+v", got, tt.want)
			}
		})
	}
}

func TestParseDescTable(t *testing.T) {
	type args struct {
		descTableSQL string
	}
	tests := []struct {
		name string
		args args
		want DescTableStatement
	}{
		{
			name: "test ParseDescTable",
			args: args{descTableSQL: "DESC TABLE user END"},
			want: DescTableStatement{TableName: "user"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ParseDescTable(tt.args.descTableSQL); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseDescTable() = %v, want %v", got, tt.want)
			}
		})
	}
}
