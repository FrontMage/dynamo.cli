package utils

import "testing"

func TestFindIndex(t *testing.T) {
	type args struct {
		from  []string
		match string
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "find index in slice",
			args: args{
				from:  []string{"SELECT", "FROM", "LIMIT", "user"},
				match: "FROM",
			},
			want: 1,
		},
		{
			name: "find index in slice",
			args: args{
				from:  []string{"SELECT", "FROM", "LIMIT", "user"},
				match: "haha",
			},
			want: -1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FindIndex(tt.args.from, tt.args.match); got != tt.want {
				t.Errorf("FindIndex() = %v, want %v", got, tt.want)
			}
		})
	}
}
