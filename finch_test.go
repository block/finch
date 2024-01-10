package finch_test

import (
	"testing"

	"github.com/square/finch"
)

func TestUint(t *testing.T) {
	tests := []struct {
		input  string
		output uint
	}{
		{input: "", output: 0},
		{input: "0", output: 0},
		{input: "1", output: 1},
		{input: "10", output: 10},
		{input: "-10", output: 0}, // error ignore, 0 returned
		{input: "x", output: 0},   // error ignore, 0 returned
	}
	for _, test := range tests {
		t.Run("Uint("+test.input+")", func(t *testing.T) {
			got := finch.Uint(test.input)
			if got != test.output {
				t.Errorf("%s -> %d, expected %d", test.input, got, test.output)
			}
		})
	}
}

func TestWithPort(t *testing.T) {
	port := "1234"
	tests := []struct {
		input  string
		output string
	}{
		{input: "", output: ":1234"},
		{input: "0", output: "0:1234"},
		{input: "local", output: "local:" + port},
		{input: "local:1234", output: "local:" + port}, // same port, no change
		{input: "local:5678", output: "local:5678"},    // differnet port, no change
	}
	for _, test := range tests {
		t.Run("WithPort("+test.input+")", func(t *testing.T) {
			got := finch.WithPort(test.input, port)
			if got != test.output {
				t.Errorf("%s -> %s, expected %s", test.input, got, test.output)
			}
		})
	}
}

func TestRunLevelGreaterThan(t *testing.T) {
	rl := finch.RunLevel{
		Stage:       1,
		ExecGroup:   1,
		ClientGroup: 1,
		Client:      1,
		Trx:         1,
		Query:       1,
	}

	prev := rl

	if rl.GreaterThan(prev, finch.SCOPE_STATEMENT) != false {
		t.Errorf("no change but got true, expecte false")
	}
	if rl.GreaterThan(prev, finch.SCOPE_CLIENT) != false {
		t.Errorf("no change but got true, expecte false")
	}

	rl.Client += 1 // increase run level
	if rl.GreaterThan(prev, finch.SCOPE_STATEMENT) != true {
		t.Errorf("Client changed but got false for STATEMENT")
	}
	if rl.GreaterThan(prev, finch.SCOPE_TRX) != true {
		t.Errorf("Client changed but got false for TRX")
	}
	if rl.GreaterThan(prev, finch.SCOPE_ITER) != true {
		t.Errorf("Client changed but got false for ITER")
	}
}
