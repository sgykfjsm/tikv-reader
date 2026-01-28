package codec

import (
	"bytes"
	"testing"
)

func TestParseKey(t *testing.T) {
	tests := []struct {
		input    string
		hasError bool
	}{
		{"t", false},
		{"t1", false},
		{"t1_r1", false},
		{"t1_i1", false},
		{"t1_r", true},     // missing row ID
		{"t1_i", true},     // missing index ID
		{"t1_r1_i2", true}, // extra parts
		{"xABC", true},     // invalid prefix
		{"x1_r1", true},    // unknown type marker
	}

	for _, tt := range tests {
		key, err := ParseKey(tt.input)
		isActualError := err != nil
		if tt.hasError {
			if !isActualError {
				t.Errorf("ParseKey(%s) error = nil, wantErr %v", tt.input, tt.hasError)
			}
			// if isActualError is also true, we don't need to check further
		} else {
			if isActualError {
				t.Errorf("ParseKey(%s) error = %v, wantErr %v", tt.input, err, tt.hasError)
			}

			if len(key) == 0 {
				t.Errorf("ParseKey(%s) = empty, want non-empty", tt.input)
			}

			if key[0] != 't' {
				t.Errorf("ParseKey(%s) = %X, first byte is not 't'", tt.input, key)
			}
		}
	}
}

func TestKeyEncodingConsistency(t *testing.T) {
	inputKey1 := "t1"
	inputKey2 := inputKey1 + "_r123"
	k1, _ := ParseKey(inputKey1)
	k2, _ := ParseKey(inputKey2)

	if !bytes.HasPrefix(k2, k1) {
		t.Errorf("%s should have prefix %s. But actaul got %X and actaul prefix: %X", inputKey2, inputKey1, k2, k1)
	}
}
