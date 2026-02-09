package codec

import (
	"bytes"
	"encoding/hex"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/types"
	tidbcodec "github.com/pingcap/tidb/pkg/util/codec"
)

func TestParseKey(t *testing.T) {
	tests := []struct {
		input    string
		hasError bool
	}{
		// valid cases
		{"t1_r1", false},
		{"t1_i1", false},
		{"t1_i1_234_567", false},
		{"t1_i1_234_567_890", false},

		// invalid cases for get (strict=true)
		{"t", true},
		{"t1", true},
		{"t1_", true},
		{"t1_r", true},
		{"t1_i", true},

		// invalid format cases
		{"t1_r_", true},
		{"t1_r1_", true},
		{"t1_i_", true},
		{"t1_i1_", true},

		// invalid values cases
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
		}
	}
}

func TestParsePrefix(t *testing.T) {
	tests := []struct {
		input    string
		hasError bool
	}{
		// valid cases
		{"t", false},
		{"t1", false},
		{"t1_", false},
		{"t1_r", false},
		{"t1_r1", false},
		{"t1_i", false},
		{"t1_i1", false},
		{"t1_i1_", false},
		{"t1_i1_234_567", false},
		{"t1_i1_234_567_890", false},

		// invalid format cases
		{"t1_r_", true},
		{"t1_r1_", true},
		{"t1_i_", true},

		// invalid values cases
		{"t1_r1_i2", true}, // extra parts
		{"xABC", true},     // invalid prefix
		{"x1_r1", true},    // unknown type marker
	}

	for _, tt := range tests {
		key, err := ParsePrefix(tt.input)
		isActualError := err != nil
		if tt.hasError {
			if !isActualError {
				t.Errorf("ParsePrefix(%s) error = nil, wantErr %v", tt.input, tt.hasError)
			}
			// if isActualError is also true, we don't need to check further
		} else {
			if isActualError {
				t.Errorf("ParsePrefix(%s) error = %v, wantErr %v", tt.input, err, tt.hasError)
			}

			if len(key) == 0 {
				t.Errorf("ParsePrefix(%s) = empty, want non-empty", tt.input)
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
		t.Errorf("%s should have prefix %s. But actual got %X and actual prefix: %X", inputKey2, inputKey1, k2, k1)
	}
}

func TestDecodeKey(t *testing.T) {
	// Ref: https://github.com/pingcap/tidb/blob/master/pkg/util/codec/codec_test.go
	tests := []struct {
		name     string
		setup    func() []byte // テスト用のキーバイト列を生成する関数
		expected string        // 期待されるデコード文字列
	}{
		{
			name: "Standard Record Key (t113_r1)",
			setup: func() []byte {
				// t + Enc(113) + _r + Enc(1)
				b := []byte{'t'}
				b = tidbcodec.EncodeInt(b, 113) // Table ID
				b = append(b, '_', 'r')
				b = tidbcodec.EncodeInt(b, 1) // Row ID
				return b
			},
			expected: "t113_r1",
		},
		{
			name: "Standard Index Key (t126_i1_594692_3769634)",
			setup: func() []byte {
				b := []byte{'t'}
				b = tidbcodec.EncodeInt(b, 126)
				b = append(b, '_', 'i')
				b = tidbcodec.EncodeInt(b, 1) // Index ID

				var err error
				datums := types.MakeDatums(594692, 3769634)
				typeCtx := types.DefaultStmtNoWarningContext.WithLocation(time.Local)
				b, err = tidbcodec.EncodeKey(typeCtx.Location(), b, datums...)
				if err != nil {
					t.Fatalf("failed to encode key: %v", err)
				}
				return b
			},
			expected: "t126_i1_594692_3769634",
		},
		{
			name: "Index Key with String (t1_i2_apple)",
			setup: func() []byte {
				b := []byte{'t'}
				b = tidbcodec.EncodeInt(b, 1)
				b = append(b, []byte("_i")...)
				b = tidbcodec.EncodeInt(b, 2)

				var err error
				datums := types.MakeDatums("apple")
				typeCtx := types.DefaultStmtNoWarningContext.WithLocation(time.Local)
				b, err = tidbcodec.EncodeKey(typeCtx.Location(), b, datums...)
				if err != nil {
					t.Fatalf("failed to encode key: %v", err)
				}
				return b
			},
			expected: "t1_i2_apple",
		},
		{
			name: "Decode Hex Data",
			setup: func() []byte {
				hexStr := "74800000000000007E5F698000000000000001038000000000091304038000000000398522"
				b, _ := hex.DecodeString(hexStr)
				return b
			},
			expected: "t126_i1_594692_3769634",
		},
		{
			name: "Table Prefix Only (t100)",
			setup: func() []byte {
				b := []byte{'t'}
				b = tidbcodec.EncodeInt(b, 100)
				return b
			},
			expected: "t100",
		},
		{
			name: "Raw Bytes (Non-TiDB Key)",
			setup: func() []byte {
				return []byte{0x11, 0x22, 0x33}
			},
			expected: "112233",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := tt.setup()
			got := DecodeKey(input)
			if got != tt.expected {
				t.Errorf("DecodeKey() = %v, want %v", got, tt.expected)
			}
		})
	}
}
