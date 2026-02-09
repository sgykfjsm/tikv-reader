package codec

import (
	"encoding/hex"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/types"
	tidbcodec "github.com/pingcap/tidb/pkg/util/codec"
)

func TestDecodeValue(t *testing.T) {
	// テスト用の Row Format V2 データ (Hex文字列)
	// ColID 2: "Aaliyah Mueller"
	// ColID 3: 1 (Int)
	// 実際のTiDBのデータから抽出したもの
	rowV2Hex := "80000200000002030f00100041616c69796168204d75656c6c657201"
	rowV2Bytes, _ := hex.DecodeString(rowV2Hex)

	// Nested Index用のデータ (RowV2の前に 0x00 が付いたもの)
	nestedRowV2Bytes := append([]byte{0x00}, rowV2Bytes...)

	tests := []struct {
		name     string
		setup    func() []byte
		expected DecodedValue
	}{
		{
			name: "Null / Empty",
			setup: func() []byte {
				return []byte{}
			},
			expected: DecodedValue{
				Type:    TypeNull,
				Payload: nil,
			},
		},
		{
			name: "Row Format V2 (Standard Table Value)",
			setup: func() []byte {
				return rowV2Bytes
			},
			expected: DecodedValue{
				Type: TypeRowV2,
				Payload: RowV2Data{
					Columns: map[int64]string{
						2: fmt.Sprintf("%q", "Aaliyah Mueller"),
						3: "Int: 1 (Hex: 0x01)",
					},
				},
			},
		},
		{
			name: "Index with Nested Row V2 (String/Collation)",
			setup: func() []byte {
				// 先頭に 0x00 が付いているケース
				return nestedRowV2Bytes
			},
			expected: DecodedValue{
				Type: TypeRowV2,
				Payload: RowV2Data{
					Columns: map[int64]string{
						2: fmt.Sprintf("%q", "Aaliyah Mueller"),
						3: "Int: 1 (Hex: 0x01)",
					},
				},
			},
		},
		{
			name: "Index with MemComparable (Ints)",
			setup: func() []byte {
				datums := types.MakeDatums(100, 200)
				typeCtx := types.DefaultStmtNoWarningContext.WithLocation(time.Local)
				b, _ := tidbcodec.EncodeKey(typeCtx.Location(), nil, datums...)
				return b
			},
			expected: DecodedValue{
				Type: TypeIndex,
				Payload: []string{
					"100",
					"200",
				},
			},
		},
		{
			name: "Index with Garbage Header + MemComparable",
			setup: func() []byte {
				prefix := []byte{0xff, 0xff} // invalid prefix
				datum := types.MakeDatums(12345)
				typeCtx := types.DefaultStmtNoWarningContext.WithLocation(time.Local)
				encoded, _ := tidbcodec.EncodeKey(typeCtx.Location(), nil, datum...)
				return append(prefix, encoded...)
			},
			expected: DecodedValue{
				Type: TypeIndex,
				Payload: []string{
					"12345",
				},
			},
		},
		{
			name: "Raw Hex (Fallback)",
			setup: func() []byte {
				// unexpected format data
				return []byte{0xff, 0xff, 0xff}
			},
			expected: DecodedValue{
				Type:    TypeRaw,
				Payload: "ffffff",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := tt.setup()
			got := DecodeValue(input)

			// 1. Type Check
			if got.Type != tt.expected.Type {
				t.Errorf("Type mismatch: got %v, want %v", got.Type, tt.expected.Type)
				return
			}

			// 2. Payload Check (Deep Compare)
			if !reflect.DeepEqual(got.Payload, tt.expected.Payload) {
				t.Errorf("Payload mismatch:\ngot  %#v\nwant %#v", got.Payload, tt.expected.Payload)
			}
		})
	}
}

// scrapeMemComparable の単体テスト (内部ロジックの確認)
func TestScrapeMemComparable(t *testing.T) {
	// normal
	datums := types.MakeDatums(10, 20)
	b, _ := tidbcodec.EncodeKey(nil, nil, datums...)

	vals, found := scrapeMemComparable(b)
	if !found {
		t.Fatal("Should be found")
	}
	if len(vals) != 2 || vals[0] != "10" || vals[1] != "20" {
		t.Errorf("Unexpected values: %v", vals)
	}

	// 異常系: 全てゴミ
	garbage := []byte{0xff, 0xff}
	_, foundGarbage := scrapeMemComparable(garbage)
	if foundGarbage {
		t.Error("Should not find anything in garbage")
	}

	// abnormal, append invalid prefix
	mixed := append([]byte{0xaa, 0xbb}, b...)
	valsMixed, foundMixed := scrapeMemComparable(mixed)
	if !foundMixed {
		t.Error("Should find values in mixed data")
	}
	if !reflect.DeepEqual(valsMixed, []string{"10", "20"}) {
		t.Errorf("Mixed values mismatch: %v", valsMixed)
	}
}
