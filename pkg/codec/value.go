package codec

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log/slog"
	"time"
	"unicode/utf8"

	"github.com/pingcap/tidb/pkg/types"
	tidbcodec "github.com/pingcap/tidb/pkg/util/codec"
)

type ValueType string

const (
	TypeNull  ValueType = "null"
	TypeRowV2 ValueType = "row_v2"
	TypeIndex ValueType = "index"
	TypeRaw   ValueType = "raw"
)

type DecodedValue struct {
	Type    ValueType   `json:"type"`
	Payload interface{} `json:"payload"`
}

type RowV2Data struct {
	Columns map[int64]string `json:"columns"` // ColID -> ValueString
}

const padding = "    " // 4 spaces

func FormatWithPadding(format string, a ...any) string {
	return fmt.Sprintf(padding+format, a...)
}

// DecodeValue decodes the given value into a human-readable string.
func DecodeValue(value []byte) DecodedValue {
	if len(value) == 0 {
		return DecodedValue{Type: TypeNull, Payload: nil}
	}

	// Check if row format v2 (first byte is 0x80)
	if value[0] == 0x80 {
		return DecodedValue{
			Type:    TypeRowV2,
			Payload: decodeRowV2(value),
		}
	}

	// Check if the index with row format v2 (maybe this index includes string values)
	// first byte should be 0x00 (TailLen). See https://github.com/pingcap/tidb/blob/master/pkg/tablecodec/tablecodec.go#L1503-L1552
	if len(value) > 1 && value[0] == 0x00 && value[1] == 0x80 {
		return DecodedValue{
			Type:    TypeRowV2,
			Payload: decodeRowV2(value[1:]),
		}
	}

	// Try decoding as index value
	if v, found := scrapeMemComparable(value); found {
		return DecodedValue{
			Type:    TypeIndex,
			Payload: v,
		}
	}

	// Try minimal decoding for other formats or fall back to hex
	return DecodedValue{
		Type:    TypeRaw,
		Payload: hex.EncodeToString(value),
	}
}

func scrapeMemComparable(data []byte) ([]string, bool) {
	if len(data) == 0 {
		return nil, false
	}

	var foundValues []string
	var found bool
	// try decode the data one by one
	for i := 0; i < len(data); {
		datums, err := tidbcodec.Decode(data[i:], 1)

		if err == nil && len(datums) > 0 {
			// successfully decoded
			d := datums[0]
			str, _ := d.ToString()
			foundValues = append(foundValues, str)
			found = true

			// calculate the length of decoded data
			typeCtx := types.DefaultStmtNoWarningContext.WithLocation(time.Local)
			encoded, _ := tidbcodec.EncodeKey(typeCtx.Location(), nil, d)
			if len(encoded) <= 0 { // something wrong? just +1 for safety
				i++
			} else {
				i += len(encoded)
			}
		} else { // failed to decode
			i++
		}
	}

	if found {
		return foundValues, true
	}

	return nil, false
}

func decodeRowV2(val []byte) RowV2Data {
	result := make(map[int64]string)

	// mapping column ID to raw data
	cols, err := parseRowV2Structure(val)
	if err == nil {
		for i, raw := range cols {
			result[i] = trySmartDecode(raw)
		}
	} else { // not row v2 data format
		result[-1] = hex.EncodeToString(val)
	}

	return RowV2Data{Columns: result}
}

func parseRowV2Structure(data []byte) (map[int64][]byte, error) {
	const expectedLength = 6 // minimal length for RowV2
	if len(data) < expectedLength {
		return nil, fmt.Errorf("data too short. expected length %d but actual %d", expectedLength, len(data))
	}

	numSmall := binary.LittleEndian.Uint16(data[2:4])
	numLarge := binary.LittleEndian.Uint16(data[4:6])

	cursor := 6
	colMap := make(map[int64][]byte)

	smallIDs := make([]int64, numSmall)
	for i := 0; i < int(numSmall); i++ {
		if cursor >= len(data) {
			return nil, fmt.Errorf("unexpected end of data while reading small column IDs")
		}
		smallIDs[i] = int64(data[cursor])
		cursor++
	}

	largeIDs := make([]int64, numLarge)
	const OffsetLarge = 4
	for i := 0; i < int(numLarge); i++ {
		if cursor+OffsetLarge >= len(data) {
			return nil, fmt.Errorf("unexpected end of data while reading large column IDs")
		}
		largeIDs[i] = int64(binary.LittleEndian.Uint32(data[cursor : cursor+OffsetLarge]))
		cursor += OffsetLarge
	}
	allIDs := append(smallIDs, largeIDs...)

	numCols := len(allIDs)
	offsets := make([]int, numCols)
	for i := range numCols {
		if cursor+2 > len(data) {
			return nil, fmt.Errorf("unexpected end of data while reading column offsets")
		}
		offsets[i] = int(binary.LittleEndian.Uint16(data[cursor : cursor+2]))
		cursor += 2
	}

	valueStartBase := cursor
	previousOffset := 0
	for i, id := range allIDs {
		endOffset := offsets[i]

		// Check bounds
		startPos := valueStartBase + previousOffset
		endPos := valueStartBase + endOffset

		if endPos > len(data) {
			return nil, fmt.Errorf("offset out of bounds for column ID %d: %d vs len %d", id, endPos, len(data))
		}

		if startPos > endPos {
			return nil, fmt.Errorf("invalid offset order for column ID %d", id)
		}

		val := data[startPos:endPos]
		valCopy := make([]byte, len(val))
		copy(valCopy, val)
		colMap[id] = valCopy

		previousOffset = endOffset
	}

	return colMap, nil
}

func trySmartDecode(b []byte) string {
	if len(b) == 0 {
		return "NULL/Empty"
	}

	// 1. Check if it's JSON (Object or Array)
	if b[0] == 0x01 || b[0] == 0x03 { // Object or Array
		if jsonStr, ok := safeDecodeJson(b); ok {
			return jsonStr
		}
	}

	// 2. Check if it's integer (small int/int/bigint)
	var intValStr string
	isInteger := false
	switch len(b) {
	case 1:
		intValStr = fmt.Sprintf("%d", int8(b[0]))
		isInteger = true
	case 2:
		intValStr = fmt.Sprintf("%d", int16(binary.LittleEndian.Uint16(b)))
		isInteger = true
	case 4:
		intValStr = fmt.Sprintf("%d", int32(binary.LittleEndian.Uint32(b)))
		isInteger = true
	case 8:
		intValStr = fmt.Sprintf("%d", int64(binary.LittleEndian.Uint64(b)))
		isInteger = true
	}

	// 3. Check if it's string
	if isLooksLikeString(b) {
		strVal := fmt.Sprintf("%q", string(b))
		if isInteger {
			return fmt.Sprintf("Int: %s Str: %s", intValStr, strVal)
		}
		return strVal
	}

	// 4. Maybe the data is not string but integer-ish.
	if isInteger {
		return fmt.Sprintf("Int: %s (Hex: 0x%x)", intValStr, b)
	}

	// 5. Fallback to hex representation
	if len(b) <= 8 {
		return fmt.Sprintf("0x%x", b)
	}

	return fmt.Sprintf("0x%x... (len=%d)", b[:8], len(b))
}

func safeDecodeJson(b []byte) (result string, ok bool) {
	defer func() {
		if r := recover(); r != nil {
			slog.Warn("panic during JSON decode", "error", r)
			ok = false
		}
	}()

	bj := types.BinaryJSON{TypeCode: b[0], Value: b[1:]}
	return bj.String(), true
}

func isLooksLikeString(b []byte) bool {
	if !utf8.Valid(b) {
		return false
	}

	controlCount := 0
	for _, v := range b {
		if v < 32 && v != '\n' && v != '\r' && v != '\t' {
			controlCount++
		}
	}

	// if more than 10% are control characters, consider it string
	return float64(controlCount)/float64(len(b)) < 0.1
}
