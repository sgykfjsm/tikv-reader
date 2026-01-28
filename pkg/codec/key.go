package codec

import (
	"encoding/binary"
	"fmt"
	"regexp"
	"strconv"
)

var (
	// parse the prefix of key patterns: t{TableID}, t{TableID}_r{RowID}, t{TableID}_i{IndexID}
	// - row data: t{TableID}_r{RowID}
	// - index data: t{TableID}_i{IndexID}_indexedColumnsValue_{RowID}
	// Ref: https://docs.pingcap.com/tidb/stable/tidb-computing/#mapping-of-table-data-to-key-value
	keyPattern = regexp.MustCompile(`^t(\d+)(?:_([ri])(\d+))?$`)
)

// Copied from https://github.com/pingcap/tidb/blob/v8.5.5/pkg/util/codec/number.go#L24
const signMask uint64 = 0x8000000000000000

// Copied from https://github.com/pingcap/tidb/blob/v8.5.5/pkg/util/codec/number.go#L26-L29
// EncodeIntToCmpUint make int v to comparable uint type
func EncodeIntToCmpUint(v int64) uint64 {
	return uint64(v) ^ signMask
}

// Copied from https://github.com/pingcap/tidb/blob/v8.5.5/pkg/util/codec/number.go#L36-L43
// EncodeInt appends the encoded value to slice b and returns the appended slice.
// EncodeInt guarantees that the encoded value is in ascending order for comparison.
func EncodeInt(b []byte, v int64) []byte {
	var data [8]byte
	u := EncodeIntToCmpUint(v)
	binary.BigEndian.PutUint64(data[:], u)
	return append(b, data[:]...)
}

// ParseKey parses a string representation of a TiDB key into its byte slice form as TiKV key.
func ParseKey(input string) ([]byte, error) {
	if input == "t" {
		return []byte{'t'}, nil
	}

	matches := keyPattern.FindStringSubmatch(input)
	if matches == nil {
		return nil, fmt.Errorf("invalid key format: %s (expected t{TID}[_r{RID}|_i{IID}])", input)
	}

	tableIDStr := matches[1]
	tableID, err := strconv.ParseInt(tableIDStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid table ID(%s): %v", tableIDStr, err)
	}

	// 1. Prefix: 't' + TableID
	// TiDB uses codec.EncodeInt to encode integers in big-endian format with a length prefix.
	buf := make([]byte, 0, 16)
	buf = append(buf, 't')
	buf = EncodeInt(buf, tableID)

	// only table prefix
	if matches[2] == "" {
		return buf, nil
	}

	// 2. Type: 'r' (row) or 'i' (index)
	typeMarker := matches[2]
	idStr := matches[3]
	idVal, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid ID(%s): %v", idStr, err)
	}

	if typeMarker == "r" {
		// row key: '_r' + RowID
		buf = append(buf, '_', 'r')
		buf = EncodeInt(buf, idVal)
	} else if typeMarker == "i" {
		// index key: '_i' + IndexID
		buf = append(buf, '_', 'i')
		buf = EncodeInt(buf, idVal)
	} else {
		return nil, fmt.Errorf("unknown type marker: %s", typeMarker)
	}

	return buf, nil
}

// PrettyPrintKey returns a hex representation of the given key for debugging.
func PrettyPrintKey(key []byte) string {
	return fmt.Sprintf("%X", key)
}
