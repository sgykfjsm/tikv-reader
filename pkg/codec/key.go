package codec

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/types"
	tidbcodec "github.com/pingcap/tidb/pkg/util/codec"
)

const separator = "_"

// parse the prefix of key patterns: t{TableID}, t{TableID}_r{RowID}, t{TableID}_i{IndexID}
// - row data: t{TableID}_r{RowID}
// - index data: t{TableID}_i{IndexID}_indexedColumnsValue_{RowID}
// Ref: https://docs.pingcap.com/tidb/stable/tidb-computing/#mapping-of-table-data-to-key-value
// Rules:
// 1. Must start with 't' followed by TableID (digits)
// 2. Optionally followed by:
//    a. '_' only for table prefix
//    b. '_r', and optionally followed by RowID (digits) for row keys
//    c. '_i', and optionally followed by IndexID (digits) for index keys
// OK: t123, t123_,  t123_r, t123_r456, t123_i, t123_i789
// NG: t123_r_, t123_r456_, t123_i_, t123_i789_

// ParseKey parses a string representation of a TiDB key into its byte slice form as TiKV key.
func ParseKey(input string) ([]byte, error) {
	return parseKey(input, true)
}

func ParsePrefix(input string) ([]byte, error) {
	return parseKey(input, false)
}

func parseKey(input string, strict bool) ([]byte, error) {
	if !strings.HasPrefix(input, "t") {
		return nil, fmt.Errorf("key must start with 't': %s", input)
	}

	if strict && strings.HasSuffix(input, "_") {
		return nil, fmt.Errorf("key must not end with '_': %s", input)
	}

	parts := strings.Split(input, separator)
	tablePart := parts[0]

	// ---- Table part -----
	if tablePart == "t" {
		if len(parts) > 1 { // e.g,. t_r123
			return nil, fmt.Errorf("invalid key format: %s", input)
		}

		if strict { // input is just "t".
			return nil, fmt.Errorf("invalid key for get: 't' is a prefix, not a specific key")
		}
		return []byte{'t'}, nil
	}

	// Extract TableID
	tableIDStr := tablePart[1:] // remove "t"
	tableID, err := strconv.ParseInt(tableIDStr, 10, 64)
	if err != nil { // e.g., tABC
		return nil, fmt.Errorf("invalid table ID(%s): %v", tableIDStr, err)
	}

	// TiDB uses codec.EncodeInt to encode integers in big-endian format with a length prefix.
	buf := make([]byte, 0, 16)
	buf = append(buf, 't')
	buf = tidbcodec.EncodeInt(buf, tableID)

	// only table prefix such as "t123"
	if len(parts) == 1 {
		if strict {
			return nil, fmt.Errorf("invalid key for get: table prefix %s is not a specific key", input)
		}
		return buf, nil
	}

	// ---- Type part -----
	// Extract Type (_r or _i)
	typePart := parts[1]
	if typePart == "" { // input ends with "_" such as "t123_"
		// maybe this block is not reached but just in case
		if strict {
			return nil, fmt.Errorf("invalid key for get: table prefix %s is not a specific key", input)
		}

		if strings.HasSuffix(input, separator) {
			buf = append(buf, []byte(separator)...)
			return buf, nil
		}

		return nil, fmt.Errorf("(unexpected error) invalid key for get: table prefix %s is not a specific key", input)
	}

	typeMarker := typePart[0:1]
	switch typeMarker {
	case "r":
		// row key
		buf = append(buf, []byte(separator)...)
		buf = append(buf, 'r')
	case "i":
		// index key
		buf = append(buf, []byte(separator)...)
		buf = append(buf, 'i')
	default:
		return nil, fmt.Errorf("unknown type marker: %s", typeMarker)
	}

	if len(typePart) == 1 { // no row/index ID such "t123_r" or "t123_i"
		if strict {
			return nil, fmt.Errorf("invalid key for get: missing ID in key %s", input)
		}

		if len(parts) > 2 { // the input ends with incorrect values such as "t123_r_aaa"
			return nil, fmt.Errorf("invalid key format: values without ID: %s", input)
		}

		// return the value like t123_r or t123_i
		return buf, nil
	}

	// 3. Extract ID (RowID or IndexID)
	idStr := typePart[1:]
	// we have RowID or IndexID to encode such as "t1_r123" or "t1_i456"
	idVal, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid ID(%s): %v", idStr, err)
	}
	buf = tidbcodec.EncodeInt(buf, idVal)

	switch typeMarker {
	case "r":
		// any values must not follow after a row key
		if len(parts) > 2 {
			// if the key is for row record and it's "t1_r_" or "t1_r"
			return nil, fmt.Errorf("row key cannot have suffix values: %s", input)
		}

		return buf, nil
	case "i":
		if len(parts) == 3 && strings.HasSuffix(input, "_") { // expected command is scan
			// if the key is for index record and it's "t1_i123_"
			buf = append(buf, '_')
			return buf, nil
		}
	}

	// we have indexed values in the key such as "t128_i2_594692_3400463811"
	if len(parts) > 2 {
		var datums []types.Datum

		for _, part := range parts[2:] {
			// input might end with the "_" or separator might be repeated in the input
			// e.g., "t123_i456_789_" or "t123_i456_789__"
			if part == "" { // expected command is scan since we return early if the input ends with "_" and the command is get.
				continue
			}

			if n, err := strconv.ParseInt(part, 10, 64); err == nil {
				datums = append(datums, types.NewIntDatum(n))
			} else {
				datums = append(datums, types.NewStringDatum(part))
			}
		}

		if len(datums) > 0 {
			var err error
			typeCtx := types.DefaultStmtNoWarningContext.WithLocation(time.Local)
			buf, err = tidbcodec.EncodeKey(typeCtx.Location(), buf, datums...)
			if err != nil {
				return nil, fmt.Errorf("failed to encode key: %v", err)
			}
		}
	}

	return buf, nil
}

func DecodeKey(key []byte) string {
	if len(key) == 0 {
		return ""
	}

	// 1. Table Prefix must start with 't'
	if key[0] != 't' {
		return hex.EncodeToString(key)
	}

	keyWithoutT := key[1:]

	// 2. Decode TableID
	// TiDB's ID is encoded with MemComparable format and Int, which is the length should be 8 bytes
	if len(keyWithoutT) < 8 {
		return hex.EncodeToString(key)
	}
	_, tableID, err := tidbcodec.DecodeInt(keyWithoutT)
	if err != nil {
		return hex.EncodeToString(key)
	}

	remaining := keyWithoutT[8:]

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("t%d", tableID))

	// 3. Check if there is more data for row/index (_r or _i)
	if len(remaining) < 2 {
		return sb.String()
	}

	// Check if it is '_r'
	if bytes.HasPrefix(remaining, []byte("_r")) {
		// Expected format: tablePrefix{TableID}_recordPrefixSep{RowID}
		sb.WriteString("_r")

		// Extract RowID
		remaining = remaining[2:]
		if len(remaining) >= 8 {
			_, rowID, err := tidbcodec.DecodeInt(remaining)
			if err == nil {
				sb.WriteString(fmt.Sprintf("%d", rowID))
				return sb.String()
			}
		}

		// If we reach here, it means there is no valid RowID
		sb.WriteString(hex.EncodeToString(remaining))
		return sb.String()
	}

	// Check if it is '_i'
	if bytes.HasPrefix(remaining, []byte("_i")) {
		// Expected format: tablePrefix{TableID}_indexPrefixSep{IndexID}_indexedColumnsValue(_{RowID})
		sb.WriteString("_i")

		// Extract IndexID
		remaining = remaining[2:]
		if len(remaining) >= 8 {
			_, indexID, err := tidbcodec.DecodeInt(remaining)
			if err == nil {
				sb.WriteString(fmt.Sprintf("%d", indexID))
				remaining = remaining[8:]
			} else { // unable to decode index ID
				sb.WriteString("_???")
			}
		}

		// Extract IndexedColumnsValue and optional RowID
		if len(remaining) != 0 {
			datum, err := tidbcodec.Decode(remaining, 10)
			if err == nil && len(datum) > 0 {
				for _, d := range datum {
					sb.WriteString("_")
					s, _ := d.ToString()
					sb.WriteString(s)
				}
			} else { // failed to decode
				sb.WriteString("_")
				sb.WriteString(hex.EncodeToString(remaining))
			}
		}

		return sb.String()
	}

	// if we reach here, the key is not valid format, that is unexpected
	sb.WriteString("_")
	sb.WriteString(hex.EncodeToString(remaining))
	return fmt.Sprintf("t%d", tableID)
}

// PrettyPrintKey returns a hex representation of the given key for debugging.
func PrettyPrintKey(key []byte) string {
	return fmt.Sprintf("%X", key)
}
