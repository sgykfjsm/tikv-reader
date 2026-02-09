# TiKV Reader (tikv-reader)

Note: This document is written by Gemini3, and edited by a human.

**TiKV Reader** is a simple CLI tool that reads data directly from a TiKV cluster (the storage engine of TiDB), decodes the raw bytes, and displays them in a human-readable format.

It accesses TiKV directly via PD (Placement Driver) without going through the SQL layer (TiDB). This makes it an ideal tool for investigating data corruption, understanding the internal behavior of TiDB/TiKV, and debugging low-level data issues.

## Target Data Scope

This tool is specifically designed to decode **Table Data Records** and **Index Records** managed by TiDB.

* **Table Records:** Keys starting with `t{TableID}_r{RowID}`.
* **Index Records:** Keys starting with `t{TableID}_i{IndexID}`.

It expects keys and values to follow the TiDB encoding format (MemComparable keys, Row Format V2 values, etc.). It is not intended for decoding raw TiKV data that is not managed by TiDB or TiDB metadata keys (like `m_...`).

## Features

* **Direct Access:** Connects via TiKV Client to fetch Raw Key-Value pairs directly.
* **Schema-less Decoding:** Parses binary structures without needing table definitions (`CREATE TABLE` statements).
* **Row Format V2:** Automatically detects and parses table row data, displaying Column IDs and Values.
* **Index Values:** Automatically decodes Handles and Restored Data (for New Collations) embedded in indexes.


* **Smart Key Parsing:** Supports logical key formats (e.g., `t132_r1`) as well as raw Hex strings (internal conversion).
* **Flexible Scanning:** Supports scanning entire tables, specific index regions, or ranges via prefixes.

## Installation

Go 1.21 or higher is required.

```bash
# Clone the repository
git clone 'git@github.com:sgykfjsm/tikv-reader.git'
cd tikv-reader

# Tidy dependencies
git submodule update --init --recursive
go mod tidy
go work vendor
# or `just mod_update`

# Build
go build -o tikv-reader main.go
# or `just build`
```

## Usage

### Global Options

```console
$ ./bin/tikv-reader help
NAME:
   tikv-reader - A simple TiKV reader tool to read keys directly from TiKV nodes in a TiDB cluster.

USAGE:
   tikv-reader [global options] [command [command options]]

COMMANDS:
   get      Get the value for a specific key
   scan     Scan keys with a specific prefix
   help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --pd string [ --pd string ]    PD server address (e.g., 127.0.0.1:2379) (default: "127.0.0.1:2379") [$TIKV_READER_PD_ADDR]
   --log-level string, -l string  Set the logging level. Available levels: debug, info, warn, error (default: "info") [$TIKV_READER_LOG_LEVEL]
   --quiet, -q                    Suppress all log output
   --help, -h                     show help
```

### 1. GET Command (Fetch Single Key)

Retrieves a specific key (Row or Index entry).

```bash
# Get a specific record by RowID 1 (TableID: 132)
./tikv-reader get --key t132_r1

# Get a specific index entry (IndexID: 1)
./tikv-reader get --key t132_i1_...

```

**Key Format:**
The `get` command requires a complete key that points to actual data (e.g., `t132` or `t132_r` are invalid for `get` as they are prefixes).

### 2. SCAN Command (Range Scan)

Scans keys based on a specified prefix.

```bash
# Scan the entire table (ID: 132)
./tikv-reader scan --prefix t132 --limit 10

# Scan only the Row Record region (_r)
./tikv-reader scan --prefix t132_r

# Scan only the Index region (_i)
./tikv-reader scan --prefix t132_i

```

**Prefix Behavior:**

* `t132`: Scans keys matching the TableID 132 prefix.
* `t132_`: Explicitly scans keys ending with the separator `_` (0x5f), distinguishing it from table IDs that might share a prefix (though rare in TiDB encoding).

## Output Examples

The tool analyzes both Key and Value byte arrays and outputs them in a structured format.

### Table Row Data (Row Format V2)

If the record in TiDB looks like...

```sql
tidb:4000 > SELECT * FROM authors WHERE id = 1772018;
+---------+-----------------+--------+------------+------------+
| id      | name            | gender | birth_year | death_year |
+---------+-----------------+--------+------------+------------+
| 1772018 | Jamar Bergstrom |      1 |       1979 |       1990 |
+---------+-----------------+--------+------------+------------+
1 row in set (0.01 sec)
```

You should get like this output:

```text
------------------------------------------------------------
Key: t132_r1772018
Value:
Row Format V2:
  ColID 2: "Jamar Bergstrom"
  ColID 3: Int: 1 (Hex: 0x01)
  ColID 4: Int: 1979 (Hex: 0xbb07)
  ColID 5: Int: 1990 (Hex: 0xc607)
  (Note: Missing columns are NULL/Default)
------------------------------------------------------------
```

### Index Data

The tool decodes "Restored Data" (used for covering indexes and collations) stored within the value, even if it is complex (Int or String).

**Integer Index Value:**

```text
------------------------------------------------------------
Key: t128_i2_594692_3400463811
  Hex: 7480000000000000805F6980000000000000020380000000000913040380000000CAAEF5C3
Value:
    IndexValues: 594692, 3400463811
------------------------------------------------------------
```

You can find more details about this output with `TIDB_DECODE_KEY` function like this:

```sql
tidb:4000 > SELECT TIDB_DECODE_KEY('7480000000000000805F6980000000000000020380000000000913040380000000CAAEF5C3');
+-----------------------------------------------------------------------------------------------+
| TIDB_DECODE_KEY('7480000000000000805F6980000000000000020380000000000913040380000000CAAEF5C3') |
+-----------------------------------------------------------------------------------------------+
| {"index_id":2,"index_vals":{"book_id":"594692","user_id":"3400463811"},"table_id":128}        |
+-----------------------------------------------------------------------------------------------+
1 row in set (0.01 sec)
```

**String Index Value (Row V2 Nested):**

```text
------------------------------------------------------------
[1]
Key: t132_i2_Aaliyah Crist_0_3829293726
  Hex: 7480000000000000845F6980000000000000020141616C6979616820FF4372697374000000FC0380000000000000000380000000E43E629E
Value:
  Row Format V2:
    ColID 2: "Aaliyah Crist"
    ColID 3: Int: 0 (Hex: 0x00)
    (Note: Missing columns are NULL/Default)
```

Of course, this index key can also query with `TIDB_DECODE_KEY` function like this:

```sql
tidb:4000 > SELECT TIDB_DECODE_KEY('7480000000000000845F6980000000000000020141616C6979616820FF4372697374000000FC0380000000000000000380000000E43E629E');
+-------------------------------------------------------------------------------------------------------------------------------------+
| TIDB_DECODE_KEY('7480000000000000845F6980000000000000020141616C6979616820FF4372697374000000FC0380000000000000000380000000E43E629E') |
+-------------------------------------------------------------------------------------------------------------------------------------+
| {"index_id":2,"index_vals":{"gender":"0","name":"Aaliyah Crist"},"table_id":132}                                                    |
+-------------------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.00 sec)
```

## Internals

This tool leverages TiDB's official libraries (like `tidb/pkg/util/codec`) but implements a custom parser to handle data without schema info (`TableInfo`).

* **Key Parsing:** Converts user input strings (`t132_r1`) into TiKV physical keys (MemComparable Format).
* **Value Decoding Strategy:**
1. **Row Format V2:** If the value starts with `0x80`.
2. **Nested Row Format V2:** If the value starts with `0x00` followed by `0x80` (Commonly found in indexes containing strings/collations).
3. **MemComparable Format:** Otherwise, it scans the byte slice to extract valid encoded data (Restored Data) embedded within the index value.



## Future Implementation

* Output in JSON format (e.g., `--output json`).
* Resolve Table ID from Table Name (requires interaction with TiDB schema).
* Resolve Index ID from Index Name.

## Disclaimer

* **Development Use Only:** This tool is intended for development, learning, and debugging purposes. Running large `scan` operations on a production TiKV cluster may impact performance.
* **Compatibility:** TiDB internal formats may change between versions. This tool is primarily designed for TiDB v5.0+ (specifically v8.x) using Row Format V2.
* **Limitation:** This tool doesn't access a schema information in a TiDB layer, which means some data type isn't decoded well.

<!-- EOF -->