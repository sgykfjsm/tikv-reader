package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"slices"
	"strings"

	pingcaplog "github.com/pingcap/log"
	"github.com/sgykfjsm/tikv-reader/pkg/client"
	"github.com/sgykfjsm/tikv-reader/pkg/codec"
	"github.com/urfave/cli/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	cmd := &cli.Command{
		Name:  "tikv-reader",
		Usage: "A simple TiKV reader tool to read keys directly from TiKV nodes in a TiDB cluster.",
		Flags: []cli.Flag{
			&cli.StringSliceFlag{
				Name:     "pd",
				Usage:    "PD server address (e.g., 127.0.0.1:2379)",
				Value:    []string{"127.0.0.1:2379"},
				Required: false,
				Sources:  cli.EnvVars("TIKV_READER_PD_ADDR"),
			},
			&cli.StringFlag{
				Name:    "log-level",
				Aliases: []string{"l"},
				Usage:   "Set the logging level. Available levels: debug, info, warn, error",
				Value:   "info",
				Sources: cli.EnvVars("TIKV_READER_LOG_LEVEL"),
			},
			&cli.BoolFlag{
				Name:    "quiet",
				Aliases: []string{"q"},
				Usage:   "Suppress all log output",
			},
		},
		Before: func(ctx context.Context, cmd *cli.Command) (context.Context, error) {
			if cmd.Bool("quiet") {
				// stop all log output
				slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
				pingcaplog.ReplaceGlobals(zap.NewNop(), nil)

				return ctx, nil
			}

			var level slog.Level
			var zapLevel zapcore.Level // set the log level for zap used by PingCAP libraries

			value := cmd.String("log-level")
			switch strings.ToLower(value) {
			case "debug":
				level = slog.LevelDebug
				zapLevel = zapcore.DebugLevel
			case "info":
				level = slog.LevelInfo
				// set WarnLevel as default for zap to reduce noise?
				zapLevel = zapcore.WarnLevel
			case "warn":
				level = slog.LevelWarn
				zapLevel = zapcore.WarnLevel
			case "error":
				level = slog.LevelError
				zapLevel = zapcore.ErrorLevel
			default:
				fmt.Fprintf(os.Stderr, "Unknown log level: %s, defaulting to INFO", value)
				level = slog.LevelInfo
				zapLevel = zapcore.WarnLevel
			}

			opts := &slog.HandlerOptions{
				Level: level,
			}
			slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, opts)))
			slog.Debug("Logger initialized", slog.String("level", level.String()))

			conf := &pingcaplog.Config{
				Level:  zapLevel.String(),
				Format: "text",
			}
			pg, _, err := pingcaplog.InitLogger(conf)
			if err != nil {
				return nil, fmt.Errorf("failed to initialize pingcap logger: %w", err)
			}
			pingcaplog.ReplaceGlobals(pg, nil)

			slog.Debug("PingCAP logger initialized", slog.String("level", zapLevel.String()))
			return ctx, nil
		},
		Commands: []*cli.Command{
			{
				Name:   "get",
				Usage:  "Get the value for a specific key",
				Action: runGet,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "key",
						Usage:    "Key to retrieve (e.g., t1_r123)",
						Required: true,
					},
				},
			},
			{
				Name:   "scan",
				Usage:  "Scan keys with a specific prefix",
				Action: runScan,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "prefix",
						Usage:    "Key prefix to scan (e.g., t1)",
						Required: true,
					},
					&cli.IntFlag{
						Name:     "limit",
						Usage:    "Number of keys to scan",
						Value:    10,
						Required: false,
					},
				},
			},
		},
	}

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		log.Fatal(err)
	}
}

type TiKVReaderFlags struct {
	PDEndpoints  []string
	TargetKey    string
	TargetPrefix string
	Limit        int
}

// parseFlags parses command-line flags into TiKVReaderFlags.
func parseFlags(cmd *cli.Command) *TiKVReaderFlags {
	return &TiKVReaderFlags{
		PDEndpoints:  cmd.StringSlice("pd"),
		TargetKey:    cmd.String("key"),
		TargetPrefix: cmd.String("prefix"),
		Limit:        cmd.Int("limit"),
	}
}

// Validate validates global flags for the TiKVReaderFlags.
func (f *TiKVReaderFlags) Validate() error {
	if len(f.PDEndpoints) == 0 {
		return fmt.Errorf("PD endpoints are required")
	}

	return nil
}

func runGet(ctx context.Context, cmd *cli.Command) error {
	f := parseFlags(cmd)
	if err := f.Validate(); err != nil {
		return err
	}

	key := f.TargetKey
	if key == "" {
		return fmt.Errorf("key is required")
	}
	if !strings.HasPrefix(key, "t") {
		return fmt.Errorf("currently only table keys (starting with 't') are supported")
	}

	slog.Info("Starting get operation", slog.String("key", key), slog.String("pd_endpoints", fmt.Sprintf("%v", f.PDEndpoints)))

	return getKey(ctx, f.PDEndpoints, key)
}

func runScan(ctx context.Context, cmd *cli.Command) error {
	f := parseFlags(cmd)
	if err := f.Validate(); err != nil {
		return err
	}

	prefix := f.TargetPrefix
	if prefix == "" {
		return fmt.Errorf("prefix is required")
	}
	if !strings.HasPrefix(prefix, "t") {
		return fmt.Errorf("currently only table key prefixes (starting with 't') are supported")
	}

	limit := f.Limit
	if limit <= 0 {
		return fmt.Errorf("limit must be greater than 0")
	}

	if limit > 1000 {
		return fmt.Errorf("limit exceeds maximum of 1000")
	}

	slog.Info("Starting scan operation",
		slog.String("prefix", prefix), slog.String("pd_endpoints", fmt.Sprintf("%v", f.PDEndpoints)), slog.Int("limit", limit))

	return scanKeys(ctx, f.PDEndpoints, prefix, limit)
}

func getKey(ctx context.Context, pdAddr []string, key string) error {
	rawkey, err := codec.ParseKey(key)
	if err != nil {
		return fmt.Errorf("failed to parse key %s: %w", key, err)
	}
	slog.Info("Processing the request", slog.String("key", key), slog.String("parsed_key", fmt.Sprintf("%X", rawkey)))

	cli, err := client.NewTiKVClient(pdAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to PD server(%v): %w", pdAddr, err)
	}
	slog.Info("connected to PD servers", slog.String("pd_addr", fmt.Sprintf("%v", pdAddr)))
	defer cli.Close()

	value, err := cli.Get(ctx, rawkey)
	if err != nil {
		return fmt.Errorf("failed to get key %s: %w", key, err)
	}

	decodedValue := codec.DecodeValue(value)
	PrintSeparatorLine(60)
	fmt.Printf("Key: %s\n", key)
	fmt.Printf("  Hex: %s\n", codec.PrettyPrintKey(rawkey))
	fmt.Printf("Value:\n")
	PrintDecodedValue(decodedValue, "    ")
	PrintSeparatorLine(60)

	return nil
}

func scanKeys(ctx context.Context, pdAddr []string, prefix string, limit int) error {
	rawPrefix, err := codec.ParsePrefix(prefix)
	if err != nil {
		return fmt.Errorf("failed to parse prefix %s: %w", prefix, err)
	}
	slog.Info("Processing the request", slog.String("prefix", prefix), slog.String("parsed_prefix", fmt.Sprintf("%X", rawPrefix)))

	cli, err := client.NewTiKVClient(pdAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to PD server(%v): %w", pdAddr, err)
	}
	defer cli.Close()
	slog.Info("connected to PD servers", slog.String("pd_addr", fmt.Sprintf("%v", pdAddr)))

	// Example scan logic (this would be more complex in a real application)
	keys, values, err := cli.Scan(ctx, rawPrefix, limit)
	if err != nil {
		return fmt.Errorf("failed to scan keys: %w", err)
	}

	fmt.Printf("Scan completed successfully. Retrieved %d key-value pairs:\n", len(keys))
	for i := range keys {
		decodedKey := codec.DecodeKey(keys[i])
		hexKey := codec.PrettyPrintKey(keys[i])
		decodedValue := codec.DecodeValue(values[i])

		PrintSeparatorLine(60)
		fmt.Printf("[%d]\n", i+1)
		fmt.Printf("Key: %s\n", decodedKey)
		fmt.Printf("  Hex: %s\n", hexKey)
		fmt.Printf("Value:\n")
		PrintDecodedValue(decodedValue, "  ")
	}
	PrintSeparatorLine(60)

	return nil
}

func PrintSeparatorLine(n int) {
	fmt.Println(strings.Repeat("-", n))
}

func PrintDecodedValue(v codec.DecodedValue, indent string) {
	switch v.Type {
	case codec.TypeNull:
		fmt.Printf("%s<Null>\n", indent)

	case codec.TypeRaw:
		fmt.Printf("%sRaw(Hex): %s\n", indent, v.Payload.(string))

	case codec.TypeIndex:
		// Indexの場合はリスト表示
		vals := v.Payload.([]string)
		fmt.Printf("%sIndexValues: %s\n", indent, strings.Join(vals, ", "))

	case codec.TypeRowV2:
		row := v.Payload.(codec.RowV2Data)
		fmt.Printf("%sRow Format V2:\n", indent)

		// Mapは順序がないので、ColIDでソートして表示する
		var ids []int64
		for id := range row.Columns {
			ids = append(ids, id)
		}
		// int64のソート (Go 1.21未満なら sort.Slice, 以上なら slices.Sort が使えるがここでは汎用的に)
		slices.Sort(ids)

		for _, id := range ids {
			val := row.Columns[id]
			if id == -1 {
				fmt.Printf("%s  Raw(Hex): %s\n", indent, val)
			} else {
				fmt.Printf("%s  ColID %d: %s\n", indent, id, val)
			}
		}
		fmt.Printf("%s  (Note: Missing columns are NULL/Default)\n", indent)

	default:
		fmt.Printf("%sUnknown Type: %v\n", indent, v.Payload)
	}
}
