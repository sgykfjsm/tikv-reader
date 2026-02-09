package client

import (
	"context"
	"fmt"

	"github.com/tikv/client-go/v2/txnkv"
)

type TiKVClient struct {
	client *txnkv.Client
}

func NewTiKVClient(pdAddrs []string) (*TiKVClient, error) {
	opts := []txnkv.ClientOpt{}
	client, err := txnkv.NewClient(pdAddrs, opts...)
	if err != nil {
		return nil, err
	}
	return &TiKVClient{client: client}, nil
}

func (c *TiKVClient) Close() error {
	if c.client == nil {
		return nil
	}

	return c.client.Close()
}

func (c *TiKVClient) Get(ctx context.Context, key []byte) ([]byte, error) {
	if c.client == nil {
		return nil, fmt.Errorf("TiKV client is not initialized")
	}

	tx, err := c.client.Begin()
	if err != nil {
		return nil, fmt.Errorf("failed to begin the transaction with key %s :%w", string(key), err)
	}
	defer tx.Rollback()

	val, err := tx.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get key %s :%w", string(key), err)
	}

	return val, nil
}

func (c *TiKVClient) Scan(ctx context.Context, prefix []byte, limit int) ([]([]byte), []([]byte), error) {
	if c.client == nil {
		return nil, nil, fmt.Errorf("TiKV client is not initialized")
	}

	tx, err := c.client.Begin()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to begin the transaction with prefix %s :%w", string(prefix), err)
	}
	defer tx.Rollback()

	iter, err := tx.Iter(prefix, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create iterator with prefix %s :%w", string(prefix), err)
	}
	defer iter.Close()

	var keys [][]byte
	var values [][]byte
	for i := 0; i < limit && iter.Valid(); i, err = i+1, iter.Next() {
		if err != nil {
			return nil, nil, fmt.Errorf("iterator error at prefix %s :%w", string(prefix), err)
		}

		k := iter.Key()
		if !hasPrefix(k, prefix) {
			break
		}

		kCopy := make([]byte, len(k))
		copy(kCopy, k)

		vCopy := make([]byte, len(iter.Value()))
		copy(vCopy, iter.Value())

		keys = append(keys, kCopy)
		values = append(values, vCopy)
	}

	return keys, values, nil
}

func hasPrefix(s, prefix []byte) bool {
	if len(s) < len(prefix) {
		return false
	}

	for i := range prefix {
		if s[i] != prefix[i] {
			return false
		}
	}

	return true
}
