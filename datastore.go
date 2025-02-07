package datastore

import (
	"context"
	"fmt"
	"io"
)

type DataStoreConfig struct {
	Type   string            `toml:"type"`
	Params map[string]string `toml:"params"`
	Schema DataStoreSchema   `toml:"schema"`
}

// DataStore defines an interface for interacting with data storage
type DataStore interface {
	GetFileMetadata(ctx context.Context, path string) (map[string]string, error)
	GetFile(ctx context.Context, path string) (io.ReadCloser, error)
	PutFile(ctx context.Context, path string, in io.WriterTo, metaData map[string]string) error
	PutFileIfNotExists(ctx context.Context, path string, in io.WriterTo, metaData map[string]string) (bool, error)
	Exists(ctx context.Context, path string) (bool, error)
	Size(ctx context.Context, path string) (int64, error)
	GetSchema() DataStoreSchema
	Close() error
	ListObjectsInRange(ctx context.Context, startSeq, endSeq uint32) ([]string, error)
}

// DefaultRegistry is the global datastore registry
var DefaultRegistry = NewRegistry()

// NewDataStore creates a new DataStore based on the config type
func NewDataStore(ctx context.Context, datastoreConfig DataStoreConfig) (DataStore, error) {
	provider, exists := DefaultRegistry.Get(datastoreConfig.Type)
	if !exists {
		return nil, fmt.Errorf("unsupported datastore type: %s", datastoreConfig.Type)
	}

	return provider(ctx, datastoreConfig.Params, datastoreConfig.Schema)
}
