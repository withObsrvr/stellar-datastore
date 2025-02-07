package datastore

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// FSDataStore implements DataStore for local filesystem
type FSDataStore struct {
	basePath string
	schema   DataStoreSchema
}

func NewFSDataStore(basePath string, schema DataStoreSchema) (DataStore, error) {
	// Ensure base path exists
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base directory: %w", err)
	}

	return &FSDataStore{
		basePath: basePath,
		schema:   schema,
	}, nil
}

func (f *FSDataStore) GetFileMetadata(ctx context.Context, path string) (map[string]string, error) {
	fullPath := filepath.Join(f.basePath, path)
	info, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, os.ErrNotExist
		}
		return nil, err
	}

	// Return basic file metadata
	return map[string]string{
		"size":        fmt.Sprintf("%d", info.Size()),
		"modified":    info.ModTime().String(),
		"permissions": info.Mode().String(),
	}, nil
}

func (f *FSDataStore) GetFile(ctx context.Context, path string) (io.ReadCloser, error) {
	fullPath := filepath.Join(f.basePath, path)
	file, err := os.Open(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, os.ErrNotExist
		}
		return nil, err
	}
	return file, nil
}

func (f *FSDataStore) PutFile(ctx context.Context, path string, in io.WriterTo, metadata map[string]string) error {
	fullPath := filepath.Join(f.basePath, path)

	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	file, err := os.Create(fullPath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	_, err = in.WriteTo(file)
	if err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	return nil
}

func (f *FSDataStore) PutFileIfNotExists(ctx context.Context, path string, in io.WriterTo, metadata map[string]string) (bool, error) {
	fullPath := filepath.Join(f.basePath, path)

	// Check if file exists
	if _, err := os.Stat(fullPath); err == nil {
		return false, nil
	}

	if err := f.PutFile(ctx, path, in, metadata); err != nil {
		return false, err
	}

	return true, nil
}

func (f *FSDataStore) Exists(ctx context.Context, path string) (bool, error) {
	fullPath := filepath.Join(f.basePath, path)
	_, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (f *FSDataStore) Size(ctx context.Context, path string) (int64, error) {
	fullPath := filepath.Join(f.basePath, path)
	info, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, os.ErrNotExist
		}
		return 0, err
	}
	return info.Size(), nil
}

func (f *FSDataStore) GetSchema() DataStoreSchema {
	return f.schema
}

func (f *FSDataStore) Close() error {
	return nil
}

func (f *FSDataStore) ListObjectsInRange(ctx context.Context, startSeq, endSeq uint32) ([]string, error) {
	startKey := f.schema.GetObjectKeyFromSequenceNumber(startSeq)
	endKey := f.schema.GetObjectKeyFromSequenceNumber(endSeq)
	baseDir := filepath.Dir(filepath.Join(f.basePath, startKey))

	var objects []string
	err := filepath.Walk(baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		relPath, err := filepath.Rel(f.basePath, path)
		if err != nil {
			return err
		}

		if relPath >= startKey && relPath <= endKey {
			objects = append(objects, relPath)
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to list objects: %w", err)
	}

	sort.Sort(sort.Reverse(sort.StringSlice(objects)))
	return objects, nil
}
