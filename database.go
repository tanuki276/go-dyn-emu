package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

const (
	StoragePath  = "./data"
	SnapshotDir  = "./snapshots"
)

type AttributeValue map[string]interface{}
type Record map[string]AttributeValue

type GsiSchema struct {
	IndexName string
	PartitionKey string
	SortKey string
}

type TableSchema struct {
	TableName string
	PartitionKey string
	SortKey string
	GSIs map[string]GsiSchema
	TTLAttribute string
}

type Database struct {
	mu sync.RWMutex
	DB *leveldb.DB
	Tables map[string]TableSchema
}

func NewDatabase() (*Database, error) {
	if _, err := os.Stat(StoragePath); os.IsNotExist(err) {
		os.MkdirAll(StoragePath, 0755)
	}

	db, err := leveldb.OpenFile(StoragePath, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open LevelDB: %w", err)
	}

	return &Database{
		DB: db,
		Tables: make(map[string]TableSchema),
	}, nil
}

func (d *Database) Close() error {
	return d.DB.Close()
}

func (d *Database) CreateSnapshot(name string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if name == "" {
		return fmt.Errorf("snapshot name cannot be empty")
	}

	dest := filepath.Join(SnapshotDir, name)
	
	if _, err := os.Stat(SnapshotDir); os.IsNotExist(err) {
		os.MkdirAll(SnapshotDir, 0755)
	}

	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		return fmt.Errorf("snapshot '%s' already exists", name)
	}
	
	files, err := os.ReadDir(StoragePath)
	if err != nil {
		return fmt.Errorf("failed to read storage directory: %w", err)
	}

	os.MkdirAll(dest, 0755)
	
	for _, file := range files {
		srcPath := filepath.Join(StoragePath, file.Name())
		dstPath := filepath.Join(dest, file.Name())

		data, err := os.ReadFile(srcPath)
		if err != nil {
			continue
		}
		if err := os.WriteFile(dstPath, data, 0644); err != nil {
			return fmt.Errorf("failed to write file to snapshot: %w", err)
		}
	}
	return nil
}

func (d *Database) LoadSnapshot(name string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	src := filepath.Join(SnapshotDir, name)
	if _, err := os.Stat(src); os.IsNotExist(err) {
		return fmt.Errorf("snapshot '%s' not found", name)
	}
	
	if err := d.DB.Close(); err != nil {
		return fmt.Errorf("failed to close current DB before loading snapshot: %w", err)
	}
	
	if err := os.RemoveAll(StoragePath); err != nil {
		return fmt.Errorf("failed to clean current storage: %w", err)
	}
	os.MkdirAll(StoragePath, 0755) 

	files, err := os.ReadDir(src)
	if err != nil {
		return fmt.Errorf("failed to read snapshot directory: %w", err)
	}

	for _, file := range files {
		srcPath := filepath.Join(src, file.Name())
		dstPath := filepath.Join(StoragePath, file.Name())
		
		data, err := os.ReadFile(srcPath)
		if err != nil {
			continue
		}
		if err := os.WriteFile(dstPath, data, 0644); err != nil {
			return fmt.Errorf("failed to write file from snapshot: %w", err)
		}
	}
	
	newDB, err := leveldb.OpenFile(StoragePath, &opt.Options{})
	if err != nil {
		return fmt.Errorf("failed to reopen LevelDB after loading snapshot: %w", err)
	}
	d.DB = newDB
	return nil
}
