package core

import (
	"log"
	"os"
	"path/filepath"
	"sync"

	"Emulator-fr-virtuelle-Datenbanken-gobes/pkg/model"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

const databasePath = "dynamodb_emulator_data"

type Database struct {
	DB *leveldb.DB
	Tables map[string]model.TableSchema
	mu sync.RWMutex
}

func NewDatabase() (*Database, error) {
	opts := &opt.Options{
		ErrorIfExist: false,
		Strict: opt.StrictAll,
	}

	db, err := leveldb.OpenFile(databasePath, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	dbInstance := &Database{
		DB: db,
		Tables: make(map[string]model.TableSchema),
	}

	if err := dbInstance.loadTableSchemas(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to load table schemas: %w", err)
	}

	return dbInstance, nil
}

func (d *Database) Close() error {
	return d.DB.Close()
}

func (d *Database) CreateTable(schema model.TableSchema) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, exists := d.Tables[schema.TableName]; exists {
		return fmt.Errorf("table already exists: %s", schema.TableName)
	}

	schemaBytes, err := json.Marshal(schema)
	if err != nil {
		return fmt.Errorf("failed to marshal schema: %w", err)
	}

	schemaKey := d.buildSchemaKey(schema.TableName)
	if err := d.DB.Put([]byte(schemaKey), schemaBytes, nil); err != nil {
		return fmt.Errorf("failed to save schema to DB: %w", err)
	}

	d.Tables[schema.TableName] = schema
	return nil
}

func (d *Database) loadTableSchemas() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	prefix := []byte(schemaPrefix)
	iter := d.DB.NewIterator(util.BytesPrefix(prefix), nil)
	defer iter.Release()

	for iter.Next() {
		var schema model.TableSchema
		if err := json.Unmarshal(iter.Value(), &schema); err != nil {
			log.Printf("Error unmarshaling schema: %v", err)
			continue
		}
		d.Tables[schema.TableName] = schema
	}

	if err := iter.Error(); err != nil {
		return err
	}
	return nil
}

const schemaPrefix = "__SCHEMA__" + model.KeySeparator

func (d *Database) buildSchemaKey(tableName string) string {
	return schemaPrefix + tableName
}

func (d *Database) DeleteAllData() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.DB.Close()
	
	if err := os.RemoveAll(databasePath); err != nil {
		return err
	}

	newDB, err := leveldb.OpenFile(databasePath, &opt.Options{Strict: opt.StrictAll})
	if err != nil {
		log.Fatalf("Failed to re-initialize database after deletion: %v", err)
	}
	d.DB = newDB
	d.Tables = make(map[string]model.TableSchema) 
	
	return nil
}

func (d *Database) CreateSnapshot(snapshotName string) error {
	d.mu.RLock()
	defer d.mu.RUnlock()

	sourceDir := databasePath
	destDir := filepath.Join(model.SnapshotDir, snapshotName)

	if err := os.MkdirAll(destDir, 0755); err != nil {
		return fmt.Errorf("failed to create snapshot directory: %w", err)
	}

	files, err := os.ReadDir(sourceDir)
	if err != nil {
		return fmt.Errorf("failed to read database directory: %w", err)
	}

	for _, file := range files {
		srcPath := filepath.Join(sourceDir, file.Name())
		dstPath := filepath.Join(destDir, file.Name())

		if file.IsDir() {
			continue
		}

		if err := copyFile(srcPath, dstPath); err != nil {
			return fmt.Errorf("failed to copy file %s: %w", file.Name(), err)
		}
	}
	return nil
}

func (d *Database) LoadSnapshot(snapshotName string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	snapshotDir := filepath.Join(model.SnapshotDir, snapshotName)
	if _, err := os.Stat(snapshotDir); os.IsNotExist(err) {
		return fmt.Errorf("snapshot not found: %s", snapshotName)
	}

	d.DB.Close()

	if err := os.RemoveAll(databasePath); err != nil {
		return fmt.Errorf("failed to remove current database: %w", err)
	}
	
	if err := os.Mkdir(databasePath, 0755); err != nil {
		return fmt.Errorf("failed to create new database directory: %w", err)
	}

	files, err := os.ReadDir(snapshotDir)
	if err != nil {
		return fmt.Errorf("failed to read snapshot directory: %w", err)
	}

	for _, file := range files {
		srcPath := filepath.Join(snapshotDir, file.Name())
		dstPath := filepath.Join(databasePath, file.Name())
		
		if file.IsDir() {
			continue
		}

		if err := copyFile(srcPath, dstPath); err != nil {
			return fmt.Errorf("failed to copy file %s from snapshot: %w", file.Name(), err)
		}
	}

	newDB, err := leveldb.OpenFile(databasePath, &opt.Options{Strict: opt.StrictAll})
	if err != nil {
		log.Fatalf("Failed to re-initialize database after loading snapshot: %v", err)
	}
	d.DB = newDB
	
	d.Tables = make(map[string]model.TableSchema) 
	if err := d.loadTableSchemas(); err != nil {
		return fmt.Errorf("failed to reload table schemas after snapshot load: %w", err)
	}

	return nil
}

import (
	"io"
	"os"
)

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}
	return out.Close()
}
