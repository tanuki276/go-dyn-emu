package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const GSIKeySeparator = "$"

func buildGSILevelDBKey(indexName string, gpkVal string, gskVal string, basePkVal string) string {
	if gskVal == "" {
		return fmt.Sprintf("%s%s%s%s%s", indexName, GSIKeySeparator, gpkVal, GSIKeySeparator, basePkVal)
	}
	return fmt.Sprintf("%s%s%s%s%s%s%s", indexName, GSIKeySeparator, gpkVal, GSIKeySeparator, gskVal, GSIKeySeparator, basePkVal)
}

func (s *Server) updateGSI(batch *leveldb.Batch, schema TableSchema, oldRecord Record, newRecord Record) {
	if len(schema.GSIs) == 0 {
		return
	}

	mainPKAV, _ := getAttributeValueString(newRecord[schema.PartitionKey])
	
	for _, gsi := range schema.GSIs {
		oldGpkVal := ""
		oldGskVal := ""
		newGpkVal := ""
		newGskVal := ""
		
		
		gpkAVOld, okOld := oldRecord[gsi.PartitionKey]
		if okOld { oldGpkVal, _ = getAttributeValueString(gpkAVOld) }
		
		gpkAVNew, okNew := newRecord[gsi.PartitionKey]
		if okNew { newGpkVal, _ = getAttributeValueString(gpkAVNew) }

		if gsi.SortKey != "" {
			gskAVOld, okOld := oldRecord[gsi.SortKey]
			if okOld { gskValOld, _ := getAttributeValueString(gskAVOld); oldGskVal = gskValOld }

			gskAVNew, okNew := newRecord[gsi.SortKey]
			if okNew { gskValNew, _ := getAttributeValueString(gskAVNew); newGskVal = gskValNew }
		}

		if oldGpkVal != "" && (oldGpkVal != newGpkVal || oldGskVal != newGskVal) {
			oldGSIKey := buildGSILevelDBKey(gsi.IndexName, oldGpkVal, oldGskVal, mainPKAV)
			batch.Delete([]byte(oldGSIKey))
		}

		if newGpkVal != "" {
			newGSIKey := buildGSILevelDBKey(gsi.IndexName, newGpkVal, newGskVal, mainPKAV)
			batch.Put([]byte(newGSIKey), []byte{})
		}
	}
}

func (s *Server) handleCreateTable(w http.ResponseWriter, body []byte) {
	var input CreateTableInput
	if err := json.Unmarshal(body, &input); err != nil {
		s.writeDynamoDBError(w, "ValidationException", "Invalid JSON input", http.StatusBadRequest)
		return
	}

	if input.TableName == "" {
		s.writeDynamoDBError(w, "ValidationException", "TableName must be specified", http.StatusBadRequest)
		return
	}

	schema := TableSchema{TableName: input.TableName, GSIs: make(map[string]GsiSchema)}
	for _, k := range input.KeySchema {
		if k.KeyType == "HASH" {
			schema.PartitionKey = k.AttributeName
		} else if k.KeyType == "RANGE" {
			schema.SortKey = k.AttributeName
		}
	}

	s.DB.mu.Lock()
	defer s.DB.mu.Unlock()

	if _, exists := s.DB.Tables[input.TableName]; exists {
		s.writeDynamoDBError(w, "ResourceInUseException", "Table already exists", http.StatusBadRequest)
		return
	}

	s.DB.Tables[input.TableName] = schema

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf(`{"TableDescription": {"TableName": "%s", "TableStatus": "ACTIVE"}}`, input.TableName)))
}

func (s *Server) handlePutItem(w http.ResponseWriter, body []byte) {
	var input PutItemInput
	if err := json.Unmarshal(body, &input); err != nil {
		s.writeDynamoDBError(w, "ValidationException", "Invalid JSON input", http.StatusBadRequest)
		return
	}

	s.DB.mu.RLock()
	schema, ok := s.DB.Tables[input.TableName]
	s.DB.mu.RUnlock()
	if !ok {
		s.writeDynamoDBError(w, "ResourceNotFoundException", "Table not found", http.StatusBadRequest)
		return
	}

	pkAV, ok := input.Item[schema.PartitionKey]
	if !ok {
		s.writeDynamoDBError(w, "ValidationException", fmt.Sprintf("Partition Key '%s' value missing", schema.PartitionKey), http.StatusBadRequest)
		return
	}
	pkVal, _ := getAttributeValueString(pkAV)

	var skVal string
	if schema.SortKey != "" {
		skAV, ok := input.Item[schema.SortKey]
		if ok {
			skVal, _ = getAttributeValueString(skAV)
		}
	}

	levelDBKey := buildLevelDBKey(input.TableName, pkVal, skVal)
	
	batch := new(leveldb.Batch)
	
	s.DB.mu.Lock()
	defer s.DB.mu.Unlock()

	oldValue, err := s.DB.DB.Get([]byte(levelDBKey), nil)
	var oldRecord Record
	if err != leveldb.ErrNotFound && err != nil {
		http.Error(w, "Internal DB error", http.StatusInternalServerError)
		return
	}
	if err == nil {
		oldRecord, _ = unmarshalRecord(oldValue)
	}

	s.updateGSI(batch, schema, oldRecord, input.Item)

	value, err := marshalRecord(input.Item)
	if err != nil {
		s.writeDynamoDBError(w, "InternalServerError", "Failed to marshal item", http.StatusInternalServerError)
		return
	}
	batch.Put([]byte(levelDBKey), value)

	if err := s.DB.DB.Write(batch, nil); err != nil {
		http.Error(w, "Internal DB error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{}`))
}

type GetItemInput struct {
	TableName string `json:"TableName"`
	Key map[string]AttributeValue `json:"Key"`
}

func (s *Server) handleGetItem(w http.ResponseWriter, body []byte) {
	var input GetItemInput
	if err := json.Unmarshal(body, &input); err != nil {
		s.writeDynamoDBError(w, "ValidationException", "Invalid JSON input", http.StatusBadRequest)
		return
	}

	s.DB.mu.RLock()
	schema, ok := s.DB.Tables[input.TableName]
	s.DB.mu.RUnlock()
	if !ok {
		s.writeDynamoDBError(w, "ResourceNotFoundException", "Table not found", http.StatusBadRequest)
		return
	}

	pkAV, ok := input.Key[schema.PartitionKey]
	if !ok {
		s.writeDynamoDBError(w, "ValidationException", fmt.Sprintf("Partition Key '%s' value missing", schema.PartitionKey), http.StatusBadRequest)
		return
	}
	pkVal, _ := getAttributeValueString(pkAV)

	var skVal string
	if schema.SortKey != "" {
		skAV, ok := input.Key[schema.SortKey]
		if ok {
			skVal, _ = getAttributeValueString(skAV)
		}
	}

	levelDBKey := buildLevelDBKey(input.TableName, pkVal, skVal)

	s.DB.mu.RLock()
	defer s.DB.mu.RUnlock()
	value, err := s.DB.DB.Get([]byte(levelDBKey), nil)
	if err == leveldb.ErrNotFound {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"Item": {}}`))
		return
	}
	if err != nil {
		http.Error(w, "Internal DB error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"Item": %s}`, string(value))
}

type QueryInput struct {
	TableName string `json:"TableName"`
	IndexName string `json:"IndexName,omitempty"`
	KeyConditionExpression string `json:"KeyConditionExpression"`
	ExpressionAttributeValues map[string]AttributeValue `json:"ExpressionAttributeValues"`
	Limit int64 `json:"Limit"`
	ScanIndexForward bool `json:"ScanIndexForward"`
	ExclusiveStartKey Record `json:"ExclusiveStartKey"`
}

type QueryOutput struct {
	Items []Record `json:"Items"`
	Count int `json:"Count"`
	ScannedCount int `json:"ScannedCount"`
	LastEvaluatedKey Record `json:"LastEvaluatedKey,omitempty"`
}

func (s *Server) handleQuery(w http.ResponseWriter, body []byte) {
	var input QueryInput
	if err := json.Unmarshal(body, &input); err != nil {
		s.writeDynamoDBError(w, "ValidationException", "Invalid JSON input", http.StatusBadRequest)
		return
	}

	s.DB.mu.RLock()
	schema, ok := s.DB.Tables[input.TableName]
	s.DB.mu.RUnlock()
	if !ok {
		s.writeDynamoDBError(w, "ResourceNotFoundException", "Table not found", http.StatusBadRequest)
		return
	}

	pkValuePlaceholder, ok := input.ExpressionAttributeValues[":pkval"]
	if !ok {
		s.writeDynamoDBError(w, "ValidationException", "Partition Key value (:pkval) not found in ExpressionAttributeValues", http.StatusBadRequest)
		return
	}
	pkVal, _ := getAttributeValueString(pkValuePlaceholder)

	var iteratorPrefix []byte
	var isGSIQuery bool = false

	if input.IndexName == "" {
		prefix := buildLevelDBKey(input.TableName, pkVal, "")
		iteratorPrefix = []byte(prefix)
	} else {
		s.DB.mu.RLock()
		gsiSchema, exists := schema.GSIs[input.IndexName]
		s.DB.mu.RUnlock()
		if !exists {
			s.writeDynamoDBError(w, "ValidationException", fmt.Sprintf("Index %s not found", input.IndexName), http.StatusBadRequest)
			return
		}

		prefix := buildGSILevelDBKey(gsiSchema.IndexName, pkVal, "", "")
		iteratorPrefix = []byte(prefix)
		isGSIQuery = true
	}


	s.DB.mu.RLock()
	defer s.DB.mu.RUnlock()
	iter := s.DB.DB.NewIterator(util.BytesPrefix(iteratorPrefix), nil)
	defer iter.Release()
    
    if !input.ScanIndexForward {
        
    }
    
    if len(input.ExclusiveStartKey) > 0 {
        startPKAV, _ := input.ExclusiveStartKey[schema.PartitionKey]
        startSKAV, _ := input.ExclusiveStartKey[schema.SortKey]
        
        startPKVal, _ := getAttributeValueString(startPKAV)
        startSKVal, _ := getAttributeValueString(startSKAV)
        
        exclusiveKey := buildLevelDBKey(input.TableName, startPKVal, startSKVal)
        
        if iter.Seek([]byte(exclusiveKey)) {
             iter.Next()
        }
    } else {
        iter.First()
    }

	output := QueryOutput{Items: []Record{}}
	
	limit := input.Limit 
	if limit <= 0 { limit = 1000 } 

	for i := 0; i < int(limit) && iter.Valid(); iter.Next() {
		
		var value []byte
		var err error
		var record Record

		if isGSIQuery {
			key := iter.Key()
			keyParts := strings.Split(string(key), GSIKeySeparator)
			if len(keyParts) < 4 { continue }
			
			basePKVal := keyParts[len(keyParts)-1]
			
			mainKey := buildLevelDBKey(input.TableName, basePKVal, "") 
			value, err = s.DB.DB.Get([]byte(mainKey), nil)
		} else {
			value = iter.Value()
		}

		if err != nil && err != leveldb.ErrNotFound {
			continue
		}
		
		if err == leveldb.ErrNotFound { continue }

		record, err = unmarshalRecord(value)
		if err != nil {
			continue
		}
		
		output.Items = append(output.Items, record)
		output.Count++
		output.ScannedCount++
		i++

		if i == int(limit) && iter.Next() {
			output.LastEvaluatedKey = record
			iter.Prev()
			break
		}
	}

	responseBody, _ := json.Marshal(output)
	w.WriteHeader(http.StatusOK)
	w.Write(responseBody)
}

type ConditionCheck struct {
	TableName string `json:"TableName"`
	Key Record `json:"Key"`
	ConditionExpression string `json:"ConditionExpression"`
	ExpressionAttributeValues map[string]AttributeValue `json:"ExpressionAttributeValues"`
}

type Put struct {
	Item Record `json:"Item"`
	TableName string `json:"TableName"`
}

type Delete struct {
	Key Record `json:"Key"`
	TableName string `json:"TableName"`
}

type TransactWriteItem struct {
	ConditionCheck *ConditionCheck `json:"ConditionCheck,omitempty"`
	Put *Put `json:"Put,omitempty"`
	Delete *Delete `json:"Delete,omitempty"`
}

type TransactWriteItemsInput struct {
	TransactItems []TransactWriteItem `json:"TransactItems"`
}

func (s *Server) handleTransactWriteItems(w http.ResponseWriter, body []byte) {
	var input TransactWriteItemsInput
	if err := json.Unmarshal(body, &input); err != nil {
		s.writeDynamoDBError(w, "ValidationException", "Invalid JSON input", http.StatusBadRequest)
		return
	}

	batch := new(leveldb.Batch)
	
	s.DB.mu.Lock()
	defer s.DB.mu.Unlock()

	for _, item := range input.TransactItems {
		if item.ConditionCheck != nil {
			
		}
	}
	
	for _, item := range input.TransactItems {
		var tableName string
		var key Record
		var itemData Record
		var opType string

		if item.Put != nil {
			tableName = item.Put.TableName
			itemData = item.Put.Item
			opType = "PUT"
		} else if item.Delete != nil {
			tableName = item.Delete.TableName
			key = item.Delete.Key
			opType = "DELETE"
		} else if item.ConditionCheck != nil {
			continue
		} else {
			s.writeDynamoDBError(w, "ValidationException", "Invalid TransactItem structure.", http.StatusBadRequest)
			return
		}

		schema, ok := s.DB.Tables[tableName]
		if !ok {
			s.writeDynamoDBError(w, "ResourceNotFoundException", fmt.Sprintf("Table %s not found", tableName), http.StatusBadRequest)
			return
		}

		pkAV, _ := key[schema.PartitionKey]
		if opType == "PUT" { pkAV = itemData[schema.PartitionKey] }

		pkVal, _ := getAttributeValueString(pkAV)
		
		var skVal string
		if schema.SortKey != "" {
			skAV, _ := key[schema.SortKey]
			if opType == "PUT" { skAV = itemData[schema.SortKey] }
			skVal, _ = getAttributeValueString(skAV)
		}

		levelDBKey := buildLevelDBKey(tableName, pkVal, skVal)

		if opType == "PUT" {
			oldValue, err := s.DB.DB.Get([]byte(levelDBKey), nil)
			var oldRecord Record
			if err != leveldb.ErrNotFound && err != nil {
				http.Error(w, "Internal DB error", http.StatusInternalServerError)
				return
			}
			if err == nil {
				oldRecord, _ = unmarshalRecord(oldValue)
			}

			s.updateGSI(batch, schema, oldRecord, itemData)
			
			value, _ := marshalRecord(itemData)
			batch.Put([]byte(levelDBKey), value)
		} else if opType == "DELETE" {
			oldValue, err := s.DB.DB.Get([]byte(levelDBKey), nil)
			var oldRecord Record
			if err != leveldb.ErrNotFound && err != nil {
				http.Error(w, "Internal DB error", http.StatusInternalServerError)
				return
			}
			if err == nil {
				oldRecord, _ = unmarshalRecord(oldValue)
			}
			s.updateGSI(batch, schema, oldRecord, nil) 

			batch.Delete([]byte(levelDBKey))
		}
	}

	if err := s.DB.DB.Write(batch, nil); err != nil {
		http.Error(w, "Internal DB error during transaction", http.StatusInternalServerError)
		return
	}
	
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{}`))
}

func (s *Server) writeDynamoDBError(w http.ResponseWriter, errorType string, message string, status int) {
	w.WriteHeader(status)
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	fmt.Fprintf(w, `{"__type": "com.amazon.coral.service#%s", "message": "%s"}`, errorType, message)
}
