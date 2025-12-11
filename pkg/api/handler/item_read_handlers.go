package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
    
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"Emulator-fr-virtuelle-Datenbanken-gobes/pkg/core"
	"Emulator-fr-virtuelle-Datenbanken-gobes/pkg/model"
)

type GetItemInput struct {
	TableName string `json:"TableName"`
	Key map[string]model.AttributeValue `json:"Key"`
}

func (s *Server) handleGetItem(w http.ResponseWriter, body []byte) {
	var input GetItemInput
	if err := json.Unmarshal(body, &input); err != nil {
		s.writeDynamoDBError(w, "ValidationException", "Invalid JSON input", http.StatusBadRequest)
		return
	}

	s.Database.RLock()
	schema, ok := s.Database.Tables[input.TableName]
	s.Database.RUnlock()
	if !ok {
		s.writeDynamoDBError(w, "ResourceNotFoundException", "Table not found", http.StatusBadRequest)
		return
	}

	pkAV, ok := input.Key[schema.PartitionKey]
	if !ok {
		s.writeDynamoDBError(w, "ValidationException", fmt.Sprintf("Partition Key '%s' value missing", schema.PartitionKey), http.StatusBadRequest)
		return
	}
	pkVal, _ := model.GetAttributeValueString(pkAV)

	var skVal string
	if schema.SortKey != "" {
		skAV, ok := input.Key[schema.SortKey]
		if ok {
			skVal, _ = model.GetAttributeValueString(skAV)
		}
	}

	levelDBKey := model.BuildLevelDBKey(input.TableName, pkVal, skVal)

	s.Database.RLock()
	defer s.Database.RUnlock()
	value, err := s.Database.DB.Get([]byte(levelDBKey), nil)
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

type QueryOutput struct {
	Items []model.Record `json:"Items"`
	Count int `json:"Count"`
	ScannedCount int `json:"ScannedCount"`
	LastEvaluatedKey model.Record `json:"LastEvaluatedKey,omitempty"`
}

func (s *Server) handleQuery(w http.ResponseWriter, body []byte) {
    var input model.QueryInput
	if err := json.Unmarshal(body, &input); err != nil {
		s.writeDynamoDBError(w, "ValidationException", "Invalid JSON input", http.StatusBadRequest)
		return
	}

	s.Database.RLock()
	schema, ok := s.Database.Tables[input.TableName]
	s.Database.RUnlock()
	if !ok {
		s.writeDynamoDBError(w, "ResourceNotFoundException", "Table not found", http.StatusBadRequest)
		return
	}

	pkValuePlaceholder, ok := input.ExpressionAttributeValues[":pkval"]
	if !ok {
		s.writeDynamoDBError(w, "ValidationException", "Partition Key value (:pkval) not found in ExpressionAttributeValues", http.StatusBadRequest)
		return
	}
	pkVal, _ := model.GetAttributeValueString(pkValuePlaceholder)

	var iteratorPrefix []byte
	var isGSIQuery bool = false

	if input.IndexName == "" {
		prefix := model.BuildLevelDBKey(input.TableName, pkVal, "")
		iteratorPrefix = []byte(prefix)
	} else {
		s.Database.RLock()
		gsiSchema, exists := schema.GSIs[input.IndexName]
		s.Database.RUnlock()
		if !exists {
			s.writeDynamoDBError(w, "ValidationException", fmt.Sprintf("Index %s not found", input.IndexName), http.StatusBadRequest)
			return
		}

		prefix := model.BuildGSILevelDBKey(gsiSchema.IndexName, pkVal, "", "")
		iteratorPrefix = []byte(prefix)
		isGSIQuery = true
	}


	s.Database.RLock()
	defer s.Database.RUnlock()
	iter := s.Database.DB.NewIterator(util.BytesPrefix(iteratorPrefix), nil)
	defer iter.Release()

    if len(input.ExclusiveStartKey) > 0 {
        startPKAV, _ := input.ExclusiveStartKey[schema.PartitionKey]
        startSKAV, _ := input.ExclusiveStartKey[schema.SortKey]

        startPKVal, _ := model.GetAttributeValueString(startPKAV)
        startSKVal, _ := model.GetAttributeValueString(startSKAV)

        exclusiveKey := model.BuildLevelDBKey(input.TableName, startPKVal, startSKVal)

        if iter.Seek([]byte(exclusiveKey)) {
             iter.Next()
        }
    } else {
        iter.First()
    }

	output := QueryOutput{Items: []model.Record{}}

	limit := input.Limit 
	if limit <= 0 { limit = 1000 } 

	for i := 0; i < int(limit) && iter.Valid(); iter.Next() {

		var value []byte
		var err error
		var record model.Record

		if isGSIQuery {
			key := iter.Key()
			keyParts := strings.Split(string(key), model.GSIKeySeparator)
			if len(keyParts) < 4 { continue }

			basePKVal := keyParts[len(keyParts)-1]

			mainKey := model.BuildLevelDBKey(input.TableName, basePKVal, "") 
			value, err = s.Database.DB.Get([]byte(mainKey), nil)
		} else {
			value = iter.Value()
		}

		if err != nil && err != leveldb.ErrNotFound {
			continue
		}

		if err == leveldb.ErrNotFound { continue }

		record, err = model.UnmarshalRecord(value)
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

type ScanInput struct {
	TableName string `json:"TableName"`
	IndexName string `json:"IndexName,omitempty"`
	FilterExpression string `json:"FilterExpression,omitempty"`
	ExpressionAttributeNames map[string]string `json:"ExpressionAttributeNames,omitempty"`
	ExpressionAttributeValues map[string]model.AttributeValue `json:"ExpressionAttributeValues,omitempty"`
	Limit int64 `json:"Limit,omitempty"`
	ExclusiveStartKey model.Record `json:"ExclusiveStartKey,omitempty"`
}

func (s *Server) handleScan(w http.ResponseWriter, body []byte) {
    var input ScanInput
	if err := json.Unmarshal(body, &input); err != nil {
		s.writeDynamoDBError(w, "ValidationException", "Invalid JSON input", http.StatusBadRequest)
		return
	}

	s.Database.RLock()
	schema, ok := s.Database.Tables[input.TableName]
	s.Database.RUnlock()
	if !ok {
		s.writeDynamoDBError(w, "ResourceNotFoundException", "Table not found", http.StatusBadRequest)
		return
	}

	var iteratorPrefix []byte
	isGSIQuery := false
	
	if input.IndexName == "" {
		iteratorPrefix = []byte(input.TableName + model.KeySeparator)
	} else {
		s.Database.RLock()
		gsiSchema, exists := schema.GSIs[input.IndexName]
		s.Database.RUnlock()
		if !exists {
			s.writeDynamoDBError(w, "ValidationException", fmt.Sprintf("Index %s not found", input.IndexName), http.StatusBadRequest)
			return
		}
		iteratorPrefix = []byte(gsiSchema.IndexName + model.GSIKeySeparator)
		isGSIQuery = true
	}
    
    s.Database.RLock()
	iter := s.Database.DB.NewIterator(util.BytesPrefix(iteratorPrefix), nil)
	defer iter.Release()
	s.Database.RUnlock()
    
	if len(input.ExclusiveStartKey) > 0 {
        startPKAV, _ := input.ExclusiveStartKey[schema.PartitionKey]
        startSKAV, _ := input.ExclusiveStartKey[schema.SortKey]

        startPKVal, _ := model.GetAttributeValueString(startPKAV)
        startSKVal, _ := model.GetAttributeValueString(startSKAV)

        exclusiveKey := model.BuildLevelDBKey(input.TableName, startPKVal, startSKVal)

        if iter.Seek([]byte(exclusiveKey)) {
             iter.Next()
        }
    } else {
        iter.First()
    }

	output := QueryOutput{Items: []model.Record{}}

	limit := input.Limit 
	if limit <= 0 { limit = 1000 } 

	for i := 0; i < int(limit) && iter.Valid(); iter.Next() {
		
		var value []byte
		var err error
		var record model.Record
        
        key := iter.Key()

		if !isGSIQuery {
			value = iter.Value()
		} else {
            keyParts := strings.Split(string(key), model.GSIKeySeparator)
			if len(keyParts) < 4 { continue }

			basePKVal := keyParts[len(keyParts)-1]

			mainKey := model.BuildLevelDBKey(input.TableName, basePKVal, "") 
            s.Database.RLock()
			value, err = s.Database.DB.Get([]byte(mainKey), nil)
            s.Database.RUnlock()
		}

		if err != nil && err != leveldb.ErrNotFound {
			continue
		}

		if err == leveldb.ErrNotFound { continue }

		record, err = model.UnmarshalRecord(value)
		if err != nil {
			continue
		}
        
        if input.FilterExpression != "" {
            conditionInput := model.ConditionInput{
                ConditionExpression:       input.FilterExpression,
                ExpressionAttributeNames:  input.ExpressionAttributeNames,
                ExpressionAttributeValues: input.ExpressionAttributeValues,
            }
            
            ok, condErr := core.EvaluateConditionExpression(record, conditionInput)
            if condErr != nil {
                s.writeDynamoDBError(w, "ValidationException", condErr.Error(), http.StatusBadRequest)
                return
            }
            if !ok {
                output.ScannedCount++
                continue
            }
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

// --- BatchGetItem handler ---

type KeysAndAttributes struct {
	Keys []model.Record `json:"Keys"`
	TableName string `json:"TableName"`
}

type BatchGetItemInput struct {
	RequestItems map[string]KeysAndAttributes `json:"RequestItems"`
}

type BatchGetItemOutput struct {
	Responses map[string][]model.Record `json:"Responses"`
	UnprocessedKeys map[string]KeysAndAttributes `json:"UnprocessedKeys,omitempty"`
}

func (s *Server) handleBatchGetItem(w http.ResponseWriter, body []byte) {
	var input BatchGetItemInput
	if err := json.Unmarshal(body, &input); err != nil {
		s.writeDynamoDBError(w, "ValidationException", "Invalid JSON input", http.StatusBadRequest)
		return
	}

	output := BatchGetItemOutput{Responses: make(map[string][]model.Record)}
	
	s.Database.RLock()
	defer s.Database.RUnlock()

	for tableName, request := range input.RequestItems {
		
		schema, ok := s.Database.Tables[tableName]
		if !ok {
			s.writeDynamoDBError(w, "ResourceNotFoundException", fmt.Sprintf("Table %s not found", tableName), http.StatusBadRequest)
			return
		}

		if len(request.Keys) == 0 {
			continue
		}
		
		var items []model.Record
		
		for _, keyMap := range request.Keys {
			pkAV, ok := keyMap[schema.PartitionKey]
			if !ok {
				s.writeDynamoDBError(w, "ValidationException", fmt.Sprintf("Partition Key '%s' value missing for table %s", schema.PartitionKey, tableName), http.StatusBadRequest)
				return
			}
			pkVal, _ := model.GetAttributeValueString(pkAV)

			var skVal string
			if schema.SortKey != "" {
				skAV, ok := keyMap[schema.SortKey]
				if ok {
					skVal, _ = model.GetAttributeValueString(skAV)
				}
			}

			levelDBKey := model.BuildLevelDBKey(tableName, pkVal, skVal)

			value, err := s.Database.DB.Get([]byte(levelDBKey), nil)
			if err == nil {
				record, err := model.UnmarshalRecord(value)
				if err == nil {
					items = append(items, record)
				}
			} else if err != leveldb.ErrNotFound {
				http.Error(w, "Internal DB error", http.StatusInternalServerError)
				return
			}
		}

		if len(items) > 0 {
			output.Responses[tableName] = items
		}
	}

	responseBody, _ := json.Marshal(output)
	w.WriteHeader(http.StatusOK)
	w.Write(responseBody)
}
