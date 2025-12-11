package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"Emulator-fr-virtuelle-Datenbanken-gobes/pkg/core"
	"Emulator-fr-virtuelle-Datenbanken-gobes/pkg/model"
	"github.com/syndtr/goleveldb/leveldb/iterator"
)

type GetItemInput struct {
	TableName string `json:"TableName"`
	Key model.Record `json:"Key"`
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
		if skAV, ok := input.Key[schema.SortKey]; ok {
			skVal, _ = model.GetAttributeValueString(skAV)
		}
	}

	levelDBKey := model.BuildLevelDBKey(input.TableName, pkVal, skVal)

	s.Database.RLock()
	value, err := s.Database.DB.Get([]byte(levelDBKey), nil)
	s.Database.RUnlock()

	if err == leveldb.ErrNotFound {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
		return
	}
	if err != nil {
		s.writeDynamoDBError(w, "InternalServerError", "Internal DB error", http.StatusInternalServerError)
		return
	}

	var record model.Record
	if err := model.UnmarshalRecord(value, &record); err != nil {
		s.writeDynamoDBError(w, "InternalServerError", "Failed to unmarshal item", http.StatusInternalServerError)
		return
	}

	respBody, _ := json.Marshal(struct {
		Item model.Record `json:"Item"`
	}{
		Item: record,
	})

	w.WriteHeader(http.StatusOK)
	w.Write(respBody)
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

	pkName := schema.PartitionKey
	skName := schema.SortKey
	
	if input.IndexName != "" {
		gsiSchema, ok := schema.GSIs[input.IndexName]
		if !ok {
			s.writeDynamoDBError(w, "ValidationException", fmt.Sprintf("GSI %s not found on table %s", input.IndexName, input.TableName), http.StatusBadRequest)
			return
		}
		pkName = gsiSchema.PartitionKey
		skName = gsiSchema.SortKey
	}

	pkValue, pkOp, err := core.ParseKeyConditionPK(input.KeyConditionExpression, pkName, input.ExpressionAttributeValues)
	if err != nil {
		s.writeDynamoDBError(w, "ValidationException", err.Error(), http.StatusBadRequest)
		return
	}
	if pkOp != "=" {
		s.writeDynamoDBError(w, "ValidationException", "Partition key must use '=' operator in Query", http.StatusBadRequest)
		return
	}

	var skExpression string
	if skName != "" {
		skExpression, err = core.ExtractKeyConditionSK(input.KeyConditionExpression, skName)
		if err != nil {
			s.writeDynamoDBError(w, "ValidationException", err.Error(), http.StatusBadRequest)
			return
		}
	}
	
	var prefix []byte
	if input.IndexName != "" {
		prefix = []byte(model.BuildGSILevelDBKey(input.IndexName, pkValue, "", ""))
	} else {
		prefix = []byte(model.BuildLevelDBKey(input.TableName, pkValue, ""))
	}

	s.Database.RLock()
	defer s.Database.RUnlock()

	iter := s.Database.DB.NewIterator(iterator.Prefix(prefix), nil)
	defer iter.Release()
	
	items := make([]model.Record, 0)
	count := 0
	limit := int(input.Limit)
	if limit == 0 {
		limit = -1 
	}

	startKeyReached := input.ExclusiveStartKey == nil || len(input.ExclusiveStartKey) == 0
	
	// Skip logic based on ExclusiveStartKey
	if !startKeyReached {
		var startKeyDBKey string
		if input.IndexName != "" {
			gsiSchema := schema.GSIs[input.IndexName]
			startPKAV := input.ExclusiveStartKey[gsiSchema.PartitionKey]
			startPKVal, _ := model.GetAttributeValueString(startPKAV)
			
			startSKVal := ""
			if gsiSchema.SortKey != "" {
				startSKAV := input.ExclusiveStartKey[gsiSchema.SortKey]
				startSKVal, _ = model.GetAttributeValueString(startSKAV)
			}
			
			basePKAV := input.ExclusiveStartKey[schema.PartitionKey]
			basePKVal, _ := model.GetAttributeValueString(basePKAV)
			
			startKeyDBKey = model.BuildGSILevelDBKey(input.IndexName, startPKVal, startSKVal, basePKVal)

		} else {
			startPKAV := input.ExclusiveStartKey[schema.PartitionKey]
			startPKVal, _ := model.GetAttributeValueString(startPKAV)
			
			startSKVal := ""
			if schema.SortKey != "" {
				startSKAV := input.ExclusiveStartKey[schema.SortKey]
				startSKVal, _ = model.GetAttributeValueString(startSKAV)
			}
			startKeyDBKey = model.BuildLevelDBKey(input.TableName, startPKVal, startSKVal)
		}

		
		found := iter.Seek([]byte(startKeyDBKey))
		if found && iter.Key() != nil && string(iter.Key()) == startKeyDBKey {
			iter.Next() 
		} else if found {
			
		}
	}

	for iter.Next() {
		if limit != -1 && count >= limit {
			break
		}
		
		var record model.Record
		if err := model.UnmarshalRecord(iter.Value(), &record); err != nil {
			continue
		}

		if skName != "" {
			if skAV, ok := record[skName]; ok {
				skVal, _ := model.GetAttributeValueString(skAV)
				if !core.EvaluateSortKeyCondition(skVal, skExpression, input.ExpressionAttributeValues) {
					continue
				}
			} else if skExpression != "" {
				continue 
			}
		}

		items = append(items, record)
		count++
	}

	if err := iter.Error(); err != nil {
		s.writeDynamoDBError(w, "InternalServerError", fmt.Sprintf("Iterator error: %v", err), http.StatusInternalServerError)
		return
	}

	lastKey := model.Record{}
	if limit != -1 && count >= limit && len(items) > 0 {
		lastKey = core.ExtractKey(items[len(items)-1], schema, input.IndexName)
	}

	respBody, _ := json.Marshal(struct {
		Items []model.Record `json:"Items"`
		Count int `json:"Count"`
		ScannedCount int `json:"ScannedCount"`
		LastEvaluatedKey model.Record `json:"LastEvaluatedKey,omitempty"`
	}{
		Items: items,
		Count: len(items),
		ScannedCount: len(items), 
		LastEvaluatedKey: lastKey,
	})

	w.WriteHeader(http.StatusOK)
	w.Write(respBody)
}

type ScanInput struct {
	TableName string `json:"TableName"`
	Limit int64 `json:"Limit"`
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

	s.Database.RLock()
	defer s.Database.RUnlock()

	iter := s.Database.DB.NewIterator(nil, nil)
	defer iter.Release()

	items := make([]model.Record, 0)
	count := 0
	limit := int(input.Limit)
	if limit == 0 {
		limit = -1 
	}
	
	startKeyReached := true
	if input.ExclusiveStartKey != nil && len(input.ExclusiveStartKey) > 0 {
		startKeyReached = false
		pkAV := input.ExclusiveStartKey[schema.PartitionKey]
		pkVal, _ := model.GetAttributeValueString(pkAV)
		
		skVal := ""
		if schema.SortKey != "" {
			skAV := input.ExclusiveStartKey[schema.SortKey]
			skVal, _ = model.GetAttributeValueString(skAV)
		}
		
		startKeyDBKey := model.BuildLevelDBKey(input.TableName, pkVal, skVal)

		if ok := iter.Seek([]byte(startKeyDBKey)); ok && string(iter.Key()) == startKeyDBKey {
			iter.Next()
			startKeyReached = true
		} else if ok {
			startKeyReached = true
		}
	}
	
	if !startKeyReached {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"message": "Internal error: Seek failed to find start key."}`))
		return
	}

	for iter.Next() {
		
		keyStr := string(iter.Key())
		if !strings.HasPrefix(keyStr, input.TableName+model.KeySeparator) {
			continue 
		}
		
		if limit != -1 && count >= limit {
			break
		}

		var record model.Record
		if err := model.UnmarshalRecord(iter.Value(), &record); err != nil {
			continue
		}

		items = append(items, record)
		count++
	}

	if err := iter.Error(); err != nil {
		s.writeDynamoDBError(w, "InternalServerError", fmt.Sprintf("Iterator error: %v", err), http.StatusInternalServerError)
		return
	}

	lastKey := model.Record{}
	if limit != -1 && count >= limit && len(items) > 0 {
		lastKey = core.ExtractKey(items[len(items)-1], schema, "") 
	}

	respBody, _ := json.Marshal(struct {
		Items []model.Record `json:"Items"`
		Count int `json:"Count"`
		ScannedCount int `json:"ScannedCount"`
		LastEvaluatedKey model.Record `json:"LastEvaluatedKey,omitempty"`
	}{
		Items: items,
		Count: len(items),
		ScannedCount: len(items), 
		LastEvaluatedKey: lastKey,
	})

	w.WriteHeader(http.StatusOK)
	w.Write(respBody)
}
