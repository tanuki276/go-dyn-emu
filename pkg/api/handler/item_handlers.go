// pkg/api/handler/item_handlers.go
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

// --------------------------------------------------
// PUT ITEM
// --------------------------------------------------

func (s *Server) handlePutItem(w http.ResponseWriter, body []byte) {
	var input model.PutItemInput
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

    pkAV, ok := input.Item[schema.PartitionKey]
    if !ok {
        s.writeDynamoDBError(w, "ValidationException", fmt.Sprintf("Partition Key '%s' value missing", schema.PartitionKey), http.StatusBadRequest)
        return
    }
    pkVal, _ := model.GetAttributeValueString(pkAV)

    var skVal string
    if schema.SortKey != "" {
        skAV, ok := input.Item[schema.SortKey]
        if ok {
            skVal, _ = model.GetAttributeValueString(skAV)
        }
    }

    levelDBKey := model.BuildLevelDBKey(input.TableName, pkVal, skVal)

	batch := new(leveldb.Batch)
	s.Database.Lock()
	defer s.Database.Unlock()

	oldValue, err := s.Database.DB.Get([]byte(levelDBKey), nil)
	var oldRecord model.Record
	recordExists := err == nil
	if err == nil {
		oldRecord, _ = model.UnmarshalRecord(oldValue)
	}

	if input.ConditionExpression != "" {
		conditionInput := model.ConditionInput{
			ConditionExpression:       input.ConditionExpression,
			ExpressionAttributeNames:  input.ExpressionAttributeNames,
			ExpressionAttributeValues: input.ExpressionAttributeValues,
		}
		
		recordForEvaluation := oldRecord
		if !recordExists { recordForEvaluation = nil }
		
		ok, condErr := core.EvaluateConditionExpression(recordForEvaluation, conditionInput)
		
		if condErr != nil {
			s.writeDynamoDBError(w, "ValidationException", condErr.Error(), http.StatusBadRequest)
			return
		}
		if !ok {
			s.writeDynamoDBError(w, "ConditionCheckFailedException", "The conditional request failed.", http.StatusBadRequest)
			return
		}
	}

	core.UpdateGSI(batch, schema, oldRecord, input.Item)

	value, err := model.MarshalRecord(input.Item)
	if err != nil {
		s.writeDynamoDBError(w, "InternalServerError", "Failed to marshal item", http.StatusInternalServerError)
		return
	}
	batch.Put([]byte(levelDBKey), value)

	if err := s.Database.DB.Write(batch, nil); err != nil {
		http.Error(w, "Internal DB error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{}`))
}

// --------------------------------------------------
// GET ITEM
// --------------------------------------------------

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

// --------------------------------------------------
// QUERY
// --------------------------------------------------

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

// --------------------------------------------------
// DELETE ITEM
// --------------------------------------------------

type DeleteItemInput struct {
	TableName string `json:"TableName"`
	Key map[string]model.AttributeValue `json:"Key"`
	ReturnValues string `json:"ReturnValues,omitempty"`
    ConditionExpression string `json:"ConditionExpression,omitempty"`
    ExpressionAttributeNames map[string]string `json:"ExpressionAttributeNames,omitempty"`
    ExpressionAttributeValues map[string]model.AttributeValue `json:"ExpressionAttributeValues,omitempty"`
}

func (s *Server) handleDeleteItem(w http.ResponseWriter, body []byte) {
	var input DeleteItemInput
	if err := json.Unmarshal(body, &input); err != nil {
		s.writeDynamoDBError(w, "ValidationException", "Invalid JSON input for DeleteItem", http.StatusBadRequest)
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
		s.writeDynamoDBError(w, "ValidationException", fmt.Sprintf("Partition Key '%s' value missing in Key", schema.PartitionKey), http.StatusBadRequest)
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

	s.Database.Lock()
	defer s.Database.Unlock()

	oldValue, err := s.Database.DB.Get([]byte(levelDBKey), nil)
	recordExists := err == nil
	var oldRecord model.Record
	
	if err == leveldb.ErrNotFound {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
		return
	}
	if err != nil {
		http.Error(w, "Internal DB error on retrieve", http.StatusInternalServerError)
		return
	}
	
	if err := model.UnmarshalRecord(oldValue, &oldRecord); err != nil {
		http.Error(w, "Failed to unmarshal existing item", http.StatusInternalServerError)
		return
	}

    if input.ConditionExpression != "" {
		conditionInput := model.ConditionInput{
			ConditionExpression:       input.ConditionExpression,
			ExpressionAttributeNames:  input.ExpressionAttributeNames,
			ExpressionAttributeValues: input.ExpressionAttributeValues,
		}
		
		ok, condErr := core.EvaluateConditionExpression(oldRecord, conditionInput)
		
		if condErr != nil {
			s.writeDynamoDBError(w, "ValidationException", condErr.Error(), http.StatusBadRequest)
			return
		}
		if !ok {
			s.writeDynamoDBError(w, "ConditionCheckFailedException", "The conditional request failed.", http.StatusBadRequest)
			return
		}
	}

	batch := new(leveldb.Batch)
	core.UpdateGSI(batch, schema, oldRecord, nil) 

	batch.Delete([]byte(levelDBKey))

	if err := s.Database.DB.Write(batch, nil); err != nil {
		http.Error(w, "Internal DB error on write", http.StatusInternalServerError)
		return
	}

    responseBody := []byte(`{}`)
    if input.ReturnValues == "ALL_OLD" {
        marshaledOldRecord, _ := json.Marshal(oldRecord)
        responseBody = []byte(fmt.Sprintf(`{"Attributes": %s}`, marshaledOldRecord))
    }

	w.WriteHeader(http.StatusOK)
	w.Write(responseBody)
}

// --------------------------------------------------
// UPDATE ITEM
// --------------------------------------------------

func (s *Server) handleUpdateItem(w http.ResponseWriter, body []byte) {
	var input model.UpdateItemInput
	if err := json.Unmarshal(body, &input); err != nil {
		s.writeDynamoDBError(w, "ValidationException", "Invalid JSON input for UpdateItem", http.StatusBadRequest)
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
		s.writeDynamoDBError(w, "ValidationException", fmt.Sprintf("Partition Key '%s' value missing in Key", schema.PartitionKey), http.StatusBadRequest)
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

	s.Database.Lock()
	defer s.Database.Unlock()

	oldValue, err := s.Database.DB.Get([]byte(levelDBKey), nil)
	oldRecord := make(model.Record)
    recordExists := err == nil
	if err != leveldb.ErrNotFound && err != nil {
		http.Error(w, "Internal DB error on retrieve", http.StatusInternalServerError)
		return
	}
	if err == nil {
		oldRecord, _ = model.UnmarshalRecord(oldValue)
	}

    if input.ConditionExpression != "" {
        conditionInput := model.ConditionInput{
            ConditionExpression:       input.ConditionExpression,
            ExpressionAttributeNames:  input.ExpressionAttributeNames,
            ExpressionAttributeValues: input.ExpressionAttributeValues,
        }
        
        recordForEvaluation := oldRecord
        if !recordExists { recordForEvaluation = nil }
        
        ok, condErr := core.EvaluateConditionExpression(recordForEvaluation, conditionInput)
        
        if condErr != nil {
            s.writeDynamoDBError(w, "ValidationException", condErr.Error(), http.StatusBadRequest)
            return
        }
        if !ok {
            s.writeDynamoDBError(w, "ConditionCheckFailedException", "The conditional request failed.", http.StatusBadRequest)
            return
        }
    }
    
    actions, err := core.ParseUpdateExpression(&input) 
    if err != nil {
        s.writeDynamoDBError(w, "ValidationException", err.Error(), http.StatusBadRequest)
        return
    }

    newRecord, err := core.ApplyUpdateActions(oldRecord, actions)
    if err != nil {
        s.writeDynamoDBError(w, "ValidationException", err.Error(), http.StatusBadRequest)
        return
    }
    
    if _, ok := actions.Set[schema.PartitionKey]; ok || len(actions.Add[schema.PartitionKey]) > 0 {
        s.writeDynamoDBError(w, "ValidationException", "Cannot update Partition Key", http.StatusBadRequest)
        return
    }
    if schema.SortKey != "" {
        if _, ok := actions.Set[schema.SortKey]; ok || len(actions.Add[schema.SortKey]) > 0 {
             s.writeDynamoDBError(w, "ValidationException", "Cannot update Sort Key", http.StatusBadRequest)
            return
        }
    }

	batch := new(leveldb.Batch)
	core.UpdateGSI(batch, schema, oldRecord, newRecord)

	value, err := model.MarshalRecord(newRecord)
	if err != nil {
		s.writeDynamoDBError(w, "InternalServerError", "Failed to marshal updated item", http.StatusInternalServerError)
		return
	}
	batch.Put([]byte(levelDBKey), value)

	if err := s.Database.DB.Write(batch, nil); err != nil {
		http.Error(w, "Internal DB error on write", http.StatusInternalServerError)
		return
	}

    responseBody := []byte(`{}`)
    if input.ReturnValues == "ALL_NEW" {
        marshaledNewRecord, _ := json.Marshal(newRecord)
        responseBody = []byte(fmt.Sprintf(`{"Attributes": %s}`, marshaledNewRecord))
    }

	w.WriteHeader(http.StatusOK)
	w.Write(responseBody)
}

// --------------------------------------------------
// TRANSACT WRITE ITEMS
// --------------------------------------------------

type TransactWriteItem struct {
	ConditionCheck *struct {
		TableName string `json:"TableName"`
		Key model.Record `json:"Key"`
		ConditionExpression string `json:"ConditionExpression"`
		ExpressionAttributeValues map[string]model.AttributeValue `json:"ExpressionAttributeValues"`
	} `json:"ConditionCheck,omitempty"`
	Put *struct {
		Item model.Record `json:"Item"`
		TableName string `json:"TableName"`
	} `json:"Put,omitempty"`
	Delete *struct {
		Key model.Record `json:"Key"`
		TableName string `json:"TableName"`
	} `json:"Delete,omitempty"`
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

	s.Database.Lock()
	defer s.Database.Unlock()

	for _, item := range input.TransactItems {
		if item.ConditionCheck != nil {
			cc := item.ConditionCheck
			
			schema, ok := s.Database.Tables[cc.TableName]
			if !ok {
				s.writeDynamoDBError(w, "ResourceNotFoundException", fmt.Sprintf("Table %s not found", cc.TableName), http.StatusBadRequest)
				return
			}
			
			pkAV, _ := cc.Key[schema.PartitionKey]
			pkVal, _ := model.GetAttributeValueString(pkAV)

			var skVal string
			if schema.SortKey != "" {
				skAV, _ := cc.Key[schema.SortKey]
				skVal, _ = model.GetAttributeValueString(skAV)
			}

			levelDBKey := model.BuildLevelDBKey(cc.TableName, pkVal, skVal)

			oldValue, err := s.Database.DB.Get([]byte(levelDBKey), nil)
			var recordForEvaluation model.Record
			
			if err != nil && err != leveldb.ErrNotFound {
				http.Error(w, "Internal DB error", http.StatusInternalServerError)
				return
			}

			if err == nil {
				recordForEvaluation, _ = model.UnmarshalRecord(oldValue)
			} else {
				recordForEvaluation = nil 
			}
			
			conditionInput := model.ConditionInput{
				ConditionExpression:       cc.ConditionExpression,
				ExpressionAttributeNames:  map[string]string{},
				ExpressionAttributeValues: cc.ExpressionAttributeValues,
			}
			
			ok, condErr := core.EvaluateConditionExpression(recordForEvaluation, conditionInput)
			
			if condErr != nil {
				s.writeDynamoDBError(w, "ValidationException", condErr.Error(), http.StatusBadRequest)
				return
			}
			if !ok {
				s.writeDynamoDBError(w, "ConditionCheckFailedException", "Transaction condition check failed.", http.StatusBadRequest)
				return
			}
		}
	}

	for _, item := range input.TransactItems {
		var tableName string
		var key model.Record
		var itemData model.Record
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

		schema, ok := s.Database.Tables[tableName]
		if !ok {
			s.writeDynamoDBError(w, "ResourceNotFoundException", fmt.Sprintf("Table %s not found", tableName), http.StatusBadRequest)
			return
		}

		pkAV, _ := key[schema.PartitionKey]
		if opType == "PUT" { pkAV = itemData[schema.PartitionKey] }

		pkVal, _ := model.GetAttributeValueString(pkAV)

		var skVal string
		if schema.SortKey != "" {
			skAV, _ := key[schema.SortKey]
			if opType == "PUT" { skAV = itemData[schema.SortKey] }
			skVal, _ = model.GetAttributeValueString(skAV)
		}

		levelDBKey := model.BuildLevelDBKey(tableName, pkVal, skVal)

		oldValue, err := s.Database.DB.Get([]byte(levelDBKey), nil)
		var oldRecord model.Record
		if err != leveldb.ErrNotFound && err != nil {
			http.Error(w, "Internal DB error", http.StatusInternalServerError)
			return
		}
		if err == nil {
			oldRecord, _ = model.UnmarshalRecord(oldValue)
		}


		if opType == "PUT" {
			core.UpdateGSI(batch, schema, oldRecord, itemData)

			value, _ := model.MarshalRecord(itemData)
			batch.Put([]byte(levelDBKey), value)
		} else if opType == "DELETE" {
			core.UpdateGSI(batch, schema, oldRecord, nil) 

			batch.Delete([]byte(levelDBKey))
		}
	}

	if err := s.Database.DB.Write(batch, nil); err != nil {
		s.writeDynamoDBError(w, "InternalServerError", "Internal DB error during transaction write.", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{}`))
}
