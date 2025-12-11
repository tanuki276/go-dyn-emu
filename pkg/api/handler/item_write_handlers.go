package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
    
	"github.com/syndtr/goleveldb/leveldb"
	"Emulator-fr-virtuelle-Datenbanken-gobes/pkg/core"
	"Emulator-fr-virtuelle-Datenbanken-gobes/pkg/model"
)

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
