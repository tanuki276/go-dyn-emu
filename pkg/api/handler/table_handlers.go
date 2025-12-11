package handler

import (
	"encoding/json"
	"fmt"
	"net/http"

	"Emulator-fr-virtuelle-Datenbanken-gobes/pkg/model"
)

type CreateTableInput struct {
	TableName string `json:"TableName"`
	KeySchema []struct {
		AttributeName string `json:"AttributeName"`
		KeyType string `json:"KeyType"` 
	} `json:"KeySchema"`
	AttributeDefinitions []struct {
		AttributeName string `json:"AttributeName"`
		AttributeType string `json:"AttributeType"` 
	} `json:"AttributeDefinitions"`
	GlobalSecondaryIndexes []struct {
		IndexName string `json:"IndexName"`
		KeySchema []struct {
			AttributeName string `json:"AttributeName"`
			KeyType string `json:"KeyType"`
		} `json:"KeySchema"`
	} `json:"GlobalSecondaryIndexes,omitempty"`
	ProvisionedThroughput struct{} `json:"ProvisionedThroughput"`
}

func (s *Server) handleCreateTable(w http.ResponseWriter, body []byte) {
	var input CreateTableInput
	if err := json.Unmarshal(body, &input); err != nil {
		s.writeDynamoDBError(w, "ValidationException", "Invalid JSON input", http.StatusBadRequest)
		return
	}

	schema := model.TableSchema{
		TableName: input.TableName,
		GSIs: make(map[string]model.GsiSchema),
	}

	for _, ks := range input.KeySchema {
		if ks.KeyType == "HASH" {
			schema.PartitionKey = ks.AttributeName
		} else if ks.KeyType == "RANGE" {
			schema.SortKey = ks.AttributeName
		}
	}
	
	if schema.PartitionKey == "" {
		s.writeDynamoDBError(w, "ValidationException", "Partition Key definition missing.", http.StatusBadRequest)
		return
	}

	for _, gsiInput := range input.GlobalSecondaryIndexes {
		gsiSchema := model.GsiSchema{
			IndexName: gsiInput.IndexName,
		}
		for _, ks := range gsiInput.KeySchema {
			if ks.KeyType == "HASH" {
				gsiSchema.PartitionKey = ks.AttributeName
			} else if ks.KeyType == "RANGE" {
				gsiSchema.SortKey = ks.AttributeName
			}
		}
		schema.GSIs[gsiInput.IndexName] = gsiSchema
	}

	if err := s.Database.CreateTable(schema); err != nil {
		s.writeDynamoDBError(w, "ResourceInUseException", err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf(`{"TableDescription": {"TableName": "%s", "TableStatus": "ACTIVE"}}`, input.TableName)))
}

type DeleteTableInput struct {
	TableName string `json:"TableName"`
}

func (s *Server) handleDeleteTable(w http.ResponseWriter, body []byte) {
	var input DeleteTableInput
	if err := json.Unmarshal(body, &input); err != nil {
		s.writeDynamoDBError(w, "ValidationException", "Invalid JSON input", http.StatusBadRequest)
		return
	}

	s.Database.Lock()
	defer s.Database.Unlock()
	
	if _, ok := s.Database.Tables[input.TableName]; !ok {
		s.writeDynamoDBError(w, "ResourceNotFoundException", "Table not found", http.StatusBadRequest)
		return
	}
	
	delete(s.Database.Tables, input.TableName)
	
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf(`{"TableDescription": {"TableName": "%s", "TableStatus": "DELETING"}}`, input.TableName)))
}

func (s *Server) handleListTables(w http.ResponseWriter) {
	s.Database.RLock()
	defer s.Database.RUnlock()

	tableNames := make([]string, 0, len(s.Database.Tables))
	for name := range s.Database.Tables {
		tableNames = append(tableNames, name)
	}

	response := struct {
		TableNames []string `json:"TableNames"`
	}{
		TableNames: tableNames,
	}

	respBody, _ := json.Marshal(response)
	w.WriteHeader(http.StatusOK)
	w.Write(respBody)
}

type SnapshotInput struct {
	SnapshotName string `json:"SnapshotName"`
}

func (s *Server) handleCreateSnapshot(w http.ResponseWriter, body []byte) {
	var input SnapshotInput
	if err := json.Unmarshal(body, &input); err != nil {
		s.writeDynamoDBError(w, "ValidationException", "Invalid JSON input", http.StatusBadRequest)
		return
	}

	if err := s.Database.CreateSnapshot(input.SnapshotName); err != nil {
		s.writeDynamoDBError(w, "InternalServerError", fmt.Sprintf("Failed to create snapshot: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf(`{"SnapshotName": "%s", "Status": "COMPLETED"}`, input.SnapshotName)))
}

func (s *Server) handleLoadSnapshot(w http.ResponseWriter, body []byte) {
	var input SnapshotInput
	if err := json.Unmarshal(body, &input); err != nil {
		s.writeDynamoDBError(w, "ValidationException", "Invalid JSON input", http.StatusBadRequest)
		return
	}

	if err := s.Database.LoadSnapshot(input.SnapshotName); err != nil {
		s.writeDynamoDBError(w, "ResourceNotFoundException", fmt.Sprintf("Failed to load snapshot: %v", err), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf(`{"SnapshotName": "%s", "Status": "LOADED"}`, input.SnapshotName)))
}

func (s *Server) handleDeleteAllData(w http.ResponseWriter) {
	if err := s.Database.DeleteAllData(); err != nil {
		s.writeDynamoDBError(w, "InternalServerError", fmt.Sprintf("Failed to delete all data: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"Status": "DELETED"}`))
}
