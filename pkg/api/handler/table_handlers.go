// pkg/api/handler/table_handlers.go
package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
    "Emulator-fr-virtuelle-Datenbanken-gobes/pkg/core"
    "Emulator-fr-virtuelle-Datenbanken-gobes/pkg/model"
)

type Server struct {
    Database *core.Database
}

func (s *Server) writeDynamoDBError(w http.ResponseWriter, errorType string, message string, status int) {
	w.WriteHeader(status)
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	fmt.Fprintf(w, `{"__type": "com.amazon.coral.service#%s", "message": "%s"}`, errorType, message)
}

type CreateTableInput struct {
	TableName string `json:"TableName"`
	KeySchema []struct {
		AttributeName string `json:"AttributeName"`
		KeyType string `json:"KeyType"`
	} `json:"KeySchema"`
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

	schema := model.TableSchema{TableName: input.TableName, GSIs: make(map[string]model.GsiSchema)}
	for _, k := range input.KeySchema {
		if k.KeyType == "HASH" {
			schema.PartitionKey = k.AttributeName
		} else if k.KeyType == "RANGE" {
			schema.SortKey = k.AttributeName
		}
	}

	s.Database.Lock()
	defer s.Database.Unlock()

	if _, exists := s.Database.Tables[input.TableName]; exists {
		s.writeDynamoDBError(w, "ResourceInUseException", "Table already exists", http.StatusBadRequest)
		return
	}

	s.Database.Tables[input.TableName] = schema

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf(`{"TableDescription": {"TableName": "%s", "TableStatus": "ACTIVE"}}`, input.TableName)))
}

type DescribeTableInput struct {
    TableName string `json:"TableName"`
}

func (s *Server) handleDescribeTable(w http.ResponseWriter, body []byte) {
    var input DescribeTableInput
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

    keySchema := []map[string]string{
        {"AttributeName": schema.PartitionKey, "KeyType": "HASH"},
    }
    if schema.SortKey != "" {
        keySchema = append(keySchema, map[string]string{"AttributeName": schema.SortKey, "KeyType": "RANGE"})
    }

    response := map[string]interface{}{
        "TableDescription": map[string]interface{}{
            "TableName": input.TableName,
            "TableStatus": "ACTIVE",
            "KeySchema": keySchema,
            "ItemCount": 0,
        },
    }

    responseBody, _ := json.Marshal(response)
    w.WriteHeader(http.StatusOK)
    w.Write(responseBody)
}

type ListTablesOutput struct {
    TableNames []string `json:"TableNames"`
}

func (s *Server) handleListTables(w http.ResponseWriter, body []byte) {
    s.Database.RLock()
    defer s.Database.RUnlock()

    tableNames := make([]string, 0, len(s.Database.Tables))
    for name := range s.Database.Tables {
        tableNames = append(tableNames, name)
    }

    output := ListTablesOutput{TableNames: tableNames}
    responseBody, _ := json.Marshal(output)

    w.WriteHeader(http.StatusOK)
    w.Write(responseBody)
}
