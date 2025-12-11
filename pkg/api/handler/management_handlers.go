package handler

import ( "encoding/json" "fmt" "net/http" )

type CreateSnapshotInput struct { SnapshotName string json:"SnapshotName" }

func (s *Server) handleCreateSnapshot(w http.ResponseWriter, body []byte) { var input CreateSnapshotInput if err := json.Unmarshal(body, &input); err != nil { s.writeDynamoDBError(w, "ValidationException", "Invalid JSON input", http.StatusBadRequest) return }

if input.SnapshotName == "" {
	s.writeDynamoDBError(w, "ValidationException", "SnapshotName cannot be empty", http.StatusBadRequest)
	return
}

if err := s.Database.CreateSnapshot(input.SnapshotName); err != nil {
	s.writeDynamoDBError(w, "InternalServerError", fmt.Sprintf("Failed to create snapshot: %v", err), http.StatusInternalServerError)
	return
}

response := map[string]string{
	"Message": fmt.Sprintf("Snapshot '%s' created successfully.", input.SnapshotName),
}
responseBody, _ := json.Marshal(response)

w.WriteHeader(http.StatusOK)
w.Write(responseBody)

}

type LoadSnapshotInput struct { SnapshotName string json:"SnapshotName" }

func (s *Server) handleLoadSnapshot(w http.ResponseWriter, body []byte) { var input LoadSnapshotInput if err := json.Unmarshal(body, &input); err != nil { s.writeDynamoDBError(w, "ValidationException", "Invalid JSON input", http.StatusBadRequest) return }

if input.SnapshotName == "" {
	s.writeDynamoDBError(w, "ValidationException", "SnapshotName cannot be empty", http.StatusBadRequest)
	return
}

if err := s.Database.LoadSnapshot(input.SnapshotName); err != nil {
	s.writeDynamoDBError(w, "InternalServerError", fmt.Sprintf("Failed to load snapshot: %v", err), http.StatusInternalServerError)
	return
}

s.Database.Lock()
s.Database.Tables = make(map[string]model.TableSchema)
s.Database.Unlock()

response := map[string]string{
	"Message": fmt.Sprintf("Snapshot '%s' loaded successfully. DB connection reopened.", input.SnapshotName),
}
responseBody, _ := json.Marshal(response)

w.WriteHeader(http.StatusOK)
w.Write(responseBody)

}