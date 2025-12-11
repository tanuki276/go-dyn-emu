package api

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
	"your_project/pkg/core"
)

type Server struct {
	DB *core.Database
}

func NewServer(db *core.Database) *Server {
	return &Server{DB: db}
}

func (s *Server) ListenAndServe(addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.dynamoDBHandler)

	start := time.Now()

	server := &http.Server{
		Addr: addr,
		Handler: mux,
	}

	go func() {
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			
		}
	}()
}

func (s *Server) dynamoDBHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		s.writeDynamoDBError(w, "MethodNotAllowed", "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	target := r.Header.Get("X-Amz-Target")
	if target == "" {
		s.writeDynamoDBError(w, "MissingTargetException", "X-Amz-Target header is missing", http.StatusBadRequest)
		return
	}
	
	parts := strings.Split(target, ".")
	operation := parts[len(parts)-1]

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/x-amz-json-1.0")

	switch operation {
	case "CreateTable":
		s.handleCreateTable(w, body)
	case "PutItem":
		s.handlePutItem(w, body)
	case "GetItem":
		s.handleGetItem(w, body)
	case "Query":
		s.handleQuery(w, body)
	case "TransactWriteItems":
		s.handleTransactWriteItems(w, body)
	case "UpdateItem":
		s.handleUpdateItem(w, body)
	default:
		s.writeDynamoDBError(w, "UnsupportedOperationException", fmt.Sprintf("Operation %s is not supported by the emulator.", operation), http.StatusBadRequest)
	}
}
