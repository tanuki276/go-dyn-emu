package main

import (
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

type Server struct {
	DB *Database
}

func NewServer(db *Database) *Server {
	return &Server{DB: db}
}

func (s *Server) ListenAndServe(addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.dynamoDBHandler)

	log.Printf("🚀 Starting DynamoDB emulator on %s...", addr)
	start := time.Now()

	server := &http.Server{
		Addr: addr,
		Handler: mux,
	}

	go func() {
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	log.Printf("✅ Startup complete! (Time taken: %s)", time.Since(start))
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
		log.Printf("Failed to read request body: %v", err)
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
	default:
		log.Printf("Unsupported operation: %s", operation)
		s.writeDynamoDBError(w, "UnsupportedOperationException", fmt.Sprintf("Operation %s is not supported by the emulator.", operation), http.StatusBadRequest)
	}
}