
package main

import (
    "log"
    "os"

    "go-dyn-emu/pkg/api"
    "go-dyn-emu/pkg/core"
)

func main() {
    log.SetFlags(log.LstdFlags | log.Lshortfile)

    db, err := core.NewDatabase()
    if err != nil {
        log.Fatalf("Failed to initialize database: %v", err)
    }
    defer db.Close()

    server := api.NewServer(db)

    addr := ":8000"
    if p := os.Getenv("PORT"); p != "" {
        addr = ":" + p
    }

    server.Start(addr)
}