package main

import (
	"os"
	"os/signal"
	"syscall"
	"fmt"
)

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "snapshot":
			if len(os.Args) < 4 {
				fmt.Println("Usage: go run . snapshot [create|load] <name>")
				os.Exit(1)
			}
			
			db, err := NewDatabase()
			if err != nil {
				os.Exit(1)
			}

			subcommand := os.Args[2]
			name := os.Args[3]

			if subcommand == "create" {
				if err := db.CreateSnapshot(name); err != nil {
					os.Exit(1)
				}
			} else if subcommand == "load" {
				if err := db.LoadSnapshot(name); err != nil {
					os.Exit(1)
				}
			} else {
				fmt.Println("Invalid snapshot subcommand. Use 'create' or 'load'.")
				os.Exit(1)
			}
			os.Exit(0)
		}
	}

	db, err := NewDatabase()
	if err != nil {
		os.Exit(1)
	}
	defer db.Close()

	server := NewServer(db)
	server.ListenAndServe(":8000")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
}
