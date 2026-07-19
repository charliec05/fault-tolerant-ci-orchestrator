package main

import (
	"flag"
	"log"
	"net/http"
	"time"

	"github.com/charliec05/fault-tolerant-ci-orchestrator/internal/httpapi"
	"github.com/charliec05/fault-tolerant-ci-orchestrator/internal/orchestrator"
)

func main() {
	address := flag.String("address", ":8090", "HTTP listen address")
	stateFile := flag.String("state-file", "data/coordinator.json", "durable coordinator state")
	leaseDuration := flag.Duration("lease-duration", 10*time.Second, "worker task lease")
	flag.Parse()

	coordinator, err := orchestrator.New(*stateFile, *leaseDuration, nil)
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		ticker := time.NewTicker(*leaseDuration / 2)
		defer ticker.Stop()
		for range ticker.C {
			if _, err := coordinator.RequeueExpired(); err != nil {
				log.Printf("requeue expired leases: %v", err)
			}
		}
	}()
	server := &http.Server{
		Addr:              *address,
		Handler:           httpapi.New(coordinator),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	log.Printf("coordinator listening on %s", *address)
	log.Fatal(server.ListenAndServe())
}
