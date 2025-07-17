package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"vm-cluster/node"

	_ "vm-cluster/node"
)

func main() {

	go node.StartVMRouting()

	nodeManager := node.NewNodeManager(node.VMConfig)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	log.Println("Starting VictoriaMetrics cluster...")
	if err := nodeManager.StartAll(ctx); err != nil {
		log.Fatalf("Failed to start cluster: %v", err)
	}

	log.Println("VictoriaMetrics cluster started successfully")
	log.Printf("Running nodes: %v", nodeManager.GetRunningNodes())
	log.Println("Press Ctrl+C to stop...")

	<-sigChan
	log.Println("Received shutdown signal, stopping cluster...")

	cancel()

	if err := nodeManager.StopAll(); err != nil {
		log.Printf("Error stopping cluster: %v", err)
	} else {
		log.Println("VictoriaMetrics cluster stopped successfully")
	}
}
