package node

import (
	"context"
	"fmt"
	"log"
	"sync"
)

// VMNode interface defines the methods that a VM node should implement
// This includes starting and stopping the node, getting its name, and checking if it is running
// The context parameter in Start allows for cancellation and timeout handling
// The IsRunning method checks if the node is currently active
type VMNode interface {
	Start(ctx context.Context) error
	Stop() error
	GetName() string
	IsRunning() bool
}

// NodeManager manages a collection of VM nodes
// It provides methods to add, remove, and retrieve nodes
// The manager uses a mutex to ensure thread-safe operations on the node list
type NodeManager struct {
	nodes []VMNode
	mu    sync.RWMutex
}

func NewNodeManager(config *Config) *NodeManager {
	nm := &NodeManager{}

	// 添加vmstorage节点
	for _, cfg := range config.VMStorage {
		if cfg.Enable {
			nm.nodes = append(nm.nodes, NewVMStorageNode(cfg))
		}
	}

	// 添加vminsert节点
	for _, cfg := range config.VMInsert {
		if cfg.Enable {
			nm.nodes = append(nm.nodes, NewVMInsertNode(cfg))
		}
	}

	// 添加vmselect节点
	for _, cfg := range config.VMSelect {
		if cfg.Enable {
			nm.nodes = append(nm.nodes, NewVMSelectNode(cfg))
		}
	}

	return nm
}

func (nm *NodeManager) StartAll(ctx context.Context) error {
	log.Println("Starting all nodes...")

	var runningNodes []string

	log.Println("Phase 1: Starting vmstorage nodes...")
	for _, node := range nm.nodes {
		if storageNode, ok := node.(*vmStorageNode); ok && storageNode.config.Enable {
			log.Printf("Starting vmstorage node: %s", node.GetName())
			if err := node.Start(ctx); err != nil {
				return fmt.Errorf("failed to start vmstorage node %s: %v", node.GetName(), err)
			}
			runningNodes = append(runningNodes, node.GetName())
		}
	}

	if len(runningNodes) > 0 {
		log.Printf("Waiting for %d vmstorage nodes to be fully ready...", len(runningNodes))

		for _, node := range nm.nodes {
			if storageNode, ok := node.(*vmStorageNode); ok && storageNode.config.Enable {
				if !node.IsRunning() {
					return fmt.Errorf("vmstorage node %s failed to start properly", node.GetName())
				}
				log.Printf("✓ vmstorage node %s is running", node.GetName())
			}
		}
	}

	log.Println("Phase 2: Starting vminsert and vmselect nodes...")
	for _, node := range nm.nodes {
		if _, ok := node.(*vmStorageNode); ok {
			continue
		}

		if insertNode, ok := node.(*vmInsertNode); ok && insertNode.config.Enable {
			log.Printf("Starting vminsert node: %s", node.GetName())
			if err := node.Start(ctx); err != nil {
				return fmt.Errorf("failed to start vminsert node %s: %v", node.GetName(), err)
			}
			runningNodes = append(runningNodes, node.GetName())
		}

		if selectNode, ok := node.(*vmSelectNode); ok && selectNode.config.Enable {
			log.Printf("Starting vmselect node: %s", node.GetName())
			if err := node.Start(ctx); err != nil {
				return fmt.Errorf("failed to start vmselect node %s: %v", node.GetName(), err)
			}
			runningNodes = append(runningNodes, node.GetName())
		}
	}

	log.Printf("All nodes started successfully. Running nodes: %v", runningNodes)

	log.Println("\n=== Access Information ===")
	for _, node := range nm.nodes {
		if selectNode, ok := node.(*vmSelectNode); ok && selectNode.config.Enable {
			log.Printf("Web UI: http://%s/select/0/vmui/", selectNode.config.HTTPListenAddr)
			log.Printf("Prometheus API: http://%s/select/0/prometheus/", selectNode.config.HTTPListenAddr)
		}
		if insertNode, ok := node.(*vmInsertNode); ok && insertNode.config.Enable {
			log.Printf("Data Ingestion: http://%s/insert/0/prometheus/api/v1/write", insertNode.config.HTTPListenAddr)
		}
	}
	fmt.Println("===========================")

	return nil
}

func (nm *NodeManager) StopAll() error {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	log.Println("Stopping all nodes in proper order...")

	log.Println("Phase 1: Stopping vminsert nodes...")
	var wg sync.WaitGroup
	for _, node := range nm.nodes {
		if insertNode, ok := node.(*vmInsertNode); ok && insertNode.config.Enable && node.IsRunning() {
			wg.Add(1)
			go func(n VMNode) {
				defer wg.Done()
				log.Printf("Stopping vminsert node: %s", n.GetName())
				if err := n.Stop(); err != nil {
					log.Printf("Error stopping vminsert %s: %v", n.GetName(), err)
				} else {
					log.Printf("✓ vminsert node %s stopped successfully", n.GetName())
				}
			}(node)
		}
	}
	wg.Wait()

	log.Println("Phase 2: Stopping vmselect nodes...")
	for _, node := range nm.nodes {
		if selectNode, ok := node.(*vmSelectNode); ok && selectNode.config.Enable && node.IsRunning() {
			wg.Add(1)
			go func(n VMNode) {
				defer wg.Done()
				log.Printf("Stopping vmselect node: %s", n.GetName())
				if err := n.Stop(); err != nil {
					log.Printf("Error stopping vmselect %s: %v", n.GetName(), err)
				} else {
					log.Printf("✓ vmselect node %s stopped successfully", n.GetName())
				}
			}(node)
		}
	}
	wg.Wait()

	log.Println("Phase 3: Stopping vmstorage nodes...")
	for _, node := range nm.nodes {
		if storageNode, ok := node.(*vmStorageNode); ok && storageNode.config.Enable && node.IsRunning() {
			wg.Add(1)
			go func(n VMNode) {
				defer wg.Done()
				log.Printf("Stopping vmstorage node: %s", n.GetName())
				if err := n.Stop(); err != nil {
					log.Printf("Error stopping vmstorage %s: %v", n.GetName(), err)
				} else {
					log.Printf("✓ vmstorage node %s stopped successfully", n.GetName())
				}
			}(node)
		}
	}
	wg.Wait()

	log.Println("All nodes stopped successfully")
	return nil
}

func (nm *NodeManager) GetRunningNodes() []string {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	var running []string
	for _, node := range nm.nodes {
		if node.IsRunning() {
			running = append(running, node.GetName())
		}
	}
	return running
}
