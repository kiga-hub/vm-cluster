package node

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"
)

type vmSelectNode struct {
	config VMSelectConfig
	cmd    *exec.Cmd
	ctx    context.Context
	cancel context.CancelFunc
}

func NewVMSelectNode(config VMSelectConfig) *vmSelectNode {
	return &vmSelectNode{
		config: config,
	}
}

func (v *vmSelectNode) Start(ctx context.Context) error {
	v.ctx, v.cancel = context.WithCancel(ctx)

	execPath := filepath.Join("bin", "vmselect-prod-linux-amd64")

	storageNodesStr := ""
	for i, node := range v.config.StorageNodes {
		if i > 0 {
			storageNodesStr += ","
		}
		storageNodesStr += node
	}

	/*
		-storageNode：指定 vmstorage 节点的地址（必选，可以多个）。
		-httpListenAddr：HTTP 服务监听地址（比如 :8481，默认 0.0.0.0:8481）。
		-vmalert.proxyURL：配置 vmalert 反向代理的 URL。
		-maxConcurrentQueries：最大并发查询数。
		-maxQueryDuration：单个查询最大执行时长。
		-cacheDataPath：查询结果缓存路径。
		-disableCache：是否禁用查询缓存。
		-logNewSeries：是否记录新时序。
		-auth.*：HTTP API 访问认证相关参数。
	*/
	args := []string{
		fmt.Sprintf("-storageNode=%s", storageNodesStr),
		fmt.Sprintf("-httpListenAddr=%s", v.config.HTTPListenAddr),
		fmt.Sprintf("-search.maxConcurrentRequests=%d", v.config.MaxConcurrentRequests),
		fmt.Sprintf("-search.maxQueryLen=%d", v.config.MaxQueryLen),
		fmt.Sprintf("-search.maxSamplesPerQuery=%d", v.config.MaxSamplesPerQuery),
		fmt.Sprintf("-search.maxQueryDuration=%s", v.config.SearchMaxQueryDuration),
	}

	v.cmd = exec.CommandContext(v.ctx, execPath, args...)
	v.cmd.Stdout = os.Stdout
	v.cmd.Stderr = os.Stderr

	log.Printf("Starting VMSelect node: %s", v.config.Name)
	log.Printf("Command: %s %v", execPath, args)

	if err := v.cmd.Start(); err != nil {
		return fmt.Errorf("failed to start vmselect %s: %v", v.config.Name, err)
	}

	time.Sleep(5 * time.Second)

	log.Printf("VMSelect node %s started successfully on %s", v.config.Name, v.config.HTTPListenAddr)
	return nil
}

func (v *vmSelectNode) Stop() error {
	if v.cancel != nil {
		v.cancel()
	}

	if v.cmd != nil && v.cmd.Process != nil {
		log.Printf("Stopping VMSelect node: %s", v.config.Name)

		if err := v.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			log.Printf("Failed to send SIGTERM to %s: %v", v.config.Name, err)
			return v.cmd.Process.Kill()
		}

		return v.cmd.Wait()
	}
	return nil
}

func (v *vmSelectNode) GetName() string {
	return v.config.Name
}

func (v *vmSelectNode) IsRunning() bool {
	return v.cmd != nil && v.cmd.Process != nil
}
