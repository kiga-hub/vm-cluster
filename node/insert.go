package node

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
)

type vmInsertNode struct {
	config VMInsertConfig
	cmd    *exec.Cmd
	ctx    context.Context
	cancel context.CancelFunc
}

func NewVMInsertNode(config VMInsertConfig) *vmInsertNode {
	return &vmInsertNode{
		config: config,
	}
}
func (v *vmInsertNode) Start(ctx context.Context) error {
	v.ctx, v.cancel = context.WithCancel(ctx)

	execPath := filepath.Join("bin", "vminsert-prod-linux-amd64")

	storageNodesStr := ""
	for i, node := range v.config.StorageNodes {
		if i > 0 {
			storageNodesStr += ","
		}
		storageNodesStr += node
	}

	/*
		-graphiteListenAddr.useProxyProtocol：Graphite 监听端口是否启用 Proxy Protocol。
		-influxListenAddr：监听 InfluxDB Line Protocol 的 TCP/UDP 地址，如 :8089。
		-influxListenAddr.useProxyProtocol：InfluxDB 端口是否启用 Proxy Protocol。
		-opentsdbListenAddr：监听 OpenTSDB 协议的 TCP/UDP 地址，如 :4242。
		-opentsdbListenAddr.useProxyProtocol：OpenTSDB 端口是否启用 Proxy Protocol。
		-opentsdbHTTPListenAddr：监听 OpenTSDB HTTP put 请求的地址。
		-opentsdbHTTPListenAddr.useProxyProtocol：OpenTSDB HTTP 端口是否启用 Proxy Protocol。
		-configAuthKey：访问 /config 页面所需的认证 key。
		-reloadAuthKey：访问 /-/reload HTTP 接口所需的认证 key。
		-maxLabelsPerTimeseries：每个时序最大标签数量，默认 40。
		-maxLabelNameLen：标签名最大长度，默认 256。
		-maxLabelValueLen：标签值最大长度，默认 4096。
	*/
	args := []string{
		fmt.Sprintf("-storageNode=%s", storageNodesStr),
		fmt.Sprintf("-promscrape.httpListenAddr=%s", v.config.HTTPListenAddr),
		fmt.Sprintf("-replicationFactor=%d", v.config.ReplicationFactor),
		fmt.Sprintf("-maxLabelsPerTimeseries=%d", v.config.MaxLabelsPerTimeseries),
		fmt.Sprintf("-maxLabelNameLen=%d", v.config.MaxLabelNameLen),
		fmt.Sprintf("-maxLabelValueLen=%d", v.config.MaxLabelValueLen),
	}

	v.cmd = exec.CommandContext(v.ctx, execPath, args...)
	v.cmd.Stdout = os.Stdout
	v.cmd.Stderr = os.Stderr

	log.Printf("Starting VMInsert node: %s", v.config.Name)
	log.Printf("Command: %s %v", execPath, args)

	if err := v.cmd.Start(); err != nil {
		return fmt.Errorf("failed to start vminsert %s: %v", v.config.Name, err)
	}

	log.Printf("VMInsert node %s started successfully on %s", v.config.Name, v.config.HTTPListenAddr)
	return nil
}

func (v *vmInsertNode) Stop() error {
	if v.cancel != nil {
		v.cancel()
	}

	if v.cmd != nil && v.cmd.Process != nil {
		log.Printf("Stopping VMInsert node: %s", v.config.Name)

		if err := v.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			log.Printf("Failed to send SIGTERM to %s: %v", v.config.Name, err)
			return v.cmd.Process.Kill()
		}

		return v.cmd.Wait()
	}
	return nil
}

func (v *vmInsertNode) GetName() string {
	return v.config.Name
}

func (v *vmInsertNode) IsRunning() bool {
	return v.cmd != nil && v.cmd.Process != nil
}
