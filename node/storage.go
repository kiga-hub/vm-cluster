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

// VMStorage节点实现
type vmStorageNode struct {
	config VMStorageConfig
	cmd    *exec.Cmd
	ctx    context.Context
	cancel context.CancelFunc
}

func NewVMStorageNode(config VMStorageConfig) *vmStorageNode {
	return &vmStorageNode{
		config: config,
	}
}

func (v *vmStorageNode) Start(ctx context.Context) error {
	// 创建数据目录
	if err := os.MkdirAll(v.config.StorageDataPath, 0755); err != nil {
		return fmt.Errorf("failed to create storage directory: %v", err)
	}

	v.ctx, v.cancel = context.WithCancel(ctx)

	execPath := filepath.Join("bin", "vmstorage-prod-linux-amd64")

	/*
		-storageDataPath：存储数据的路径，默认 victoria-metrics-data
		-storage.minFreeDiskSpaceBytes：数据目录最小剩余磁盘空间，低于此值拒绝新数据，默认 10MB
		-precisionBits：每个值存储的精度位数，默认 64
		-snapshotsMaxAge：自动删除早于指定时间的快照（如 3d）
		2. 保留策略相关
		-retentionPeriod：数据保留时长
		-retentionTimezoneOffset：索引数据库轮换的时区偏移量
		3. 端口与监听相关
		-httpListenAddr：HTTP 服务监听地址，默认 :8482
		-grpcListenAddr：gRPC 服务监听地址，默认 :8400
		4. 性能与缓存
		-storage.cacheSizeStorageTSID：storage/tsid 缓存最大大小
		-storage.cacheSizeIndexDB：indexdb 缓存最大大小
		5. 限流与基数控制
		-storage.maxHourlySeries：每小时最大新时序数
		-storage.maxDailySeries：每天最大新时序数
		6. 调试与安全
		-logNewSeries：是否记录新时序（调试用）
		-denyQueriesOutsideRetention：是否拒绝超出保留周期的查询
		-forceFlushAuthKey：用于 /internal/force_flush 页面的 authKey
		-httpAuth.username / -httpAuth.password：HTTP 基本认证
		7. 其他
		-envflag.enable：是否支持从环境变量加载参数
		-envflag.prefix：环境变量前缀
		-loggerLevel：日志级别
		-loggerFormat：日志格式
		-version：显示版本信息
	*/
	args := []string{
		fmt.Sprintf("-storageDataPath=%s", v.config.StorageDataPath),
		fmt.Sprintf("-retentionPeriod=%s", v.config.RetentionPeriod),
		fmt.Sprintf("-precisionBits=%d", v.config.PrecisionBits),
		fmt.Sprintf("-storage.minFreeDiskSpaceBytes=%s", v.config.MinFreeDiskSpaceBytes),
		fmt.Sprintf("-httpListenAddr=%s", v.config.HTTPListenAddr),
		fmt.Sprintf("-vminsertAddr=%s", v.config.VMInsertAddr),
		fmt.Sprintf("-vmselectAddr=%s", v.config.VMSelectAddr),
	}

	v.cmd = exec.CommandContext(v.ctx, execPath, args...)
	v.cmd.Stdout = os.Stdout
	v.cmd.Stderr = os.Stderr

	log.Printf("Starting VMStorage node on httpListenAddr: %s", v.config.HTTPListenAddr)
	log.Printf("Command: %s %v", execPath, args)

	if err := v.cmd.Start(); err != nil {
		return fmt.Errorf("failed to start vmstorage %s: %v", v.config.Name, err)
	}

	// 等待一段时间让 vmstorage 完全启动
	time.Sleep(2 * time.Second)

	log.Printf("VMStorage node %s started successfully", v.config.Name)
	return nil
}

func (v *vmStorageNode) Stop() error {
	if v.cancel != nil {
		v.cancel()
	}

	if v.cmd != nil && v.cmd.Process != nil {
		log.Printf("Stopping VMStorage node: %s", v.config.Name)

		if err := v.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			log.Printf("Failed to send SIGTERM to %s: %v", v.config.Name, err)
			return v.cmd.Process.Kill()
		}

		return v.cmd.Wait()
	}
	return nil
}

func (v *vmStorageNode) GetName() string {
	return v.config.Name
}

func (v *vmStorageNode) IsRunning() bool {
	return v.cmd != nil && v.cmd.Process != nil
}
