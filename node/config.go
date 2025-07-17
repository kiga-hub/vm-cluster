package node

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/BurntSushi/toml"
)

var VMConfig *Config

// Config vm configuration
// This struct defines the configuration for the VM node, including storage, insert, and select configurations
type Config struct {
	VMStorage []VMStorageConfig `toml:"vmstorage"`
	VMInsert  []VMInsertConfig  `toml:"vminsert"`
	VMSelect  []VMSelectConfig  `toml:"vmselect"`
}

type DiskSize int64

func (d *DiskSize) UnmarshalText(text []byte) error {
	value := strings.TrimSpace(string(text))

	if num, err := strconv.ParseFloat(value, 64); err == nil {
		*d = DiskSize(int64(num))
		return nil
	}

	units := map[string]int64{
		"B":  1,
		"KB": 1024,
		"MB": 1024 * 1024,
		"GB": 1024 * 1024 * 1024,
		"TB": 1024 * 1024 * 1024 * 1024,
	}

	for unit, multiplier := range units {
		if strings.HasSuffix(strings.ToUpper(value), unit) {
			numStr := strings.TrimSuffix(strings.ToUpper(value), unit)
			num, err := strconv.ParseFloat(numStr, 64)
			if err != nil {
				return fmt.Errorf("invalid number format: %s", numStr)
			}
			*d = DiskSize(int64(num * float64(multiplier)))
			return nil
		}
	}

	if num, err := strconv.ParseInt(value, 10, 64); err == nil {
		*d = DiskSize(num)
		return nil
	}

	return fmt.Errorf("invalid disk size format: %s", value)
}

// Int64 returns the int64 value of DiskSize
func (d DiskSize) Int64() int64 {
	return int64(d)
}

// String returns a human-readable string representation
func (d DiskSize) String() string {
	size := int64(d)

	if size >= 1024*1024*1024*1024 {
		return fmt.Sprintf("%.2fTB", float64(size)/(1024*1024*1024*1024))
	} else if size >= 1024*1024*1024 {
		return fmt.Sprintf("%.2fGB", float64(size)/(1024*1024*1024))
	} else if size >= 1024*1024 {
		return fmt.Sprintf("%.2fMB", float64(size)/(1024*1024))
	} else if size >= 1024 {
		return fmt.Sprintf("%.2fKB", float64(size)/1024)
	} else {
		return fmt.Sprintf("%dB", size)
	}
}

// VMStorageConfig defines the configuration for the VM storage node
type VMStorageConfig struct {
	Name                  string   `toml:"name"`
	StorageDataPath       string   `toml:"storageDataPath"`
	RetentionPeriod       string   `toml:"retentionPeriod"`
	PrecisionBits         int      `toml:"precisionBits"`
	MinFreeDiskSpaceBytes DiskSize `toml:"minFreeDiskSpaceBytes"`
	HTTPListenAddr        string   `toml:"httpListenAddr"`
	VMInsertAddr          string   `toml:"vminsertAddr"`
	VMSelectAddr          string   `toml:"vmselectAddr"`
	MaxIngestionRate      int      `toml:"maxIngestionRate"`
	Enable                bool     `toml:"enable"`
	Proxy                 bool     `toml:"proxy"` // Indicates if this node is a proxy or a storage node
}

// VMInsertConfig defines the configuration for the VM insert node
type VMInsertConfig struct {
	Name                         string   `toml:"name"`
	HTTPListenAddr               string   `toml:"httpListenAddr"`
	StorageNodes                 []string `toml:"storageNodes"`
	ReplicationFactor            int      `toml:"replicationFactor"`
	GraphiteListenAddr           string   `toml:"graphiteListenAddr"`
	GraphiteUseProxyProtocol     bool     `toml:"graphiteUseProxyProtocol"`
	InfluxListenAddr             string   `toml:"influxListenAddr"`
	InfluxUseProxyProtocol       bool     `toml:"influxUseProxyProtocol"`
	OpentsdbListenAddr           string   `toml:"opentsdbListenAddr"`
	OpentsdbUseProxyProtocol     bool     `toml:"opentsdbUseProxyProtocol"`
	OpentsdbHTTPListenAddr       string   `toml:"opentsdbHTTPListenAddr"`
	OpentsdbHTTPUseProxyProtocol bool     `toml:"opentsdbHTTPUseProxyProtocol"`
	ConfigAuthKey                string   `toml:"configAuthKey"`
	ReloadAuthKey                string   `toml:"reloadAuthKey"`
	MaxLabelsPerTimeseries       int      `toml:"maxLabelsPerTimeseries"`
	MaxLabelNameLen              int      `toml:"maxLabelNameLen"`
	MaxLabelValueLen             int      `toml:"maxLabelValueLen"`
	Enable                       bool     `toml:"enable"`
}

// VMSelectConfig defines the configuration for the VM select node
type VMSelectConfig struct {
	Name                   string   `toml:"name"`
	HTTPListenAddr         string   `toml:"httpListenAddr"`
	StorageNodes           []string `toml:"storageNodes"`
	MaxConcurrentRequests  int      `toml:"maxConcurrentRequests"`
	MaxQueryLen            int      `toml:"maxQueryLen"`
	MaxSamplesPerQuery     int      `toml:"maxSamplesPerQuery"`
	SelectTimeout          string   `toml:"selectTimeout"`
	SearchMaxQueryDuration string   `toml:"searchMaxQueryDuration"`
	DataPath               string   `toml:"dataPath"`
	Enable                 bool     `toml:"enable"`
}

// GetStorageConfig returns the storage configuration by name
func (c *Config) GetStorageConfig(name string) *VMStorageConfig {
	for _, storage := range c.VMStorage {
		if storage.Name == name {
			return &storage
		}
	}
	return nil
}

// GetInsertConfig returns the insert configuration by name
func (c *Config) GetInsertConfig(name string) *VMInsertConfig {
	for _, insert := range c.VMInsert {
		if insert.Name == name {
			return &insert
		}
	}
	return nil
}

// GetSelectConfig returns the select configuration by name
func (c *Config) GetSelectConfig(name string) *VMSelectConfig {
	for _, vmselect := range c.VMSelect {
		if vmselect.Name == name {
			return &vmselect
		}
	}
	return nil
}

// loadConfig loads the VM configuration from a TOML file - "./nodes.toml"
// It returns a Config struct or an error if the file cannot be decoded.
func loadConfig(filename string) (*Config, error) {
	var config Config

	if _, err := toml.DecodeFile(filename, &config); err != nil {
		return nil, fmt.Errorf("failed to decode config file %s: %v", filename, err)
	}

	return &config, nil
}

func init() {
	// Load the VM configuration from the TOML file
	config, err := loadConfig("./vm_cluster.toml")
	if err != nil {
		panic(fmt.Sprintf("Error loading VM configuration: %v", err))
	}

	VMConfig = config
}
