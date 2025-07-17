package node

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"gopkg.in/yaml.v2"
)

type RouterConfig struct {
	ListenAddr        string            `json:"listen_addr" yaml:"listen_addr"`
	DefaultStorage    string            `json:"default_storage" yaml:"default_storage"`
	StorageNodes      map[string]string `json:"storage_nodes" yaml:"storage_nodes"`
	RoutingRules      []RoutingRule     `json:"routing_rules" yaml:"routing_rules"`
	Timeout           string            `json:"timeout" yaml:"timeout"`
	MaxRetries        int               `json:"max_retries" yaml:"max_retries"`
	EnableCompression bool              `json:"enable_compression" yaml:"enable_compression"`
	LogLevel          string            `json:"log_level" yaml:"log_level"`
}

type RoutingRule struct {
	Name          string `json:"name" yaml:"name"`
	LabelName     string `json:"label_name" yaml:"label_name"`
	LabelValue    string `json:"label_value" yaml:"label_value"`
	LabelRegex    string `json:"label_regex,omitempty" yaml:"label_regex,omitempty"`
	MetricRegex   string `json:"metric_regex,omitempty" yaml:"metric_regex,omitempty"`
	StorageNode   string `json:"storage_node" yaml:"storage_node"`
	Priority      int    `json:"priority" yaml:"priority"`
	compiledRegex *regexp.Regexp
	metricRegex   *regexp.Regexp
}

type MetricRouter struct {
	config     *RouterConfig
	client     *http.Client
	rules      []RoutingRule
	mutex      sync.RWMutex
	stats      *RouterStats
	timeoutDur time.Duration
}

type RouterStats struct {
	TotalRequests    int64            `json:"total_requests"`
	SuccessfulRoutes int64            `json:"successful_routes"`
	FailedRoutes     int64            `json:"failed_routes"`
	StorageStats     map[string]int64 `json:"storage_stats"`
	LastError        string           `json:"last_error,omitempty"`
	LastErrorTime    time.Time        `json:"last_error_time,omitempty"`
	mutex            sync.RWMutex
}

type RouteResult struct {
	StorageNode string
	Metrics     []prompb.TimeSeries
	RuleName    string
}

func NewMetricRouter(config *RouterConfig) (*MetricRouter, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	timeout := 30 * time.Second
	if config.Timeout != "" {
		if dur, err := time.ParseDuration(config.Timeout); err == nil {
			timeout = dur
		}
	}

	router := &MetricRouter{
		config:     config,
		timeoutDur: timeout,
		client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		stats: &RouterStats{
			StorageStats: make(map[string]int64),
		},
	}

	if err := router.compileRules(); err != nil {
		return nil, fmt.Errorf("failed to compile routing rules: %v", err)
	}

	return router, nil
}

func (mr *MetricRouter) compileRules() error {
	mr.mutex.Lock()
	defer mr.mutex.Unlock()

	mr.rules = make([]RoutingRule, len(mr.config.RoutingRules))
	copy(mr.rules, mr.config.RoutingRules)

	sort.Slice(mr.rules, func(i, j int) bool {
		return mr.rules[i].Priority > mr.rules[j].Priority
	})

	for i := range mr.rules {
		if mr.rules[i].LabelRegex != "" {
			regex, err := regexp.Compile(mr.rules[i].LabelRegex)
			if err != nil {
				return fmt.Errorf("invalid label regex for rule %s: %v", mr.rules[i].Name, err)
			}
			mr.rules[i].compiledRegex = regex
		}

		if mr.rules[i].MetricRegex != "" {
			regex, err := regexp.Compile(mr.rules[i].MetricRegex)
			if err != nil {
				return fmt.Errorf("invalid metric regex for rule %s: %v", mr.rules[i].Name, err)
			}
			mr.rules[i].metricRegex = regex
		}
	}

	return nil
}

func (mr *MetricRouter) HandleRemoteWrite(w http.ResponseWriter, r *http.Request) {
	mr.updateStats(func(s *RouterStats) { s.TotalRequests++ })

	start := time.Now()
	defer func() {
		log.Printf("Request processed in %v", time.Since(start))
	}()

	writeRequest, err := mr.parseRemoteWriteRequest(r)
	if err != nil {
		mr.handleError(w, fmt.Sprintf("Failed to parse request: %v", err), http.StatusBadRequest)
		return
	}

	if len(writeRequest.Timeseries) == 0 {
		w.WriteHeader(http.StatusOK)
		return
	}

	routeResults := mr.routeMetrics(writeRequest.Timeseries)

	if err := mr.sendToStorageNodes(r.Context(), routeResults, r.Header); err != nil {
		mr.handleError(w, fmt.Sprintf("Failed to send metrics: %v", err), http.StatusInternalServerError)
		return
	}

	mr.updateStats(func(s *RouterStats) { s.SuccessfulRoutes++ })
	w.WriteHeader(http.StatusOK)

	log.Printf("Successfully routed %d timeseries to %d storage nodes",
		len(writeRequest.Timeseries), len(routeResults))
}

func (mr *MetricRouter) parseRemoteWriteRequest(r *http.Request) (*prompb.WriteRequest, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read request body: %v", err)
	}
	defer r.Body.Close()

	var data []byte
	switch r.Header.Get("Content-Encoding") {
	case "snappy":
		data, err = snappy.Decode(nil, body)
		if err != nil {
			return nil, fmt.Errorf("failed to decode snappy: %v", err)
		}
	case "gzip":
		reader, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("failed to create gzip reader: %v", err)
		}
		defer reader.Close()

		data, err = io.ReadAll(reader)
		if err != nil {
			return nil, fmt.Errorf("failed to read gzip data: %v", err)
		}
	default:
		data = body
	}

	var writeRequest prompb.WriteRequest
	if err := proto.Unmarshal(data, &writeRequest); err != nil {
		return nil, fmt.Errorf("failed to unmarshal protobuf: %v", err)
	}

	return &writeRequest, nil
}

func (mr *MetricRouter) routeMetrics(timeseries []prompb.TimeSeries) map[string]*RouteResult {
	mr.mutex.RLock()
	rules := mr.rules
	mr.mutex.RUnlock()

	results := make(map[string]*RouteResult)

	for _, ts := range timeseries {
		storageNode, ruleName := mr.findStorageNode(ts, rules)

		if results[storageNode] == nil {
			results[storageNode] = &RouteResult{
				StorageNode: storageNode,
				Metrics:     make([]prompb.TimeSeries, 0),
				RuleName:    ruleName,
			}
		}

		results[storageNode].Metrics = append(results[storageNode].Metrics, ts)
	}

	return results
}

func (mr *MetricRouter) findStorageNode(ts prompb.TimeSeries, rules []RoutingRule) (string, string) {
	metricName := mr.getMetricName(ts)
	labels := mr.getLabelsMap(ts)

	for _, rule := range rules {
		if mr.matchesRule(ts, metricName, labels, rule) {
			log.Printf("Metric %s matched rule %s, routing to %s",
				metricName, rule.Name, rule.StorageNode)
			return rule.StorageNode, rule.Name
		}
	}

	log.Printf("Metric %s using default storage %s", metricName, mr.config.DefaultStorage)
	return mr.config.DefaultStorage, "default"
}

func (mr *MetricRouter) matchesRule(ts prompb.TimeSeries, metricName string, labels map[string]string, rule RoutingRule) bool {
	if rule.metricRegex != nil && !rule.metricRegex.MatchString(metricName) {
		return false
	}

	if rule.LabelName != "" {
		labelValue, exists := labels[rule.LabelName]
		if !exists {
			return false
		}

		if rule.LabelValue != "" && labelValue != rule.LabelValue {
			return false
		}

		if rule.compiledRegex != nil && !rule.compiledRegex.MatchString(labelValue) {
			return false
		}
	}

	return true
}

func (mr *MetricRouter) getMetricName(ts prompb.TimeSeries) string {
	for _, label := range ts.Labels {
		if label.Name == "__name__" {
			return label.Value
		}
	}
	return "unknown"
}

func (mr *MetricRouter) getLabelsMap(ts prompb.TimeSeries) map[string]string {
	labels := make(map[string]string)
	for _, label := range ts.Labels {
		labels[label.Name] = label.Value
	}
	return labels
}

func (mr *MetricRouter) sendToStorageNodes(ctx context.Context, results map[string]*RouteResult, headers http.Header) error {
	if len(results) == 0 {
		return nil
	}

	errChan := make(chan error, len(results))
	var wg sync.WaitGroup

	for storageNode, result := range results {
		wg.Add(1)
		go func(node string, res *RouteResult) {
			defer wg.Done()

			if err := mr.sendToStorageNode(ctx, node, res.Metrics, headers); err != nil {
				errChan <- fmt.Errorf("failed to send to %s: %v", node, err)
				return
			}

			mr.updateStats(func(s *RouterStats) {
				s.StorageStats[node] += int64(len(res.Metrics))
			})

			log.Printf("Successfully sent %d metrics to %s via rule %s",
				len(res.Metrics), node, res.RuleName)
		}(storageNode, result)
	}

	wg.Wait()
	close(errChan)

	var errors []string
	for err := range errChan {
		errors = append(errors, err.Error())
	}

	if len(errors) > 0 {
		return fmt.Errorf("storage errors: %s", strings.Join(errors, "; "))
	}

	return nil
}

func (mr *MetricRouter) sendToStorageNode(ctx context.Context, storageNode string, metrics []prompb.TimeSeries, headers http.Header) error {
	if len(metrics) == 0 {
		return nil
	}

	url, exists := mr.config.StorageNodes[storageNode]
	if !exists {
		return fmt.Errorf("storage node %s not found in configuration", storageNode)
	}

	writeRequest := &prompb.WriteRequest{
		Timeseries: metrics,
	}

	data, err := proto.Marshal(writeRequest)
	if err != nil {
		return fmt.Errorf("failed to marshal protobuf: %v", err)
	}

	var body []byte
	var contentEncoding string

	if mr.config.EnableCompression {
		body = snappy.Encode(nil, data)
		contentEncoding = "snappy"
	} else {
		body = data
	}

	fmt.Println("--------------------------------------", url)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

	req.Header.Set("Content-Type", "application/x-protobuf")
	if contentEncoding != "" {
		req.Header.Set("Content-Encoding", contentEncoding)
	}
	req.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")

	for key, values := range headers {
		if strings.HasPrefix(strings.ToLower(key), "x-") ||
			strings.ToLower(key) == "authorization" {
			for _, value := range values {
				req.Header.Add(key, value)
			}
		}
	}

	return mr.sendWithRetry(req, storageNode)
}

func (mr *MetricRouter) sendWithRetry(req *http.Request, storageNode string) error {
	var lastErr error

	for attempt := 0; attempt <= mr.config.MaxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(attempt*attempt) * 100 * time.Millisecond
			time.Sleep(backoff)
			log.Printf("Retry attempt %d for %s", attempt, storageNode)
		}

		resp, err := mr.client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("HTTP request failed: %v", err)
			log.Printf("Attempt %d failed for %s: %v", attempt+1, storageNode, err)
			continue
		}

		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}

		lastErr = fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
		log.Printf("Attempt %d failed for %s: %v", attempt+1, storageNode, lastErr)

		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			break
		}
	}

	return fmt.Errorf("failed after %d attempts to %s: %v", mr.config.MaxRetries+1, storageNode, lastErr)
}

func (mr *MetricRouter) handleError(w http.ResponseWriter, message string, statusCode int) {
	mr.updateStats(func(s *RouterStats) {
		s.FailedRoutes++
		s.LastError = message
		s.LastErrorTime = time.Now()
	})

	log.Printf("Error: %s", message)
	http.Error(w, message, statusCode)
}

func (mr *MetricRouter) updateStats(fn func(*RouterStats)) {
	mr.stats.mutex.Lock()
	defer mr.stats.mutex.Unlock()
	fn(mr.stats)
}

func (mr *MetricRouter) GetStats() *RouterStats {
	mr.stats.mutex.RLock()
	defer mr.stats.mutex.RUnlock()

	stats := &RouterStats{
		TotalRequests:    mr.stats.TotalRequests,
		SuccessfulRoutes: mr.stats.SuccessfulRoutes,
		FailedRoutes:     mr.stats.FailedRoutes,
		StorageStats:     make(map[string]int64),
		LastError:        mr.stats.LastError,
		LastErrorTime:    mr.stats.LastErrorTime,
	}

	for k, v := range mr.stats.StorageStats {
		stats.StorageStats[k] = v
	}

	return stats
}

func (mr *MetricRouter) HandleStats(w http.ResponseWriter, r *http.Request) {
	stats := mr.GetStats()

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(stats); err != nil {
		http.Error(w, "Failed to encode stats", http.StatusInternalServerError)
		return
	}
}

func (mr *MetricRouter) HandleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	healthStatus := make(map[string]string)

	for node, url := range mr.config.StorageNodes {
		baseURL := strings.Replace(url, "/insert/0/prometheus/api/v1/write", "", 1)
		healthURL := baseURL + "/health"

		req, err := http.NewRequestWithContext(ctx, "GET", healthURL, nil)
		if err != nil {
			healthStatus[node] = "error: " + err.Error()
			continue
		}

		resp, err := mr.client.Do(req)
		if err != nil {
			healthStatus[node] = "unreachable: " + err.Error()
			continue
		}
		resp.Body.Close()

		if resp.StatusCode == 200 {
			healthStatus[node] = "healthy"
		} else {
			healthStatus[node] = "unhealthy: HTTP " + strconv.Itoa(resp.StatusCode)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":        "ok",
		"storage_nodes": healthStatus,
		"timestamp":     time.Now(),
	})
}

func LoadRouteConfig(configPath string) (*RouterConfig, error) {
	configData, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %v", configPath, err)
	}
	var config RouterConfig
	if err := yaml.Unmarshal(configData, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file %s: %v", configPath, err)
	}

	if config.ListenAddr == "" {
		config.ListenAddr = ":8300"
	}
	if config.DefaultStorage == "" {
		config.DefaultStorage = "short_term"
	}
	if config.MaxRetries == 0 {
		config.MaxRetries = 3
	}
	if config.Timeout == "" {
		config.Timeout = "30s"
	}
	if config.LogLevel == "" {
		config.LogLevel = "info"
	}

	if err := validateConfig(&config); err != nil {
		return nil, fmt.Errorf("invalid configuration: %v", err)
	}

	return &config, nil
}

func validateConfig(config *RouterConfig) error {
	if len(config.StorageNodes) == 0 {
		return fmt.Errorf("no storage nodes configured")
	}

	if _, exists := config.StorageNodes[config.DefaultStorage]; !exists {
		return fmt.Errorf("default storage node '%s' not found in storage_nodes", config.DefaultStorage)
	}

	for i, rule := range config.RoutingRules {
		if rule.Name == "" {
			return fmt.Errorf("routing rule %d: name is required", i)
		}
		if rule.StorageNode == "" {
			return fmt.Errorf("routing rule %s: storage_node is required", rule.Name)
		}
		if _, exists := config.StorageNodes[rule.StorageNode]; !exists {
			return fmt.Errorf("routing rule %s: storage node '%s' not found in storage_nodes", rule.Name, rule.StorageNode)
		}
	}

	return nil
}

func StartVMRouting() {
	configPath := "./vm_route.yaml"

	config, err := LoadRouteConfig(configPath)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	router, err := NewMetricRouter(config)
	if err != nil {
		log.Fatalf("Failed to create metric router: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/write", router.HandleRemoteWrite)
	mux.HandleFunc("/insert/0/prometheus/api/v1/write", router.HandleRemoteWrite)
	mux.HandleFunc("/stats", router.HandleStats)
	mux.HandleFunc("/health", router.HandleHealth)

	log.Printf("Metric router started on %s", config.ListenAddr)
	log.Printf("Using configuration file: %s", configPath)
	log.Printf("Default storage: %s", config.DefaultStorage)
	log.Printf("Storage nodes:")
	for name, url := range config.StorageNodes {
		log.Printf("  %s: %s", name, url)
	}
	log.Printf("Routing rules:")
	for i, rule := range config.RoutingRules {
		log.Printf("  %d. %s -> %s (priority: %d)", i+1, rule.Name, rule.StorageNode, rule.Priority)
	}

	server := &http.Server{
		Addr:         config.ListenAddr,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}
