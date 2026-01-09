package main

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"html/template"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

//go:embed index.html
var tmplHTML string

const AdminSecret = "admin888"

var allowedExtensions = []string{".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv", ".webm", ".m4v"}

var enableReplicationFixer = false // 配置开关：是否启用自动副本修复功能

func isAllowedExtension(filename string) bool {
	ext := strings.ToLower(filepath.Ext(filename))
	for _, allowed := range allowedExtensions {
		if ext == allowed {
			return true
		}
	}
	return false
}

func getContentType(filename string) string {
	ext := strings.ToLower(filepath.Ext(filename))
	mimeType := map[string]string{
		".mp4":  "video/mp4",
		".mkv":  "video/x-matroska",
		".avi":  "video/x-msvideo",
		".mov":  "video/quicktime",
		".wmv":  "video/x-ms-wmv",
		".flv":  "video/x-flv",
		".webm": "video/webm",
		".m4v":  "video/mp4",
	}
	if ct, ok := mimeType[ext]; ok {
		return ct
	}
	return "application/octet-stream"
}

type HashRing struct {
	nodes    []int
	nodeMap  map[int]string
	replicas int
}

func NewHashRing(reps int) *HashRing { return &HashRing{nodeMap: make(map[int]string), replicas: reps} }
func (h *HashRing) AddNode(addr string) {
	for i := 0; i < h.replicas; i++ {
		hash := int(crc32.ChecksumIEEE([]byte(strconv.Itoa(i) + addr)))
		h.nodes = append(h.nodes, hash)
		h.nodeMap[hash] = addr
	}
	sort.Ints(h.nodes)
}
func (h *HashRing) GetNodes(key string, count int) []string {
	if len(h.nodes) == 0 {
		return nil
	}
	hash := int(crc32.ChecksumIEEE([]byte(key)))
	idx := sort.Search(len(h.nodes), func(i int) bool { return h.nodes[i] >= hash })
	res := []string{}
	seen := make(map[string]bool)
	for len(res) < count && len(res) < len(h.nodeMap)/h.replicas {
		if idx == len(h.nodes) {
			idx = 0
		}
		addr := h.nodeMap[h.nodes[idx]]
		if !seen[addr] {
			res = append(res, addr)
			seen[addr] = true
		}
		idx++
	}
	return res
}

type GlobalState struct {
	mu           sync.RWMutex
	activeNodes  map[string]time.Time
	fileIndex    map[string]map[string]bool
	checksums    map[string]string
	deletedFiles map[string]time.Time
	ring         *HashRing
}

var state = GlobalState{
	activeNodes:  make(map[string]time.Time),
	fileIndex:    make(map[string]map[string]bool),
	checksums:    make(map[string]string),
	deletedFiles: make(map[string]time.Time),
	ring:         NewHashRing(10),
}

func main() {
	go healthChecker()

	if enableReplicationFixer {
		go replicationFixer()
		fmt.Println("✓ 副本修复功能已启用")
	} else {
		fmt.Println("✗ 副本修复功能已禁用")
	}

	go tombstoneCleaner()

	http.HandleFunc("/register", handleRegister)
	http.HandleFunc("/checksum", handleChecksum)
	http.HandleFunc("/get-checksum", handleGetChecksum)
	http.HandleFunc("/verify", handleVerify)
	http.HandleFunc("/delete", handleDelete)
	http.HandleFunc("/health", handleHealth)
	http.HandleFunc("/stats", handleStats)
	http.HandleFunc("/metrics", handleMetrics)
	http.HandleFunc("/search", handleSearch)
	http.HandleFunc("/", handleIndex)
	http.HandleFunc("/upload", handleUpload)
	http.HandleFunc("/download", handleDownload)
	http.HandleFunc("/play", handlePlay)

	fmt.Println("🚀 MDFS Master Pro 启动在 :8080")
	http.ListenAndServe(":8080", nil)
}

func handleRegister(w http.ResponseWriter, r *http.Request) {
	addr := r.URL.Query().Get("addr")
	var files []string
	json.NewDecoder(r.Body).Decode(&files)
	state.mu.Lock()
	defer state.mu.Unlock()

	if _, ok := state.activeNodes[addr]; !ok {
		state.ring.AddNode(addr)
	}
	state.activeNodes[addr] = time.Now()

	for _, f := range files {
		if deleteTime, exists := state.deletedFiles[f]; exists {
			if time.Since(deleteTime) < 24*time.Hour {
				go func(name string) {
					time.Sleep(1 * time.Second)
					resp, err := http.Get(addr + "/delete?name=" + url.QueryEscape(name))
					if err == nil {
						resp.Body.Close()
						fmt.Printf("墓碑机制：自动删除重启节点上的残留文件 %s\n", name)
					}
				}(f)
				continue
			} else {
				delete(state.deletedFiles, f)
			}
		}

		if state.fileIndex[f] == nil {
			state.fileIndex[f] = make(map[string]bool)
		}
		state.fileIndex[f][addr] = true
	}
}

func handleChecksum(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	checksum := r.URL.Query().Get("checksum")
	if name == "" || checksum == "" {
		http.Error(w, "缺少参数", 400)
		return
	}
	state.mu.Lock()
	state.checksums[name] = checksum
	state.mu.Unlock()
	w.Write([]byte("OK"))
}

func handleGetChecksum(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	state.mu.RLock()
	checksum := state.checksums[name]
	nodes := state.fileIndex[name]
	state.mu.RUnlock()

	if checksum != "" {
		w.Write([]byte(checksum))
		return
	}

	if len(nodes) > 0 {
		for node := range nodes {
			resp, err := http.Get(node + "/checksum?name=" + url.QueryEscape(name))
			if err != nil {
				continue
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == 200 && len(body) > 0 {
				w.Write(body)
				return
			}
		}
	}

	http.Error(w, "未找到校验和", 404)
}

func handleVerify(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	state.mu.RLock()
	nodes := state.fileIndex[name]
	expectedChecksum := state.checksums[name]
	state.mu.RUnlock()

	if len(nodes) == 0 {
		http.Error(w, "文件不存在", 404)
		return
	}

	type VerifyResult struct {
		Node     string `json:"node"`
		Checksum string `json:"checksum"`
		Valid    bool   `json:"valid"`
	}
	results := []VerifyResult{}

	for node := range nodes {
		resp, err := http.Get(node + "/verify?name=" + url.QueryEscape(name) + "&checksum=" + expectedChecksum)
		if err != nil {
			results = append(results, VerifyResult{Node: node, Checksum: "ERROR", Valid: false})
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		bodyStr := string(body)

		var checksum string
		var valid bool

		if strings.HasPrefix(bodyStr, "OK:") {
			checksum = strings.TrimPrefix(bodyStr, "OK:")
			valid = true
		} else if strings.HasPrefix(bodyStr, "CHECKSUM_MISMATCH:") {
			checksum = strings.TrimPrefix(bodyStr, "CHECKSUM_MISMATCH:")
			valid = false
		} else {
			checksum = "UNKNOWN"
			valid = false
		}

		results = append(results, VerifyResult{
			Node:     node,
			Checksum: checksum,
			Valid:    valid,
		})
	}

	jsonData, _ := json.Marshal(results)
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonData)
}

func handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.FormValue("secret") != AdminSecret {
		http.Error(w, "Unauthorized", 401)
		return
	}

	name := r.URL.Query().Get("name")
	state.mu.Lock()
	nodes, exists := state.fileIndex[name]
	if !exists {
		state.mu.Unlock()
		http.Error(w, "文件不存在", 404)
		return
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	successNodes := []string{}

	for node := range nodes {
		wg.Add(1)
		go func(n string) {
			defer wg.Done()
			resp, err := http.Get(n + "/delete?name=" + url.QueryEscape(name))
			if err != nil {
				fmt.Printf("删除请求失败: %s -> %s\n", name, n)
				return
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == 200 && string(body) == "OK" {
				mu.Lock()
				successNodes = append(successNodes, n)
				mu.Unlock()
			}
		}(node)
	}
	wg.Wait()

	if len(successNodes) >= 1 {
		state.deletedFiles[name] = time.Now()
	}

	for _, n := range successNodes {
		delete(nodes, n)
	}

	if len(nodes) == 0 {
		delete(state.checksums, name)
		delete(state.fileIndex, name)
		fmt.Printf("文件 %s 已从所有节点删除，创建墓碑\n", name)
	} else {
		fmt.Printf("文件 %s 部分删除失败（剩余 %d 个节点），创建墓碑并保留元数据\n", name, len(nodes))
	}

	state.mu.Unlock()
	w.Write([]byte(fmt.Sprintf("OK:%d", len(successNodes))))
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("OK"))
}

func handleStats(w http.ResponseWriter, r *http.Request) {
	state.mu.RLock()
	stats := map[string]interface{}{
		"active_nodes":    len(state.activeNodes),
		"total_files":     len(state.fileIndex),
		"total_checksums": len(state.checksums),
		"ring_size":       len(state.ring.nodes),
	}
	state.mu.RUnlock()

	jsonData, _ := json.Marshal(stats)
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonData)
}

func handleMetrics(w http.ResponseWriter, r *http.Request) {
	state.mu.RLock()
	nodeCount := len(state.activeNodes)
	fileCount := len(state.fileIndex)
	underReplicated := 0
	for _, nodes := range state.fileIndex {
		if len(nodes) < 2 {
			underReplicated++
		}
	}
	state.mu.RUnlock()

	metrics := fmt.Sprintf(`# HELP mdfs_active_nodes Number of active worker nodes
# TYPE mdfs_active_nodes gauge
mdfs_active_nodes %d
# HELP mdfs_total_files Total number of stored files
# TYPE mdfs_total_files gauge
mdfs_total_files %d
# HELP mdfs_under_replicated_files Number of files with less than 2 replicas
# TYPE mdfs_under_replicated_files gauge
mdfs_under_replicated_files %d
# HELP mdfs_up System is up
# TYPE mdfs_up gauge
mdfs_up 1
`, nodeCount, fileCount, underReplicated)

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Write([]byte(metrics))
}

func handleIndex(w http.ResponseWriter, r *http.Request) {
	state.mu.RLock()

	// 计算副本不足的文件数
	underReplicated := 0
	for _, nodes := range state.fileIndex {
		if len(nodes) < 2 {
			underReplicated++
		}
	}

	// 计算副本完整率
	fileCount := len(state.fileIndex)
	replicationRate := 0.0
	if fileCount > 0 {
		replicationRate = float64(fileCount-underReplicated) / float64(fileCount) * 100
	}

	data := struct {
		Nodes           map[string]time.Time
		Files           map[string]map[string]bool
		AdminKey        string
		NodeCount       int
		FileCount       int
		UnderReplicated int
		ReplicationRate float64
	}{
		Nodes:           state.activeNodes,
		Files:           state.fileIndex,
		AdminKey:        AdminSecret,
		NodeCount:       len(state.activeNodes),
		FileCount:       fileCount,
		UnderReplicated: underReplicated,
		ReplicationRate: replicationRate,
	}
	state.mu.RUnlock()

	// 绑定模板函数 (JS 转义等)
	funcMap := template.FuncMap{
		"js_escape": func(s string) string { return strings.ReplaceAll(s, "'", "\\'") },
	}

	// 解析嵌入的 HTML 模板
	tmpl, err := template.New("index").Funcs(funcMap).Parse(tmplHTML)
	if err != nil {
		http.Error(w, "模板加载失败: "+err.Error(), 500)
		return
	}
	tmpl.Execute(w, data)
}

func handleSearch(w http.ResponseWriter, r *http.Request) {
	query := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("query")))

	state.mu.RLock()
	defer state.mu.RUnlock()

	results := map[string]map[string]bool{}

	if query == "" {
		results = state.fileIndex
	} else {
		for name, nodes := range state.fileIndex {
			if strings.Contains(strings.ToLower(name), query) {
				results[name] = nodes
			}
		}
	}

	jsonData, _ := json.Marshal(results)
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonData)
}

func handleUpload(w http.ResponseWriter, r *http.Request) {
	// 1. 权限校验
	if r.FormValue("secret") != AdminSecret {
		http.Error(w, "Unauthorized", 401)
		return
	}

	// 2. 解析文件
	file, header, err := r.FormFile("movie")
	if err != nil {
		http.Error(w, "文件解析失败", 400)
		return
	}
	defer file.Close()

	name := header.Filename

	if !isAllowedExtension(name) {
		http.Error(w, "仅支持上传视频文件 (mp4, mkv, avi, mov, wmv, flv, webm, m4v)", 400)
		return
	}

	// 将文件内容读取到内存，为每个目标创建独立的数据流
	fileData, err := io.ReadAll(file)
	if err != nil {
		http.Error(w, "文件读取失败", 400)
		return
	}
	targets := state.ring.GetNodes(name, 2)
	if len(targets) == 0 {
		http.Error(w, "无可用存储节点", 500)
		return
	}

	var wg sync.WaitGroup
	var mu sync.Mutex // 用于保护 successNodes 切片
	successNodes := []string{}

	for _, node := range targets {
		wg.Add(1)
		go func(n string) {
			defer wg.Done()

			pr, pw := io.Pipe()
			writer := multipart.NewWriter(pw)

			go func() {
				defer func() {
					if r := recover(); r != nil {
						fmt.Printf("捕获到协程异常: %v\n", r)
					}
				}()
				defer pw.Close()
				defer writer.Close()

				part, err := writer.CreateFormFile("file", name)
				if err != nil {
					return
				}

				reader := bytes.NewReader(fileData)
				_, copyErr := io.Copy(part, reader)
				if copyErr != nil {
					fmt.Printf("数据拷贝中断（可能是网络连接已关闭）: %v\n", copyErr)
				}
			}()

			targetURL := fmt.Sprintf("%s/upload?name=%s", n, url.QueryEscape(name))
			resp, err := http.Post(targetURL, writer.FormDataContentType(), pr)

			if err == nil && resp.StatusCode == 200 {
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				bodyStr := string(body)
				if strings.HasPrefix(bodyStr, "OK:") {
					checksum := strings.TrimPrefix(bodyStr, "OK:")
					state.mu.Lock()
					state.checksums[name] = checksum
					state.mu.Unlock()
				}
				mu.Lock()
				successNodes = append(successNodes, n)
				mu.Unlock()
				fmt.Printf("副本分发成功: %s -> %s (checksum: %s)\n", name, n, strings.TrimPrefix(bodyStr, "OK:"))
			} else {
				fmt.Printf("副本分发失败: %s -> %s\n", name, n)
			}
		}(node)
	}

	wg.Wait()

	if len(successNodes) > 0 {
		state.mu.Lock()
		if state.fileIndex[name] == nil {
			state.fileIndex[name] = make(map[string]bool)
		}
		for _, n := range successNodes {
			state.fileIndex[name][n] = true
		}
		state.mu.Unlock()
		w.WriteHeader(200)
		fmt.Printf("文件 %s 分发完成，成功副本数: %d\n", name, len(successNodes))
	} else {
		http.Error(w, "所有存储节点写入失败", 500)
	}
}

func tombstoneCleaner() {
	for {
		time.Sleep(1 * time.Hour)
		state.mu.Lock()
		for name, deleteTime := range state.deletedFiles {
			if time.Since(deleteTime) > 30*24*time.Hour {
				delete(state.deletedFiles, name)
				fmt.Printf("清理过期墓碑记录: %s\n", name)
			}

		}
	}
	state.mu.Unlock()
}

func handleDownload(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	state.mu.RLock()
	nodes := state.fileIndex[name]
	state.mu.RUnlock()
	for node := range nodes {
		req, _ := http.NewRequest("GET", node+"/download?name="+url.QueryEscape(name), nil)
		if rH := r.Header.Get("Range"); rH != "" {
			req.Header.Set("Range", rH)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil || resp.StatusCode >= 400 {
			continue
		}
		defer resp.Body.Close()
		for k, v := range resp.Header {
			for _, vv := range v {
				w.Header().Add(k, vv)
			}
		}
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"; filename*=UTF-8''%s", name, url.PathEscape(name)))
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		return
	}
	http.Error(w, "Unavailable", 404)
}

func handlePlay(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	state.mu.RLock()
	nodes := state.fileIndex[name]
	state.mu.RUnlock()

	for node := range nodes {
		req, _ := http.NewRequest("GET", node+"/download?name="+url.QueryEscape(name), nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil || resp.StatusCode >= 400 {
			continue
		}
		defer resp.Body.Close()

		for k, v := range resp.Header {
			for _, vv := range v {
				w.Header().Add(k, vv)
			}
		}

		contentType := getContentType(name)
		w.Header().Set("Content-Type", contentType)
		w.Header().Set("Content-Disposition", fmt.Sprintf("inline; filename=\"%s\"; filename*=UTF-8''%s", name, url.PathEscape(name)))

		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		return
	}
	http.Error(w, "Unavailable", 404)
}

func healthChecker() {
	for {
		time.Sleep(5 * time.Second)
		state.mu.Lock()
		for addr, last := range state.activeNodes {
			if time.Since(last) > 15*time.Second {
				delete(state.activeNodes, addr)
				for _, ns := range state.fileIndex {
					delete(ns, addr)
				}
				newRing := NewHashRing(10)
				for a := range state.activeNodes {
					newRing.AddNode(a)
				}
				state.ring = newRing
			}
		}
		state.mu.Unlock()
	}
}

func replicationFixer() {
	for {
		time.Sleep(10 * time.Second)
		state.mu.Lock()
		for name, nodes := range state.fileIndex {
			if _, deleted := state.deletedFiles[name]; deleted {
				continue
			}

			if len(nodes) < 2 && len(state.activeNodes) >= 2 {
				var src string
				for n := range nodes {
					src = n
					break
				}
				if src == "" {
					continue
				}
				targets := state.ring.GetNodes(name, 2)
				for _, t := range targets {
					if !nodes[t] {
						go func(f, s, target string) {
							res, _ := http.Get(s + "/download?name=" + url.QueryEscape(f))
							if res == nil {
								return
							}
							defer res.Body.Close()
							pr, pw := io.Pipe()
							wr := multipart.NewWriter(pw)
							go func() { defer pw.Close(); defer wr.Close(); p, _ := wr.CreateFormFile("file", f); io.Copy(p, res.Body) }()
							http.Post(target+"/upload?name="+url.QueryEscape(f), wr.FormDataContentType(), pr)
						}(name, src, t)
					}
				}
			}
		}
		state.mu.Unlock()
	}
}
