package main

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"html/template"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"time"
)

const AdminSecret = "admin888"

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
	mu          sync.RWMutex
	activeNodes map[string]time.Time
	fileIndex   map[string]map[string]bool
	ring        *HashRing
}

var state = GlobalState{
	activeNodes: make(map[string]time.Time),
	fileIndex:   make(map[string]map[string]bool),
	ring:        NewHashRing(10),
}

func main() {
	go healthChecker()
	go replicationFixer()

	http.HandleFunc("/register", handleRegister)
	http.HandleFunc("/", handleIndex)
	http.HandleFunc("/upload", handleUpload)
	http.HandleFunc("/download", handleDownload)

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
		if state.fileIndex[f] == nil {
			state.fileIndex[f] = make(map[string]bool)
		}
		state.fileIndex[f][addr] = true
	}
}

func handleIndex(w http.ResponseWriter, r *http.Request) {
	tmpl := `<!DOCTYPE html>
	<html>
	<head>
		<meta charset="UTF-8"><title>MDFS Pro 控制台</title>
		<link href="https://cdn.bootcdn.net/ajax/libs/twitter-bootstrap/5.2.3/css/bootstrap.min.css" rel="stylesheet">
		<style>body{background:#f8f9fa} .card{margin-top:20px; border:none; box-shadow:0 2px 10px rgba(0,0,0,0.05)}</style>
	</head>
	<body class="container">
		<div class="card"><div class="card-body">
			<div class="d-flex justify-content-between">
				<h1>🎬 MDFS Pro 云存储</h1>
				<div>
					<button id="loginBtn" class="btn btn-outline-primary btn-sm" onclick="adminLogin()">管理登录</button>
					<button id="logoutBtn" class="btn btn-outline-danger btn-sm" style="display:none" onclick="adminLogout()">退出</button>
				</div>
			</div>
			<p class="text-muted">集群节点: {{range $n, $t := .Nodes}}<span class="badge bg-success me-1">#{{$n}}</span>{{end}}</p>
			<div id="adminSection" style="display:none" class="mt-3">
				<div class="input-group"><input type="file" id="fileInput" class="form-control"><button class="btn btn-primary" id="upBtn" onclick="upload()">分发上传</button></div>
				<div class="progress mt-2" id="pCont" style="display:none"><div id="pBar" class="progress-bar progress-bar-striped progress-bar-animated" style="width:0%">0%</div></div>
			</div>
		</div></div>
		<div class="card"><div class="card-body">
			<table class="table table-hover">
				<thead><tr><th>文件名</th><th>副本状态</th><th>操作</th></tr></thead>
				<tbody>
					{{range $name, $nodes := .Files}}
					<tr>
						<td><strong>{{$name}}</strong></td>
						<td><span class="badge {{if ge (len $nodes) 2}}bg-info{{else}}bg-warning{{end}}">{{len $nodes}} 副本在线</span></td>
						<td>{{if gt (len $nodes) 0}}<a href="/download?name={{urlquery $name}}" class="btn btn-sm btn-primary">下载/播放</a>{{else}}<button class="btn btn-sm btn-secondary" disabled>离线</button>{{end}}</td>
					</tr>
					{{end}}
				</tbody>
			</table>
		</div></div>
		<script>
			const token = localStorage.getItem("mdfs_token");
			if(token === "{{.AdminKey}}"){ document.getElementById("adminSection").style.display="block"; document.getElementById("loginBtn").style.display="none"; document.getElementById("logoutBtn").style.display="block"; }
			function adminLogin(){ const p = prompt("密钥:"); if(p==="{{.AdminKey}}"){localStorage.setItem("mdfs_token",p); location.reload();} }
			function adminLogout(){ localStorage.removeItem("mdfs_token"); location.reload(); }
			function upload(){
				const file = document.getElementById('fileInput').files[0]; if(!file) return;
				const btn = document.getElementById('upBtn'); const pBar = document.getElementById('pBar');
				document.getElementById('pCont').style.display='flex'; btn.disabled=true;
				const fd = new FormData(); fd.append("movie", file); fd.append("secret", token);
				const xhr = new XMLHttpRequest(); xhr.open("POST", "/upload");
				xhr.upload.onprogress = (e) => { const per = Math.round((e.loaded/e.total)*100); pBar.style.width=per+"%"; pBar.innerText=per+"%"; };
				xhr.onload = () => { if(xhr.status===200){ alert("成功"); location.reload(); }else{ alert("失败: "+xhr.status); btn.disabled=false; } };
				xhr.send(fd);
			}
		</script>
	</body></html>`
	state.mu.RLock()
	defer state.mu.RUnlock()
	t, _ := template.New("i").Parse(tmpl)
	t.Execute(w, struct {
		Nodes    map[string]time.Time
		Files    map[string]map[string]bool
		AdminKey string
	}{state.activeNodes, state.fileIndex, AdminSecret})
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
	targets := state.ring.GetNodes(name, 2)
	if len(targets) == 0 {
		http.Error(w, "无可用存储节点", 500)
		return
	}

	var wg sync.WaitGroup
	var mu sync.Mutex     // 用于保护 successNodes 切片
	var fileMu sync.Mutex // 用于保护文件读取指针（防止并发 Seek 冲突）
	successNodes := []string{}

	for _, node := range targets {
		wg.Add(1)
		go func(n string) {
			defer wg.Done()

			pr, pw := io.Pipe()
			writer := multipart.NewWriter(pw)

			// 启动协程写入数据到 Pipe
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
					return // 如果 Pipe 已经关了，直接退出不报错
				}

				fileMu.Lock()
				file.Seek(0, 0)
				_, copyErr := io.Copy(part, file)
				fileMu.Unlock()

				if copyErr != nil {
					// 如果这里报错，通常说明网络已经断了，Worker 端可能没收全
					fmt.Printf("数据拷贝中断（可能是网络连接已关闭）: %v\n", copyErr)
				}
			}()

			// 发送 POST 请求到 Worker
			targetURL := fmt.Sprintf("%s/upload?name=%s", n, url.QueryEscape(name))
			resp, err := http.Post(targetURL, writer.FormDataContentType(), pr)

			if err == nil && resp.StatusCode == 200 {
				resp.Body.Close()
				mu.Lock()
				successNodes = append(successNodes, n)
				mu.Unlock()
				fmt.Printf("副本分发成功: %s -> %s\n", name, n)
			} else {
				fmt.Printf("副本分发失败: %s -> %s\n", name, n)
			}
		}(node)
	}

	wg.Wait()

	// 3. 更新元数据索引
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
