package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url" // 必须导入这个包来处理地址转义
	"os"
	"path/filepath"
	"time"
)

// 全局变量，用于存储数据目录路径
var dataDir string

func main() {
	port := flag.String("port", "8081", "Worker Port")
	flag.Parse()

	// 1. 初始化存储目录逻辑
	dataDir = "./data_" + *port
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		fmt.Printf("无法创建存储目录: %v\n", err)
		return
	}

	// 2. 获取地址信息
	addr := os.Getenv("MY_ADDR")
	if addr == "" {
		addr = "http://localhost:" + *port
	}

	masterURL := os.Getenv("MASTER_URL")
	if masterURL == "" {
		masterURL = "http://localhost:8080"
	}

	// 3. 核心：心跳与元数据汇报逻辑
	go func() {
		for {
			files := getLocalFiles()
			jsonData, _ := json.Marshal(files)

			// 修正：使用 url.QueryEscape 处理地址中的特殊字符
			targetURL := fmt.Sprintf("%s/register?addr=%s", masterURL, url.QueryEscape(addr))

			resp, err := http.Post(targetURL, "application/json", bytes.NewBuffer(jsonData))
			if err == nil {
				resp.Body.Close()
			} else {
				fmt.Printf("等待 Master 响应... (%v)\n", err)
			}
			time.Sleep(5 * time.Second)
		}
	}()

	// 4. 处理文件上传
	http.HandleFunc("/upload", func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Query().Get("name")
		file, _, err := r.FormFile("file")
		if err != nil {
			http.Error(w, "无法读取上传文件", 400)
			return
		}
		defer file.Close()

		// 确保路径正确
		out, err := os.Create(filepath.Join(dataDir, name))
		if err != nil {
			fmt.Printf("磁盘写入失败: %v\n", err)
			http.Error(w, "存储失败", 500)
			return
		}
		defer out.Close()

		io.Copy(out, file)
		fmt.Printf("成功存储文件: %s\n", name)
		w.Write([]byte("OK"))
	})

	// 5. 处理文件下载
	http.HandleFunc("/download", func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Query().Get("name")
		path := filepath.Join(dataDir, name)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			http.NotFound(w, r)
			return
		}
		http.ServeFile(w, r, path)
	})

	fmt.Printf("Worker %s 启动，存储目录: %s\n", addr, dataDir)
	http.ListenAndServe(":"+*port, nil)
}

// 补全：getLocalFiles 函数，用于扫描本地已存文件
func getLocalFiles() []string {
	var fileNames []string
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		fmt.Printf("扫描目录失败: %v\n", err)
		return fileNames
	}
	for _, e := range entries {
		// 排除文件夹
		if !e.IsDir() {
			fileNames = append(fileNames, e.Name())
		}
	}
	return fileNames
}
