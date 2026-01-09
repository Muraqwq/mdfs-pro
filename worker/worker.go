package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

		tempFile, err := os.Create(filepath.Join(dataDir, name+".tmp"))
		if err != nil {
			fmt.Printf("无法创建临时文件: %v\n", err)
			http.Error(w, "存储失败", 500)
			return
		}

		hash := crc32.NewIEEE()
		writer := io.MultiWriter(tempFile, hash)

		_, err = io.Copy(writer, file)
		tempFile.Close()
		if err != nil {
			os.Remove(filepath.Join(dataDir, name+".tmp"))
			fmt.Printf("文件写入失败: %v\n", err)
			http.Error(w, "存储失败", 500)
			return
		}

		checksum := hash.Sum32()
		checksumStr := fmt.Sprintf("%08x", checksum)

		err = os.Rename(filepath.Join(dataDir, name+".tmp"), filepath.Join(dataDir, name))
		if err != nil {
			os.Remove(filepath.Join(dataDir, name+".tmp"))
			fmt.Printf("文件重命名失败: %v\n", err)
			http.Error(w, "存储失败", 500)
			return
		}

		checksumFile, err := os.Create(filepath.Join(dataDir, name+".checksum"))
		if err != nil {
			fmt.Printf("无法创建校验文件: %v\n", err)
			http.Error(w, "存储失败", 500)
			return
		}
		checksumFile.WriteString(checksumStr)
		checksumFile.Close()

		fmt.Printf("成功存储文件: %s (checksum: %s)\n", name, checksumStr)
		w.Write([]byte("OK:" + checksumStr))
	})

	// 5. 处理文件下载
	http.HandleFunc("/download", func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Query().Get("name")
		path := filepath.Join(dataDir, name)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			http.NotFound(w, r)
			return
		}

		file, err := os.Open(path)
		if err != nil {
			http.Error(w, "文件打开失败", 500)
			return
		}
		defer file.Close()

		info, _ := file.Stat()
		size := info.Size()

		contentType := "application/octet-stream"
		ext := strings.ToLower(filepath.Ext(name))
		mimeTypes := map[string]string{
			".mp4":  "video/mp4",
			".mkv":  "video/x-matroska",
			".avi":  "video/x-msvideo",
			".mov":  "video/quicktime",
			".wmv":  "video/x-ms-wmv",
			".flv":  "video/x-flv",
			".webm": "video/webm",
			".m4v":  "video/mp4",
		}
		if ct, ok := mimeTypes[ext]; ok {
			contentType = ct
		}

		w.Header().Set("Content-Type", contentType)
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))

		if rangeHeader := r.Header.Get("Range"); rangeHeader != "" {
			http.ServeContent(w, r, name, info.ModTime(), file)
		} else {
			io.Copy(w, file)
		}
	})

	// 6. 校验文件校验和
	http.HandleFunc("/verify", func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Query().Get("name")
		expectedChecksum := r.URL.Query().Get("checksum")

		filePath := filepath.Join(dataDir, name)
		checksumPath := filepath.Join(dataDir, name+".checksum")

		file, err := os.Open(filePath)
		if err != nil {
			http.Error(w, "文件不存在", 404)
			return
		}
		defer file.Close()

		hash := crc32.NewIEEE()
		io.Copy(hash, file)
		actualChecksum := fmt.Sprintf("%08x", hash.Sum32())

		if expectedChecksum != "" {
			if actualChecksum != expectedChecksum {
				w.WriteHeader(400)
				w.Write([]byte("CHECKSUM_MISMATCH:" + actualChecksum))
				return
			}
			w.Write([]byte("OK:" + actualChecksum))
			return
		}

		storedChecksum, err := os.ReadFile(checksumPath)
		if err != nil {
			w.WriteHeader(500)
			w.Write([]byte("NO_CHECKSUM_FILE"))
			return
		}

		if string(bytes.TrimSpace(storedChecksum)) == actualChecksum {
			w.Write([]byte("OK:" + actualChecksum))
		} else {
			w.WriteHeader(400)
			w.Write([]byte("CHECKSUM_MISMATCH:" + actualChecksum))
		}
	})

	// 7. 获取校验和
	http.HandleFunc("/checksum", func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Query().Get("name")
		checksumPath := filepath.Join(dataDir, name+".checksum")

		data, err := os.ReadFile(checksumPath)
		if err != nil {
			http.Error(w, "校验文件不存在", 404)
			return
		}
		w.Write(data)
	})

	// 8. 删除文件
	http.HandleFunc("/delete", func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Query().Get("name")
		filePath := filepath.Join(dataDir, name)
		checksumPath := filepath.Join(dataDir, name+".checksum")

		fileExists := false
		if _, err := os.Stat(filePath); err == nil {
			if err := os.Remove(filePath); err != nil {
				fmt.Printf("删除文件失败: %v\n", err)
				http.Error(w, "删除失败", 500)
				return
			}
			fileExists = true
		}
		if _, err := os.Stat(checksumPath); err == nil {
			os.Remove(checksumPath)
		}

		if fileExists {
			fmt.Printf("已删除文件: %s\n", name)
		} else {
			fmt.Printf("文件不存在，视为删除成功: %s\n", name)
		}
		w.Write([]byte("OK"))
	})

	// 9. 健康检查
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("OK"))
	})

	// 10. 监控指标
	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		files := getLocalFiles()
		var totalSize int64
		for _, f := range files {
			info, err := os.Stat(filepath.Join(dataDir, f))
			if err == nil {
				totalSize += info.Size()
			}
		}

		metrics := fmt.Sprintf(`# HELP mdfs_worker_files Number of files stored
# TYPE mdfs_worker_files gauge
mdfs_worker_files %d
# HELP mdfs_worker_bytes_total Total bytes stored
# TYPE mdfs_worker_bytes_total counter
mdfs_worker_bytes_total %d
# HELP mdfs_worker_up Worker is up
# TYPE mdfs_worker_up gauge
mdfs_worker_up 1
`, len(files), totalSize)

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Write([]byte(metrics))
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
		if !e.IsDir() && !strings.HasSuffix(e.Name(), ".checksum") && !strings.HasSuffix(e.Name(), ".tmp") {
			fileNames = append(fileNames, e.Name())
		}
	}
	return fileNames
}
