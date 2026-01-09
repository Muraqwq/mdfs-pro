# 🎬 MDFS Pro (Movie Distributed File System)

MDFS Pro 是一个基于 Go 语言和 Docker 构建的**高度可用、具备自愈能力**的分布式电影存储系统。它采用了经典的 Master-Worker 架构，通过一致性哈希和副本机制实现了海量数据的分片存储与容错。

## 🌟 核心特性

- **分布式文件分布 (Consistent Hashing)**：系统利用**一致性哈希算法**（Consistent Hashing）将电影数据均匀、智能地分布在集群中的多个 Worker 节点上，以实现文件的分布式存储。每个物理节点对应多个虚拟节点，有效避免了数据倾斜，确保了负载的均衡分布和节点动态伸缩的平滑性。
- **高可用副本 (Replication)**：默认采用 $N=2$ 的副本策略，即每份电影数据会在两个独立的 Worker 节点上存储，极大地提升了数据的可靠性和读取时的可用性。即使单个存储节点故障，系统也能自动从其他副本节点提供服务。
- **故障自愈 (Self-Healing)**：内置 `Replication Fixer` 协程，持续监控文件副本状态。当检测到因节点下线导致的副本数量不足时，系统会自动协调从健康副本节点拉取数据并复制到新的可用 Worker 节点，实现数据的自动修复和集群的持续高可用。
- **分布式元数据管理**：Master 节点负责维护全局文件索引和 Worker 节点状态。Worker 节点通过心跳机制主动向 Master 汇报其存储的文件列表，使 Master 在重启后能快速重建完整的元数据视图，无需额外的持久化存储。
- **数据完整性校验 (CRC32 Checksum)**：系统已集成 CRC32 校验和机制。文件上传时在 Worker 端计算并存储校验和，Master 端记录，并在下载和验证时可用于验证数据完整性，防止数据损坏。
- **文件删除 (Distributed Deletion & Tombstone)**：提供管理员权限的文件删除 API，支持从所有存储节点删除文件。引入墓碑机制，确保即使 Worker 重启，已删除文件也不会“复活”，维护集群数据一致性。
- **流媒体播放支持 (HTTP Range)**：完美支持 HTTP Range 请求，实现大文件的断点续传以及在线视频播放时的进度条拖动功能，提供了流畅的观看体验。
- **系统监控与可观测性 (Prometheus & Grafana)**：Master 和 Worker 节点均提供 `/metrics` 接口，输出 Prometheus 格式的系统指标（如活跃节点数、文件总数、副本状态、存储字节数等）。配合 Docker Compose 一键部署 Prometheus 和 Grafana，可实现全面的集群运行状态监控与可视化。
- **Web 管理控制台**：Master 节点提供一个简洁直观的 Web 界面，方便管理员进行文件上传、播放、下载、校验和删除等操作，并实时查看集群统计信息和文件状态。

## 🏗️ 系统架构

MDFS Pro 采用经典的 Master-Worker 架构。
- **Master 节点**：作为系统的核心控制点，负责元数据管理、一致性哈希环的维护、Worker 节点的健康监控、故障自愈协调、文件上传下载的调度以及对外 API 接口的提供。
- **Worker 节点**：作为实际的数据存储单元，负责接收并持久化文件数据、计算并存储文件校验和、响应 Master 的心跳请求和数据服务请求（如下载、删除等）。
- **一致性哈希环**：实现逻辑上的数据分片和节点映射，确保数据在集群中的均匀分布和弹性伸缩。

```text
[ Client ] 
    |
    ▼ (HTTP / Range Request)
[ Master (Gateway, Metadata, Orchestrator) ] ── (Consistent Hash & Replication)
    |
    ├─── [ Worker 1 (Data Storage, Heartbeat) ]
    ├─── [ Worker 2 (Data Storage, Heartbeat) ]
    └─── [ Worker 3 (Data Storage, Heartbeat) ]
        ▲                        ▲
        |                        |
        ---------------------------
          (Metrics for Prometheus)
```

## 🚀 快速部署

通过 Docker Compose 可以快速启动整个 MDFS Pro 集群，包括 Master 节点、多个 Worker 节点以及可选的 Prometheus 和 Grafana 监控服务。

```bash
# 启动整个集群 (Master, Worker, Prometheus, Grafana)
docker-compose up -d

# 查看集群运行日志
docker-compose logs -f

# 停止并移除集群
docker-compose down
```

**部署后访问：**

- **Master Web 控制台**：`http://localhost:8080`

**常用 Master API 访问示例：**

- **系统健康检查**：`curl http://localhost:8080/health`
- **系统统计信息**：`curl http://localhost:8080/stats`
- **Prometheus 格式指标**：`curl http://localhost:8080/metrics`
