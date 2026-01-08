# 🎬 MDFS Pro (Movie Distributed File System)

MDFS Pro 是一个基于 Go 语言和 Docker 构建的**高度可用、具备自愈能力**的分布式电影存储系统。它采用了典型的 Master-Worker 架构，通过一致性哈希和副本机制实现了海量数据的分片存储与容错。

## 🌟 核心特性

- **分片存储 (Sharding)**：利用 **一致性哈希 (Consistent Hashing)** 算法，根据文件名将数据均匀分布在不同的物理节点上。
- **高可用副本 (Replication)**：默认采用 $N=2$ 副本策略。即使单个存储节点下线，资源依然可以正常下载。
- **故障自愈 (Self-Healing)**：内置 `Replication Fixer` 协程，自动检测并补齐由于节点宕机导致的副本缺失。
- **分布式元数据重构**：Worker 节点采用主动汇报机制（Heartbeat with Block Report），Master 重启后可瞬间恢复全局索引。
- **流媒体播放支持**：完美支持 HTTP Range 请求，支持大文件断点续传及在线视频进度条拖动。
- **权限分离**：通过 Token 机制区分 **管理员(审核员)** 与 **普通用户** 角色。
- **自动化运维**：支持 Docker Compose 一键部署与全集群日志统一收集。

## 🏗️ 系统架构

```text
[ Client ] 
    |
    ▼ (HTTP / Range)
[ Master (Gateway & Indexer) ] ── (Consistent Hash Ring)
    |
    ├─── [ Worker 1 (Data Node) ]
    ├─── [ Worker 2 (Data Node) ]
    └─── [ Worker 3 (Data Node) ]