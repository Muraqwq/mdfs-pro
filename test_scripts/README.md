# 分布式存储系统性能测试指南

## 环境准备

### 1. 检查依赖

确保已安装以下软件：
- Python 3.x
- FFmpeg
- requests, tqdm, psutil 库

```bash
# 检查 FFmpeg
ffmpeg -version

# 检查 Python
python3 --version

# 安装 Python 依赖
pip3 install requests tqdm psutil matplotlib
```

### 2. 准备测试视频

将你的测试电影文件（1.71GB）复制到测试目录：

```bash
# 将你的 movie.mkv 文件复制到 test_videos/input/ 目录
cp /path/to/your/movie.mkv test_videos/input/
```

### 3. 启动分布式系统

确保 Master 和 Worker 节点正在运行：

```bash
# 查看运行状态
docker-compose ps

# 如果未运行，启动系统
docker-compose up -d

# 查看日志
docker-compose logs -f
```

## 测试执行步骤

### 步骤 1: 切割视频（生成测试样本）

将大电影切割成 200 个小片段，每个片段约 8.5 秒。

```bash
cd test_scripts

# 执行切割脚本
./split_video.sh -i ../test_videos/input/movie.mkv -c 200 -d 8.5

# 参数说明：
# -i: 输入文件路径
# -c: 切割片段数量（200个）
# -d: 平均每段时长（8.5秒）

# 或者使用完整路径
./split_video.sh \
  --input ../test_videos/input/movie.mkv \
  --count 200 \
  --duration 8.5 \
  --output ../test_videos/split_output \
  --prefix test_movie
```

**预期结果**：
- 生成 200 个视频片段：`test_movie_0001.mp4` 到 `test_movie_0200.mp4`
- 总大小约 1.7 GB
- 每个片段约 8.7 MB
- 生成切割报告：`test_videos/split_output/split_report.txt`

**预计时间**：5-10 分钟（取决于 CPU 性能）

### 步骤 2: 启动性能监控（可选）

在一个独立的终端中启动监控脚本：

```bash
cd test_scripts

# 启动监控（5秒间隔，持续监控）
python3 performance_monitor.py

# 或者指定监控时长（例如：监控 10 分钟）
python3 performance_monitor.py --duration 600

# 或者保存监控数据
python3 performance_monitor.py --output ../test_videos/logs/monitor_data.json
```

### 步骤 3: 执行批量上传测试

#### 3.1 基准测试（单线程）

首先进行基准测试，建立性能基准：

```bash
cd test_scripts

# 单线程上传（基准测试）
python3 bulk_upload.py \
  --input_dir ../test_videos/split_output \
  --concurrent 1 \
  --output_dir ../test_videos/logs
```

#### 3.2 并发性能测试（主要测试）

进行 8 线程并发上传测试：

```bash
# 8 线程并发上传
python3 bulk_upload.py \
  --input_dir ../test_videos/split_output \
  --concurrent 8 \
  --retry 3 \
  --timeout 300 \
  --output_dir ../test_videos/logs

# 参数说明：
# --concurrent: 并发上传数（8个线程）
# --retry: 失败重试次数（3次）
# --timeout: 单个上传超时时间（300秒）
# --output_dir: 报告输出目录
```

#### 3.3 压力测试（可选）

测试系统的极限性能：

```bash
# 16 线程压力测试
python3 bulk_upload.py \
  --input_dir ../test_videos/split_output \
  --concurrent 16 \
  --retry 2 \
  --timeout 200
```

#### 3.4 限制测试（快速验证）

只上传少量文件进行快速验证：

```bash
# 只上传前 10 个文件
python3 bulk_upload.py \
  --input_dir ../test_videos/split_output \
  --limit 10 \
  --concurrent 2

# 干运行模式（只扫描，不上传）
python3 bulk_upload.py --dry_run
```

### 步骤 4: 墓碑机制验证测试

#### 4.1 删除后重启测试

```bash
# 1. 上传测试数据（如果还没有）
python3 bulk_upload.py --limit 50

# 2. 停止某个 Worker 节点
docker-compose stop worker2

# 3. 删除部分视频
# 通过 Web 界面或 API 删除

# 4. 重启 Worker 节点
docker-compose start worker2

# 5. 查看日志，验证墓碑机制
docker-compose logs worker2 | grep "墓碑机制"
```

#### 4.2 部分删除失败测试

```bash
# 1. 停止多个 Worker 节点
docker-compose stop worker1 worker2

# 2. 尝试删除文件
# 通过 Web 界面删除文件

# 3. 检查 Master 状态
curl http://localhost:8080/stats | jq .

# 4. 重启节点
docker-compose start worker1 worker2

# 5. 验证自动清理
docker-compose logs | grep "墓碑"
```

### 步骤 5: 查看测试报告

上传测试完成后，会自动生成报告：

```bash
# 查看生成的报告文件
ls -lh ../test_videos/logs/

# 查看 Markdown 格式的报告
cat ../test_videos/logs/upload_report_*.md

# 查看 JSON 格式的性能指标
cat ../test_videos/logs/upload_metrics_*.json
```

## 性能指标解读

### 基准性能指标（参考值）

基于 8 线程并发上传 200 个视频片段（1.71GB）：

**理想性能**：
- **总上传时间**: 2-4 分钟
- **平均上传速度**: 5-10 MB/s
- **吞吐量**: 40-80 MB/s (8并发)
- **成功率**: ≥ 99%
- **平均上传时间**: 0.5-1.5秒/文件

**可接受性能**：
- **总上传时间**: 4-6 分钟
- **平均上传速度**: 3-5 MB/s
- **吞吐量**: 24-40 MB/s (8并发)
- **成功率**: ≥ 95%
- **平均上传时间**: 1-2秒/文件

### 关键性能指标说明

1. **上传速度 (Upload Speed)**：
   - 单位: MB/s
   - 计算: 已上传总大小 / 总上传时间
   - 衡量: 数据传输效率

2. **吞吐量 (Throughput)**：
   - 单位: 文件/秒
   - 计算: 成功上传文件数 / 总上传时间
   - 衡量: 并发处理能力

3. **平均上传时间 (Avg Upload Time)**：
   - 单位: 秒
   - 计算: 所有文件上传时间的平均值
   - 衡量: 单文件处理效率

4. **成功率 (Success Rate)**：
   - 单位: 百分比
   - 计算: 成功上传数 / 总上传数 × 100%
   - 衡量: 系统稳定性

5. **服务器资源使用**：
   - CPU 使用率: ≤ 80% 为佳
   - 内存使用率: ≤ 70% 为佳
   - 网络流量: 需要监控瓶颈

## 故障排查

### 常见问题

#### 1. FFmpeg 找不到

```bash
# 检查 FFmpeg 是否安装
ffmpeg -version

# 如果未安装，使用 Homebrew 安装
brew install ffmpeg
```

#### 2. Python 依赖缺失

```bash
# 安装所有必需的 Python 库
pip3 install requests tqdm psutil matplotlib
```

#### 3. Docker 容器未运行

```bash
# 检查容器状态
docker-compose ps

# 启动容器
docker-compose up -d

# 查看日志
docker-compose logs -f
```

#### 4. 上传失败

**可能原因**：
- Worker 节点未运行
- 网络连接问题
- 文件损坏
- 超时时间设置太短

**解决方法**：
```bash
# 检查 Worker 状态
docker-compose ps

# 增加 timeout 参数
python3 bulk_upload.py --timeout 600

# 查看详细错误日志
cat ../test_videos/logs/upload_report_*.md
```

#### 5. 性能较差

**可能原因**：
- 磁盘 I/O 瓶颈
- 网络带宽限制
- CPU 性能不足
- 并发数设置不合理

**解决方法**：
```bash
# 检查系统资源
top
iostat
netstat -i

# 调整并发数
python3 bulk_upload.py --concurrent 4  # 降低并发数
python3 bulk_upload.py --concurrent 16 # 提高并发数

# 监控系统性能
python3 performance_monitor.py
```

## 测试场景清单

### 基础功能测试
- [ ] 视频切割成功，生成正确数量的片段
- [ ] 单文件上传成功
- [ ] 批量上传成功
- [ ] 文件下载正常
- [ ] 在线播放正常

### 性能测试
- [ ] 基准测试（单线程）
- [ ] 并发性能测试（8线程）
- [ ] 压力测试（16线程）
- [ ] 极限测试（找出系统上限）

### 墓碑机制测试
- [ ] 删除后节点重启，文件不"复活"
- [ ] 部分删除失败，墓碑记录正确创建
- [ ] 残留文件自动清理
- [ ] 墓碑清理机制正常运行

### 一致性测试
- [ ] 元数据一致性
- [ ] 副本数量正确
- [ ] 校验和验证通过
- [ ] 文件完整性验证

## 报告分析

### 成功的测试报告应包含：

1. **测试环境信息**
   - 系统配置
   - 网络环境
   - 测试参数

2. **性能指标**
   - 上传速度、吞吐量
   - 时间统计
   - 资源使用情况

3. **测试结果**
   - 成功率统计
   - 错误分析
   - 瓶颈识别

4. **优化建议**
   - 性能优化方向
   - 系统改进建议
   - 配置调整建议

## 下一步

完成性能测试后，你可以：

1. **分析测试报告**：识别性能瓶颈和优化点
2. **优化系统配置**：根据测试结果调整参数
3. **进行对比测试**：测试优化后的性能提升
4. **生成最终报告**：总结测试结果和改进建议

## 联系与支持

如遇到问题，请查看：
- 测试报告中的错误详情
- Docker 容器日志
- 系统监控数据

---

**测试完成后，请清理测试数据**：

```bash
# 清理测试视频
rm -rf test_videos/split_output/*

# 停止监控
# 按 Ctrl+C 停止监控脚本

# 查看并保留测试报告
ls -lh test_videos/logs/
```

**祝你测试顺利！**