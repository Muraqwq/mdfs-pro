#!/bin/bash

# 快速测试启动脚本
# 一键执行完整的性能测试流程

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}分布式存储系统性能测试${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# 检查参数
INPUT_FILE="$1"

if [ -z "$INPUT_FILE" ]; then
    echo -e "${RED}错误: 请指定输入视频文件路径${NC}"
    echo ""
    echo "使用方法:"
    echo "  $0 <video_file>"
    echo ""
    echo "示例:"
    echo "  $0 /path/to/movie.mkv"
    echo "  $0 movie.mkv"
    exit 1
fi

# 检查文件是否存在
if [ ! -f "$INPUT_FILE" ]; then
    echo -e "${RED}错误: 文件不存在: $INPUT_FILE${NC}"
    exit 1
fi

# 获取文件信息
FILE_SIZE=$(du -h "$INPUT_FILE" | cut -f1)
FILE_SIZE_BYTES=$(stat -f%z "$INPUT_FILE" 2>/dev/null || stat -c%s "$INPUT_FILE" 2>/dev/null)

echo "输入文件:"
echo "  路径: $INPUT_FILE"
echo "  大小: $FILE_SIZE ($FILE_SIZE_BYTES 字节)"
echo ""

# 确认继续
echo -e "${YELLOW}警告: 此操作将：${NC}"
echo "  1. 将电影切割成 200 个片段"
echo "  2. 上传所有片段到分布式系统"
echo "  3. 进行性能测试"
echo ""
read -p "确认继续? (y/n): " confirm

if [ "$confirm" != "y" ]; then
    echo "操作取消"
    exit 0
fi

# 确保在正确的目录
cd "$(dirname "$0")"
SCRIPT_DIR=$(pwd)

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}步骤 1: 准备环境${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# 创建必要的目录
mkdir -p "../test_videos/input"
mkdir -p "../test_videos/split_output"
mkdir -p "../test_videos/logs"
mkdir -p "../test_reports"

echo "✓ 目录结构已创建"

# 复制视频文件到测试目录
cp "$INPUT_FILE" "../test_videos/input/movie.mkv"
echo "✓ 视频文件已复制到测试目录"

# 检查 Docker 容器是否运行
echo ""
echo -e "${YELLOW}检查 Docker 容器状态...${NC}"
if ! docker-compose ps | grep -q "Up"; then
    echo -e "${RED}警告: Docker 容器未运行${NC}"
    echo "请先启动容器: docker-compose up -d"
    exit 1
fi
echo "✓ Docker 容器运行正常"

# 检查 Python 依赖
echo ""
echo -e "${YELLOW}检查 Python 依赖...${NC}"
if ! python3 -c "import requests, tqdm" 2>/dev/null; then
    echo -e "${RED}缺少 Python 依赖，正在安装...${NC}"
    pip3 install requests tqdm psutil matplotlib
fi
echo "✓ Python 依赖已安装"

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}步骤 2: 切割视频${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# 切割视频
echo "开始切割视频..."
echo "  片段数量: 200"
echo "  平均时长: 8.5秒"
echo "  预计时间: 5-10分钟"
echo ""

./split_video.sh \
    --input "../test_videos/input/movie.mkv" \
    --output "../test_videos/split_output" \
    --count 200 \
    --duration 8.5 \
    --prefix test_movie

if [ $? -ne 0 ]; then
    echo -e "${RED}视频切割失败${NC}"
    exit 1
fi

echo -e "${GREEN}✓ 视频切割完成${NC}"

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}步骤 3: 启动性能监控${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# 启动监控（后台运行）
echo "启动性能监控..."
python3 performance_monitor.py --duration 1200 --output "../test_videos/logs/monitor_data.json" &
MONITOR_PID=$!
echo "✓ 监控已启动 (PID: $MONITOR_PID)"
echo "  监控时长: 20分钟"
echo "  监控数据将保存到: ../test_videos/logs/monitor_data.json"
echo ""

# 等待监控启动
sleep 2

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}步骤 4: 执行批量上传测试${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# 执行上传测试
echo "开始批量上传..."
echo "  并发上传数: 8"
echo "  失败重试次数: 3"
echo "  超时时间: 300秒"
echo ""

python3 bulk_upload.py \
    --input_dir "../test_videos/split_output" \
    --output_dir "../test_videos/logs" \
    --concurrent 8 \
    --retry 3 \
    --timeout 300

UPLOAD_RESULT=$?

# 停止监控
echo ""
echo -e "${YELLOW}停止性能监控...${NC}"
kill $MONITOR_PID 2>/dev/null || true
wait $MONITOR_PID 2>/dev/null || true
echo "✓ 监控已停止"

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}测试完成！${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

if [ $UPLOAD_RESULT -eq 0 ]; then
    echo -e "${GREEN}✓ 所有文件上传成功${NC}"
else
    echo -e "${YELLOW}⚠ 部分文件上传失败${NC}"
fi

echo ""
echo "测试报告:"
ls -lh "../test_videos/logs/upload_report_"*.md 2>/dev/null | tail -1 | awk '{print "  Markdown: " $NF}'
ls -lh "../test_videos/logs/upload_metrics_"*.json 2>/dev/null | tail -1 | awk '{print "  JSON: " $NF}'
ls -lh "../test_videos/logs/monitor_data.json" 2>/dev/null | awk '{print "  监控数据: " $NF}'

echo ""
echo "下一步:"
echo "  1. 查看测试报告: cat ../test_videos/logs/upload_report_*.md"
echo "  2. 查看监控数据: cat ../test_videos/logs/monitor_data.json"
echo "  3. 运行墓碑机制测试: 参考 README.md"
echo "  4. 清理测试数据: rm -rf ../test_videos/split_output/*"

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}测试流程完成${NC}"
echo -e "${BLUE}========================================${NC}"

exit $UPLOAD_RESULT