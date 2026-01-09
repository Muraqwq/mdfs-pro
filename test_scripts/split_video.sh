#!/bin/bash

# 视频切割脚本 - 将电影切割成多个小片段用于测试
# 使用方法: ./split_video.sh [选项]

set -e

# 默认参数
INPUT_FILE=""
OUTPUT_DIR="./test_videos/split_output"
SEGMENT_COUNT=200
AVG_DURATION=8.5
MIN_DURATION=5
MAX_DURATION=15
VIDEO_CODEC="libx264"
AUDIO_CODEC="aac"
VIDEO_FORMAT="mp4"
QUALITY=23
PRESET="fast"
OUTPUT_PREFIX="test_movie"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 显示帮助信息
show_help() {
    echo "视频切割脚本 - 将大电影切割成多个小片段用于性能测试"
    echo ""
    echo "使用方法: $0 [选项]"
    echo ""
    echo "选项:"
    echo "  -i, --input FILE          输入视频文件路径"
    echo "  -o, --output DIR          输出目录 (默认: ./test_videos/split_output)"
    echo "  -c, --count NUMBER        切割片段数量 (默认: 200)"
    echo "  -d, --duration SECONDS    平均每段时长 (默认: 8.5)"
    echo "  -p, --prefix STRING       输出文件前缀 (默认: test_movie)"
    echo "  -q, --quality NUMBER      CRF质量值 (默认: 23, 范围: 18-28)"
    echo "  -h, --help                显示此帮助信息"
    echo ""
    echo "示例:"
    echo "  $0 -i movie.mkv -c 200 -d 8.5"
    echo "  $0 --input movie.mkv --count 100 --duration 10"
}

# 解析命令行参数
while [[ $# -gt 0 ]]; do
    case $1 in
        -i|--input)
            INPUT_FILE="$2"
            shift 2
            ;;
        -o|--output)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        -c|--count)
            SEGMENT_COUNT="$2"
            shift 2
            ;;
        -d|--duration)
            AVG_DURATION="$2"
            shift 2
            ;;
        -p|--prefix)
            OUTPUT_PREFIX="$2"
            shift 2
            ;;
        -q|--quality)
            QUALITY="$2"
            shift 2
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            echo -e "${RED}未知选项: $1${NC}"
            show_help
            exit 1
            ;;
    esac
done

# 验证输入文件
if [ -z "$INPUT_FILE" ]; then
    echo -e "${RED}错误: 必须指定输入文件${NC}"
    show_help
    exit 1
fi

if [ ! -f "$INPUT_FILE" ]; then
    echo -e "${RED}错误: 输入文件不存在: $INPUT_FILE${NC}"
    exit 1
fi

# 创建输出目录
mkdir -p "$OUTPUT_DIR"

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}视频切割脚本启动${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "配置参数:"
echo "  输入文件: $INPUT_FILE"
echo "  输出目录: $OUTPUT_DIR"
echo "  片段数量: $SEGMENT_COUNT"
echo "  平均时长: ${AVG_DURATION}秒"
echo "  输出格式: $VIDEO_FORMAT"
echo "  视频编码: $VIDEO_CODEC"
echo "  音频编码: $AUDIO_CODEC"
echo "  质量设置: CRF $QUALITY"
echo "  文件前缀: $OUTPUT_PREFIX"
echo ""

# 获取视频信息
echo -e "${YELLOW}[1/5] 分析输入视频...${NC}"
VIDEO_INFO=$(ffprobe -v error -show_entries format=duration,size -show_entries stream=codec_name,width,height -of csv=p=0 "$INPUT_FILE")
VIDEO_DURATION=$(echo "$VIDEO_INFO" | head -1 | cut -d',' -f1)
VIDEO_SIZE=$(echo "$VIDEO_INFO" | head -1 | cut -d',' -f2)
VIDEO_CODEC_NAME=$(echo "$VIDEO_INFO" | tail -1 | cut -d',' -f1)
VIDEO_WIDTH=$(echo "$VIDEO_INFO" | tail -1 | cut -d',' -f2)
VIDEO_HEIGHT=$(echo "$VIDEO_INFO" | tail -1 | cut -d',' -f3)

DURATION_INT=$(echo "$VIDEO_DURATION" | cut -d'.' -f1)
echo "  视频时长: ${VIDEO_DURATION}秒"
echo "  文件大小: $VIDEO_SIZE 字节"
echo "  视频编码: $VIDEO_CODEC_NAME"
echo "  分辨率: ${VIDEO_WIDTH}x${VIDEO_HEIGHT}"
echo ""

# 验证切割参数
TOTAL_DURATION=$(echo "$DURATION_INT * $AVG_DURATION" | bc | cut -d'.' -f1)
if [ "$TOTAL_DURATION" -gt "$DURATION_INT" ]; then
    echo -e "${RED}警告: 按当前设置，总切割时长(${TOTAL_DURATION}秒)将超过视频时长(${DURATION_INT}秒)${NC}"
    echo "调整切割时长以适应视频..."
    NEW_DURATION=$(echo "scale=2; $VIDEO_DURATION / $SEGMENT_COUNT" | bc)
    echo -e "${YELLOW}新的平均时长: ${NEW_DURATION}秒${NC}"
    AVG_DURATION=$NEW_DURATION
fi

# 计算切割时间点
echo -e "${YELLOW}[2/5] 计算切割时间点...${NC}"
TIMES_FILE=$(mktemp)
for i in $(seq 1 $((SEGMENT_COUNT - 1))); do
    TIME=$(echo "scale=2; $i * $AVG_DURATION" | bc)
    echo "$TIME" >> "$TIMES_FILE"
done

SEGMENT_TIMES=$(cat "$TIMES_FILE" | tr '\n' ',' | sed 's/,$//')
echo "  生成 $SEGMENT_COUNT 个切割点"
echo ""

# 清理旧的输出文件
echo -e "${YELLOW}[3/5] 清理旧文件...${NC}"
rm -f "${OUTPUT_DIR}/${OUTPUT_PREFIX}"*.${VIDEO_FORMAT}
echo "  已清理旧文件"
echo ""

# 执行视频切割
echo -e "${YELLOW}[4/5] 开始切割视频...${NC}"
echo "这可能需要几分钟，请耐心等待..."

START_TIME=$(date +%s)

ffmpeg -i "$INPUT_FILE" \
    -c:v $VIDEO_CODEC -crf $QUALITY -preset $PRESET \
    -c:a $AUDIO_CODEC -b:a 128k \
    -f segment \
    -segment_time $AVG_DURATION \
    -reset_timestamps 1 \
    -segment_format $VIDEO_FORMAT \
    -avoid_negative_ts 1 \
    -movflags +faststart \
    "${OUTPUT_DIR}/${OUTPUT_PREFIX}_%04d.${VIDEO_FORMAT}" \
    < /dev/null 2>&1

END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

echo -e "${GREEN}  切割完成！用时: ${ELAPSED}秒${NC}"
echo ""

# 验证输出
echo -e "${YELLOW}[5/5] 验证输出文件...${NC}"

OUTPUT_COUNT=$(ls -1 "${OUTPUT_DIR}/${OUTPUT_PREFIX}"*.${VIDEO_FORMAT} 2>/dev/null | wc -l)
if [ "$OUTPUT_COUNT" -ne "$SEGMENT_COUNT" ]; then
    echo -e "${RED}警告: 输出片段数量 ($OUTPUT_COUNT) 与预期 ($SEGMENT_COUNT) 不匹配${NC}"
else
    echo -e "${GREEN}  成功生成 $OUTPUT_COUNT 个片段${NC}"
fi

# 获取文件大小信息
echo ""
echo "输出文件统计:"
TOTAL_SIZE=0
MAX_SIZE=0
MIN_SIZE=999999999999999

for file in "${OUTPUT_DIR}/${OUTPUT_PREFIX}"*.${VIDEO_FORMAT}; do
    SIZE=$(stat -f%z "$file" 2>/dev/null || stat -c%s "$file" 2>/dev/null)
    TOTAL_SIZE=$((TOTAL_SIZE + SIZE))

    if [ "$SIZE" -gt "$MAX_SIZE" ]; then
        MAX_SIZE=$SIZE
    fi
    if [ "$SIZE" -lt "$MIN_SIZE" ]; then
        MIN_SIZE=$SIZE
    fi
done

AVG_SIZE=$((TOTAL_SIZE / OUTPUT_COUNT))

# 格式化文件大小
format_size() {
    local size=$1
    if [ "$size" -lt 1024 ]; then
        echo "${size}B"
    elif [ "$size" -lt 1048576 ]; then
        echo "$((size / 1024))KB"
    elif [ "$size" -lt 1073741824 ]; then
        echo "$((size / 1048576))MB"
    else
        echo "$(echo "scale=2; $size / 1073741824" | bc)GB"
    fi
}

echo "  总大小: $(format_size $TOTAL_SIZE)"
echo "  平均大小: $(format_size $AVG_SIZE)"
echo "  最大文件: $(format_size $MAX_SIZE)"
echo "  最小文件: $(format_size $MIN_SIZE)"

# 验证视频质量
echo ""
echo "视频质量验证:"
FAILED=0
for file in "${OUTPUT_DIR}/${OUTPUT_PREFIX}"*.${VIDEO_FORMAT}; do
    DURATION_CHECK=$(ffprobe -v error -show_entries format=duration -of csv=p=0 "$file" 2>/dev/null)
    if [ -z "$DURATION_CHECK" ]; then
        echo -e "${RED}  错误: 文件损坏 $(basename $file)${NC}"
        FAILED=$((FAILED + 1))
    fi
done

if [ "$FAILED" -eq 0 ]; then
    echo -e "${GREEN}  所有片段视频质量正常${NC}"
else
    echo -e "${RED}  发现 $FAILED 个损坏的文件${NC}"
fi

# 生成切割报告
REPORT_FILE="${OUTPUT_DIR}/split_report.txt"
cat > "$REPORT_FILE" << EOF
视频切割报告
生成时间: $(date)

输入文件信息:
  文件路径: $INPUT_FILE
  文件大小: $VIDEO_SIZE 字节
  视频时长: $VIDEO_DURATION 秒
  视频编码: $VIDEO_CODEC_NAME
  分辨率: ${VIDEO_WIDTH}x${VIDEO_HEIGHT}

切割参数:
  片段数量: $SEGMENT_COUNT
  平均时长: $AVG_DURATION 秒
  输出格式: $VIDEO_FORMAT
  视频编码: $VIDEO_CODEC
  音频编码: $AUDIO_CODEC
  质量设置: CRF $QUALITY

输出统计:
  生成片段: $OUTPUT_COUNT
  总大小: $(format_size $TOTAL_SIZE)
  平均大小: $(format_size $AVG_SIZE)
  最大文件: $(format_size $MAX_SIZE)
  最小文件: $(format_size $MIN_SIZE)
  处理时间: ${ELAPSED} 秒

质量验证:
  损坏文件: $FAILED 个
  成功率: $(echo "scale=2; ($OUTPUT_COUNT - $FAILED) * 100 / $OUTPUT_COUNT" | bc)%

输出文件列表:
$(ls -lh "${OUTPUT_DIR}/${OUTPUT_PREFIX}"*.${VIDEO_FORMAT})
EOF

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}切割完成！${NC}"
echo -e "${GREEN}========================================${NC}"
echo "输出目录: $OUTPUT_DIR"
echo "报告文件: $REPORT_FILE"
echo ""
echo "下一步: 运行上传脚本进行性能测试"
echo "  python3 ../test_scripts/bulk_upload.py --output_dir '$OUTPUT_DIR'"
echo ""

# 清理临时文件
rm -f "$TIMES_FILE"

exit 0