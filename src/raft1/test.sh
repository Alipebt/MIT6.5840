#!/bin/bash

set -e  # 遇到错误退出

# 默认参数值
TEST_PATTERN="${1:-3A}"
LOG_COUNT="${2:-3}"
RACE_FLAG=""

# 解析参数
while [[ $# -gt 0 ]]; do
    case $1 in
        -race|--race)
            RACE_FLAG="-r"
            shift
            ;;
        -p|--pattern)
            TEST_PATTERN="$2"
            shift 2
            ;;
        -c|--count)
            LOG_COUNT="$2"
            shift 2
            ;;
        *)
            # 处理位置参数
            if [[ -z "$POSITIONAL_1" ]]; then
                POSITIONAL_1="$1"
                TEST_PATTERN="$1"
            elif [[ -z "$POSITIONAL_2" ]]; then
                POSITIONAL_2="$1"
                LOG_COUNT="$1"
            fi
            shift
            ;;
    esac
done

# 配置变量
LOG_DIR="tmp"
LOG_FILE="$LOG_DIR/test_output_$(date +%Y%m%d_%H%M%S).log"
PIPE_FILE="test_pipe.$$"  # 使用PID确保唯一性
MAX_LOG_FILES=5  # 保留的最新日志文件数量

# 创建日志目录
mkdir -p "$LOG_DIR"

# 清理函数
cleanup() {
    rm -f "$PIPE_FILE"
    echo "清理临时文件完成"
}

# 日志文件管理函数
manage_log_files() {
    echo "管理日志文件..."

    # 删除旧的日志文件，只保留最新的 MAX_LOG_FILES 个
    if ls "$LOG_DIR"/test_output_*.log 1> /dev/null 2>&1; then
        # 按时间排序，删除最旧的文件
        ls -t "$LOG_DIR"/test_output_*.log | tail -n +$(($MAX_LOG_FILES + 1)) | while read old_file; do
            echo "删除旧日志文件: $old_file"
            rm -f "$old_file"
        done
    fi

    echo "当前日志文件:"
    ls -l "$LOG_DIR"/test_output_*.log 2>/dev/null || echo "暂无日志文件"
}

# 突出显示重要信息的函数
highlight_important() {
    while IFS= read -r line; do
        # 输出所有行
        echo "$line"

        # 突出显示以PASS、info、ok开头或以空格开头的行
        if [[ "$line" =~ ^PASS ]] || \
           [[ "$line" =~ ^info ]] || \
           [[ "$line" =~ ^ok ]] || \
           [[ "$line" =~ ^[[:space:]] ]]; then
            echo -e "\033[32m>>> 重要信息: $line\033[0m"
        fi
    done
}

# 设置退出时清理
trap cleanup EXIT

# 显示参数信息
echo "测试模式: $TEST_PATTERN"
echo "日志计数: $LOG_COUNT"
if [[ -n "$RACE_FLAG" ]]; then
    echo "竞态检测: 启用"
    RACE_INFO="（含竞态检测）"
else
    echo "竞态检测: 禁用"
    RACE_INFO=""
fi
echo "测试日志将保存到: $LOG_FILE"

# 管理日志文件（清理旧的）
manage_log_files

# 创建命名管道
if ! mkfifo "$PIPE_FILE"; then
    echo "错误: 无法创建命名管道" >&2
    exit 1
fi

echo "开始测试..."
echo "=== 测试开始于: $(date) $RACE_INFO ===" >> "$LOG_FILE"

# 构建测试命令
TEST_CMD="go test -run \"$TEST_PATTERN\" $RACE_FLAG"

echo "执行命令: $TEST_CMD"
echo "执行命令: $TEST_CMD" >> "$LOG_FILE"

# 运行测试
{
    eval go test -run "$TEST_PATTERN" $RACE_FLAG 2>&1 | tee -a "$LOG_FILE" | highlight_important > "$PIPE_FILE" &
} 2>/dev/null

# 通过python处理实时输出
python3 dslogs.py -c "$LOG_COUNT" < "$PIPE_FILE" &

# 等待所有后台任务完成
wait

echo "=== 测试结束于: $(date) ===" >> "$LOG_FILE"

# 显示日志文件信息
echo -e "\n测试完成！"
echo "详细日志已保存到: $LOG_FILE"
echo -e "\n最近的日志文件:"
ls -lt "$LOG_DIR"/test_output_*.log 2>/dev/null | head -$MAX_LOG_FILES || echo "暂无日志文件"