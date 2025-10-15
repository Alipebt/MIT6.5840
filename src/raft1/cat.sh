#!/bin/bash

# 检查参数
if [ $# -ne 1 ]; then
    echo "用法: $0 <日志文件路径>"
    exit 1
fi

LOG_FILE="$1"

# 检查文件是否存在
if [ ! -f "$LOG_FILE" ]; then
    echo "错误: 文件 '$LOG_FILE' 不存在"
    exit 1
fi

# 执行命令
cat "$LOG_FILE" | head -n 3000| python3 dslogs.py -c 7