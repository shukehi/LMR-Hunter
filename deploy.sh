#!/bin/bash
# 将本地 src/ 同步到 VPS 并可选地重启服务
# 用法:
#   ./deploy.sh          # 只同步代码
#   ./deploy.sh restart  # 同步后重启 observer

set -e

REMOTE="quant-vps"
REMOTE_DIR="/opt/lmr-hunter"

echo "→ 同步代码到 $REMOTE..."
rsync -avz --delete \
  -e "ssh -o RemoteCommand=none -o RequestTTY=no" \
  --exclude '__pycache__' \
  --exclude '*.pyc' \
  --exclude '.DS_Store' \
  src/ "$REMOTE:$REMOTE_DIR/src/"

# 同步配置文件（如果存在）
if [ -f config/.env ]; then
  rsync -avz -e "ssh -o RemoteCommand=none -o RequestTTY=no" \
    config/.env "$REMOTE:$REMOTE_DIR/config/.env"
fi

echo "✓ 同步完成"

if [ "$1" = "restart" ]; then
  echo "→ 重启 observer..."
  ssh -o RequestTTY=no -o RemoteCommand=none "$REMOTE" \
    "cd $REMOTE_DIR && tmux send-keys -t lmr 'C-c' '' 2>/dev/null; sleep 1; tmux send-keys -t lmr '.venv/bin/python -m src.main' Enter"
  echo "✓ 已重启"
fi
