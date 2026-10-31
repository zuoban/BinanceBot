#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

for command in cargo node npm; do
  if ! command -v "$command" >/dev/null 2>&1; then
    printf '缺少命令：%s，请先安装所需的开发环境。\n' "$command" >&2
    exit 1
  fi
done

if [[ ! -d "$ROOT_DIR/frontend/node_modules" ]]; then
  printf '正在安装前端依赖...\n'
  (cd "$ROOT_DIR/frontend" && npm ci)
fi

# 独立进程组确保 npm 启动的 Vite 等子进程也能收到退出信号。
set -m
backend_pid=""
frontend_pid=""
cleanup() {
  trap - EXIT INT TERM
  for pid in "$backend_pid" "$frontend_pid"; do
    if [[ -n "$pid" ]]; then
      kill -TERM -- "-$pid" 2>/dev/null || true
    fi
  done
  for pid in "$backend_pid" "$frontend_pid"; do
    if [[ -n "$pid" ]]; then
      wait "$pid" 2>/dev/null || true
    fi
  done
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

printf '正在启动后端；首次启动的管理员初始化码会显示在后端日志中。\n'
(cd "$ROOT_DIR" && exec cargo run --release -- "$@") &
backend_pid=$!
backend_port=""
args=("$@")
for ((i = 0; i < ${#args[@]}; i++)); do
  case "${args[i]}" in
    --port|-p) if ((i + 1 < ${#args[@]})); then backend_port="${args[i+1]}"; fi ;;
    --port=*) backend_port="${args[i]#--port=}" ;;
    -p?*) backend_port="${args[i]#-p}" ;;
  esac
done
if [[ -n "$backend_port" && -z "${API_PROXY_TARGET:-}" ]]; then
  export API_PROXY_TARGET="http://127.0.0.1:$backend_port"
fi
printf '正在启动前端，请访问 Vite 输出的地址。按 Ctrl+C 停止。\n'
(cd "$ROOT_DIR/frontend" && exec npm run dev) &
frontend_pid=$!

while kill -0 "$backend_pid" 2>/dev/null && kill -0 "$frontend_pid" 2>/dev/null; do
  sleep 1
done

if ! kill -0 "$backend_pid" 2>/dev/null; then
  wait "$backend_pid" || exit $?
  printf '后端已退出，正在停止前端。\n'
else
  wait "$frontend_pid" || exit $?
  printf '前端已退出，正在停止后端。\n'
fi
