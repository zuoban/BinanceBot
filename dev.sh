#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

for command in cargo node npm; do
  if ! command -v "$command" >/dev/null 2>&1; then
    printf '缺少命令：%s，请先安装所需的开发环境。\n' "$command" >&2
    exit 1
  fi
done

has_password_arg=false
backend_host=""
backend_port=""
args=("$@")
for ((i = 0; i < ${#args[@]}; i++)); do
  case "${args[i]}" in
    --)
      break
      ;;
    --admin-password)
      if ((i + 1 < ${#args[@]})) && [[ "${args[i+1]}" != -* ]]; then
        if [[ -n "${args[i+1]}" ]]; then has_password_arg=true; fi
        i=$((i + 1))
      fi
      ;;
    --admin-password=?*) has_password_arg=true ;;
    --host)
      if ((i + 1 < ${#args[@]})) && [[ "${args[i+1]}" != -* ]]; then
        backend_host="${args[i+1]}"
        i=$((i + 1))
      fi
      ;;
    --host=*) backend_host="${args[i]#--host=}" ;;
    --port|-p)
      if ((i + 1 < ${#args[@]})) && [[ "${args[i+1]}" != -* ]]; then
        backend_port="${args[i+1]}"
        i=$((i + 1))
      fi
      ;;
    --port=*) backend_port="${args[i]#--port=}" ;;
    -p=*) backend_port="${args[i]#-p=}" ;;
    -p?*) backend_port="${args[i]#-p}" ;;
    --db|-d)
      if ((i + 1 < ${#args[@]})) && [[ "${args[i+1]}" != -* ]]; then
        i=$((i + 1))
      fi
      ;;
  esac
done
dev_host="${DEV_HOST:-127.0.0.1}"
is_loopback_ip() {
  case "$1" in
    ""|127.0.0.1|::1|"[::1]") return 0 ;;
    *) return 1 ;;
  esac
}
extra_backend_args=()
if [[ -z "${ADMIN_PASSWORD:-}" && "$has_password_arg" == false ]]; then
  if { [[ "$dev_host" != "localhost" ]] && ! is_loopback_ip "$dev_host"; } || ! is_loopback_ip "$backend_host"; then
    printf '拒绝在未设置 ADMIN_PASSWORD（或 --admin-password）时绑定非回环地址（DEV_HOST=%s, --host=%s）。\n' "$dev_host" "${backend_host:-127.0.0.1}" >&2
    exit 1
  fi
  if [[ -z "$backend_host" ]]; then
    extra_backend_args=(--host 127.0.0.1)
  fi
  printf '未设置 ADMIN_PASSWORD，本次强制绑定本机回环（127.0.0.1）访问。\n'
fi
if [[ ! -d "$ROOT_DIR/frontend/node_modules" ]]; then
  printf '正在安装前端依赖...\n'
  (cd "$ROOT_DIR/frontend" && npm ci --ignore-scripts)
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

printf '正在启动后端；管理员认证由 ADMIN_PASSWORD 或 --admin-password 决定。\n'
(cd "$ROOT_DIR" && exec cargo run --release -- ${extra_backend_args[@]+"${extra_backend_args[@]}"} "$@") &
backend_pid=$!
if [[ -z "${API_PROXY_TARGET:-}" && ( -n "$backend_host" || -n "$backend_port" ) ]]; then
  proxy_host="${backend_host:-127.0.0.1}"
  case "$proxy_host" in
    0.0.0.0) proxy_host="127.0.0.1" ;;
    ::|"[::]"|::1|"[::1]") proxy_host="[::1]" ;;
    *:*)
      if [[ "$proxy_host" != \[*\] ]]; then
        proxy_host="[$proxy_host]"
      fi
      ;;
  esac
  export API_PROXY_TARGET="http://${proxy_host}:${backend_port:-8080}"
fi
printf '正在启动前端，请访问 Vite 输出的地址。按 Ctrl+C 停止。\n'
(cd "$ROOT_DIR/frontend" && exec npm run dev -- --host "$dev_host") &
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
