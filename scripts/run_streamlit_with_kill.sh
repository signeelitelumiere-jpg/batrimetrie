#!/usr/bin/env bash
# Kill any process listening on a port then launch Streamlit
# Usage: ./run_streamlit_with_kill.sh [PORT]
PORT=${1:-8501}
PYTHON=${PYTHON:-"environment/python.exe"}
set -e

echo "Checking port $PORT..."
PIDS=""
if command -v lsof >/dev/null 2>&1; then
  PIDS=$(lsof -ti tcp:$PORT)
elif command -v ss >/dev/null 2>&1; then
  PIDS=$(ss -ltnp "sport = :$PORT" 2>/dev/null | awk '{print $6}' | sed -n 's/.*pid=\([0-9]\+\).*/\1/p' | sort -u)
else
  PIDS=$(netstat -ano 2>/dev/null | grep -E ":$PORT\\s" | awk '{print $NF}' | sort -u)
fi

if [ -n "$PIDS" ]; then
  echo "Killing PIDs: $PIDS"
  for pid in $PIDS; do
    if [ -n "$pid" ]; then
      if command -v taskkill >/dev/null 2>&1; then
        taskkill /PID $pid /F >/dev/null 2>&1 || kill -9 $pid >/dev/null 2>&1 || true
      else
        kill -9 $pid >/dev/null 2>&1 || true
      fi
    fi
  done
else
  echo "No process found on port $PORT"
fi

echo "Starting Streamlit on port $PORT..."
"$PYTHON" -m streamlit run app.py --server.port $PORT
