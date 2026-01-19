#!/bin/bash

# 1. Create Virtual Env if not exists
if [ ! -d "venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv venv
fi

# 2. Activate
source venv/bin/activate

# 3. Install Deps
echo "⬇️  Installing dependencies..."
pip install -r stratosphere/requirements.txt

# 4. Run Server
echo "🚀 Starting Stratosphere..."
# Fix Import Issue: Change to package directory so 'storage', 'core' are top-level
cd stratosphere

PORT=8000
echo "🔍 Ensuring port $PORT is free..."
PID=$(lsof -ti :$PORT)
if [ ! -z "$PID" ]; then
  echo "⚠️  Port $PORT is in use by PID $PID. Killing it..."
  kill -9 $PID
fi

echo "🔥 Starting Backend on 0.0.0.0:$PORT..."
uvicorn api.main:app --reload --host 0.0.0.0 --port $PORT
