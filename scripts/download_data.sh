#!/usr/bin/env bash
# Tải bộ Olist từ Kaggle vào data/raw. Cần: pip install kaggle và file ~/.kaggle/kaggle.json
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p data/raw
if ! command -v kaggle >/dev/null 2>&1; then
  echo "Chưa có Kaggle CLI. Chạy: pip install kaggle"
  echo "Hoặc tải tay tại https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce rồi giải nén vào data/raw/"
  exit 1
fi
kaggle datasets download -d olistbr/brazilian-ecommerce -p data/raw --unzip
ls -lh data/raw
