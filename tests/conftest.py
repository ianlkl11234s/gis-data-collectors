"""pytest 共用 fixture 與 path 設定"""

import sys
from pathlib import Path

# 讓 tests/ 內的 import 能找到專案根目錄的模組
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
