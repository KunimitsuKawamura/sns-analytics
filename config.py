"""
SNS Performance Pipeline - Configuration
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# プロジェクトルート
PROJECT_ROOT = Path(__file__).parent
load_dotenv(PROJECT_ROOT / ".env")

# === Database ===
DB_PATH = PROJECT_ROOT / "data" / "sns_performance.db"

# === Instagram Graph API ===
IG_APP_ID = os.getenv("IG_APP_ID", "")
IG_APP_SECRET = os.getenv("IG_APP_SECRET", "")
IG_ACCESS_TOKEN = os.getenv("IG_ACCESS_TOKEN", "")
IG_BUSINESS_ACCOUNT_ID = os.getenv("IG_BUSINESS_ACCOUNT_ID", "")
IG_PAGE_ID = os.getenv("IG_PAGE_ID", "")

# === Threads API ===
THREADS_APP_ID = os.getenv("THREADS_APP_ID", "")
THREADS_APP_SECRET = os.getenv("THREADS_APP_SECRET", "")
THREADS_ACCESS_TOKEN = os.getenv("THREADS_ACCESS_TOKEN", "")

# === SocialData API (X/Twitter) ===
SOCIALDATA_API_KEY = os.getenv("SOCIALDATA_API_KEY", "")

# === GA4 ===
GA4_PROPERTY_ID = os.getenv("GA4_PROPERTY_ID", "")
GA4_SERVICE_ACCOUNT_KEY_PATH = os.getenv("GA4_SERVICE_ACCOUNT_KEY_PATH", "")

# === 自社アカウント設定 ===
ACCOUNTS = {
    "x": {
        "screen_name": "info_meetcareer",
        "display_name": "ミートキャリア",
    },
    "instagram": {
        "username": "meetcareer_official",
        "business_account_id": IG_BUSINESS_ACCOUNT_ID,
    },
    "threads": {
        "username": "meetcareer_official",
    },
}

# === UTMソースマッピング ===
UTM_SOURCE_MAP = {
    "x": ["t.co", "x.com", "twitter.com", "x"],
    "instagram": ["instagram", "ig", "l.instagram.com"],
    "threads": ["threads", "threads.net"],
}

# === API設定 ===
GRAPH_API_VERSION = "v25.0"
GRAPH_API_BASE = f"https://graph.facebook.com/{GRAPH_API_VERSION}"
THREADS_API_BASE = "https://graph.threads.net/v1.0"
SOCIALDATA_API_BASE = "https://api.socialdata.tools"

# === レポート設定 ===
REPORT_OUTPUT_DIR = PROJECT_ROOT / "output" / "reports"

# === 担当者設定 ===
# 各担当者が管理するプラットフォームを定義
# 必要に応じてカスタマイズしてください
STAFF_ASSIGNMENTS = {
    "SNS担当A": {
        "platforms": ["instagram", "threads"],
        "role": "IG・Threads運用担当",
    },
    "SNS担当B": {
        "platforms": ["x"],
        "role": "X（Twitter）運用担当",
    },
}

