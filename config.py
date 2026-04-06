"""
SNS Performance Pipeline - Configuration
"""
import os
import subprocess
from pathlib import Path
from dotenv import load_dotenv

# プロジェクトルート
PROJECT_ROOT = Path(__file__).parent
load_dotenv(PROJECT_ROOT / ".env")


def _get_secret(key: str, service: str = "sns-analytics") -> str:
    """macOS Keychainから取得 → 環境変数にフォールバック"""
    # 1. 環境変数（GitHub Actions等のCI環境）
    val = os.getenv(key, "")
    if val:
        return val
    # 2. macOS Keychain（ローカル環境）
    try:
        result = subprocess.run(
            ["security", "find-generic-password", "-s", service, "-a", key, "-w"],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return ""


# === Database ===
DB_PATH = PROJECT_ROOT / "data" / "sns_performance.db"

# === Instagram Graph API ===
IG_APP_ID = _get_secret("IG_APP_ID")
IG_APP_SECRET = _get_secret("IG_APP_SECRET")
IG_ACCESS_TOKEN = _get_secret("IG_ACCESS_TOKEN")
IG_BUSINESS_ACCOUNT_ID = _get_secret("IG_BUSINESS_ACCOUNT_ID")
IG_PAGE_ID = _get_secret("IG_PAGE_ID")

# === Meta Ads (Marketing API) ===
META_AD_ACCOUNT_ID = _get_secret("META_AD_ACCOUNT_ID")

# === Threads API ===
THREADS_APP_ID = _get_secret("THREADS_APP_ID")
THREADS_APP_SECRET = _get_secret("THREADS_APP_SECRET")
THREADS_ACCESS_TOKEN = _get_secret("THREADS_ACCESS_TOKEN")

# === SocialData API (X/Twitter) ===
SOCIALDATA_API_KEY = _get_secret("SOCIALDATA_API_KEY")

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

