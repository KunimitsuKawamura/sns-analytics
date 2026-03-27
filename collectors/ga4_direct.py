"""
GA4 Data API 直接呼び出しコレクター
google-analytics-data ライブラリを使用してサービスアカウント認証で
GA4データを直接取得する。MCP不要。

GitHub Actions環境ではサービスアカウントキーをSecret経由で取得。
"""
import json
import os
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import GA4_PROPERTY_ID, GA4_SERVICE_ACCOUNT_KEY_PATH, UTM_SOURCE_MAP
from collectors.ga4_collector import filter_sns_sessions, to_db_records


def _get_credentials():
    """サービスアカウント認証情報を取得"""
    from google.oauth2 import service_account

    # 1. 環境変数にJSON文字列がある場合（GitHub Actions）
    key_json = os.getenv("GA4_SERVICE_ACCOUNT_KEY_JSON", "")
    if key_json:
        info = json.loads(key_json)
        return service_account.Credentials.from_service_account_info(
            info,
            scopes=["https://www.googleapis.com/auth/analytics.readonly"],
        )

    # 2. ファイルパスが指定されている場合（ローカル）
    if GA4_SERVICE_ACCOUNT_KEY_PATH and Path(GA4_SERVICE_ACCOUNT_KEY_PATH).exists():
        return service_account.Credentials.from_service_account_file(
            GA4_SERVICE_ACCOUNT_KEY_PATH,
            scopes=["https://www.googleapis.com/auth/analytics.readonly"],
        )

    return None


def fetch_sns_sessions(days: int = 30) -> list[dict]:
    """
    GA4 Data APIから直接SNS流入データを取得

    Returns:
        DB格納用のレコードリスト
    """
    from google.analytics.data_v1beta import BetaAnalyticsDataClient
    from google.analytics.data_v1beta.types import (
        RunReportRequest,
        Dimension,
        Metric,
        DateRange,
    )

    if not GA4_PROPERTY_ID:
        print("   ⚠️ GA4_PROPERTY_ID が未設定")
        return []

    credentials = _get_credentials()
    if credentials is None:
        print("   ⚠️ GA4サービスアカウントキーが見つかりません")
        return []

    client = BetaAnalyticsDataClient(credentials=credentials)

    end = date.today()
    start = end - timedelta(days=days)

    request = RunReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        dimensions=[
            Dimension(name="date"),
            Dimension(name="sessionSource"),
            Dimension(name="sessionMedium"),
            Dimension(name="sessionCampaignName"),
            Dimension(name="sessionManualAdContent"),
            Dimension(name="landingPage"),
        ],
        metrics=[
            Metric(name="sessions"),
            Metric(name="engagedSessions"),
            Metric(name="conversions"),
            Metric(name="averageSessionDuration"),
        ],
        date_ranges=[DateRange(start_date=start.isoformat(), end_date=end.isoformat())],
    )

    response = client.run_report(request)

    # response→dict変換
    records = []
    dim_names = [h.name for h in response.dimension_headers]
    met_names = [h.name for h in response.metric_headers]

    for row in response.rows:
        record = {}
        for i, dv in enumerate(row.dimension_values):
            record[dim_names[i]] = dv.value
        for i, mv in enumerate(row.metric_values):
            val = mv.value
            try:
                record[met_names[i]] = float(val) if "." in val else int(val)
            except ValueError:
                record[met_names[i]] = val
        records.append(record)

    # SNSフィルタ＋DB形式変換
    sns_records = filter_sns_sessions(records)
    return to_db_records(sns_records)


def is_available() -> bool:
    """GA4直接API呼び出しが利用可能か判定"""
    if not GA4_PROPERTY_ID:
        return False
    return _get_credentials() is not None


if __name__ == "__main__":
    if is_available():
        records = fetch_sns_sessions()
        print(f"✅ {len(records)}件のSNSセッションを取得")
        for r in records[:5]:
            print(f"  {r['date']} | {r['source']} | {r['sessions']}セッション")
    else:
        print("⚠️ GA4直接APIは利用不可（Property ID または サービスアカウントキーが未設定）")
