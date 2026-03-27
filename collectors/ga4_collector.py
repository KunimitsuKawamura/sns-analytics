"""
GA4 Data API Collector (MCP経由)
SNS流入のセッション・CVデータを取得

NOTE: GA4 Data API はMCPサーバー経由でアクセス可能。
      このモジュールは直接API呼び出しとMCP結果のインポートの
      両方に対応。
"""
import json
from datetime import datetime, date, timedelta
from config import UTM_SOURCE_MAP

# GA4 MCPサーバー経由のデータを処理するための関数群


def parse_mcp_report(report_data: dict) -> list[dict]:
    """MCP runReport の結果をフラットな辞書リストに変換"""
    rows = report_data.get("rows", [])
    dim_headers = [h["name"] for h in report_data.get("dimensionHeaders", [])]
    metric_headers = [h["name"] for h in report_data.get("metricHeaders", [])]

    results = []
    for row in rows:
        record = {}
        for i, dim in enumerate(row.get("dimensionValues", [])):
            record[dim_headers[i]] = dim.get("value", "")
        for i, met in enumerate(row.get("metricValues", [])):
            val = met.get("value", "0")
            # 数値変換
            try:
                record[metric_headers[i]] = float(val) if "." in val else int(val)
            except ValueError:
                record[metric_headers[i]] = val
        results.append(record)
    return results


def filter_sns_sessions(records: list[dict]) -> list[dict]:
    """SNS関連ソースのみにフィルタリング"""
    all_sources = set()
    for sources in UTM_SOURCE_MAP.values():
        all_sources.update(s.lower() for s in sources)
    # socialも追加
    all_sources.add("social")

    return [
        r for r in records
        if (r.get("sessionSource", "").lower() in all_sources
            or r.get("sessionMedium", "").lower() == "social"
            or r.get("sessionDefaultChannelGroup", "").lower() == "organic social")
    ]


def map_source_to_platform(source: str, medium: str = "") -> str:
    """GA4ソース名をプラットフォーム名に変換"""
    source_lower = source.lower()
    for platform, sources in UTM_SOURCE_MAP.items():
        if source_lower in [s.lower() for s in sources]:
            return platform
    return "other_social"


def to_db_records(records: list[dict]) -> list[dict]:
    """GA4レコードをDB格納用に変換"""
    db_records = []
    for r in records:
        db_records.append({
            "date": _format_date(r.get("date", "")),
            "source": r.get("sessionSource", ""),
            "medium": r.get("sessionMedium", ""),
            "campaign": r.get("sessionCampaignName", "(not set)"),
            "content": r.get("sessionManualAdContent", "(not set)"),
            "landing_page": r.get("landingPage", ""),
            "sessions": r.get("sessions", 0),
            "engaged_sessions": r.get("engagedSessions", 0),
            "conversions": r.get("conversions", 0),
            "avg_session_duration": r.get("averageSessionDuration", 0),
        })
    return db_records


def _format_date(date_str: str) -> str:
    """GA4形式の日付(YYYYMMDD)をISO形式に変換"""
    if len(date_str) == 8:
        return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    return date_str


def get_sns_query_params(start_date: str, end_date: str) -> dict:
    """MCP runReport用のクエリパラメータを生成"""
    return {
        "startDate": start_date,
        "endDate": end_date,
        "dimensions": [
            {"name": "date"},
            {"name": "sessionSource"},
            {"name": "sessionMedium"},
            {"name": "sessionCampaignName"},
            {"name": "sessionManualAdContent"},
            {"name": "landingPage"},
        ],
        "metrics": [
            {"name": "sessions"},
            {"name": "engagedSessions"},
            {"name": "conversions"},
            {"name": "averageSessionDuration"},
        ],
    }


if __name__ == "__main__":
    print("GA4 Collector: MCP経由でデータ取得します")
    print("使用方法: run_collection.py 内で MCP runReport を呼び出し、")
    print("          parse_mcp_report() でパース → filter_sns_sessions() でフィルタ")
