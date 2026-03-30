"""
前週比較分析エンジン
今週 vs 前週のKPI差分を算出し、トレンド変化を可視化
"""
import sqlite3
from datetime import date, timedelta
from collections import defaultdict
from config import DB_PATH


def get_connection():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def _week_range(offset: int = 0):
    """月曜始まりの週範囲を取得。offset=0で直近完了週、offset=-1でその前週。
    
    パイプラインが月曜朝に実行されるため、「今週」はまだデータがない。
    そのため実質的な比較は「先週 vs 先々週」となるよう、
    今週（月曜当日）はまだ始まったばかりなので1週ずらす。
    """
    today = date.today()
    monday = today - timedelta(days=today.weekday())

    # 月曜日の場合、今週はまだデータがないため1週前にずらす
    if today.weekday() == 0:
        monday = monday - timedelta(weeks=1)

    start = monday + timedelta(weeks=offset)
    end = start + timedelta(days=6)
    return start.isoformat(), end.isoformat()


def compare_platform_kpi() -> dict:
    """プラットフォーム別KPIの今週 vs 前週比較"""
    conn = get_connection()
    this_start, this_end = _week_range(0)
    prev_start, prev_end = _week_range(-1)

    results = {}
    for platform in ["instagram", "threads", "x"]:
        this_week = _fetch_period_kpi(conn, platform, this_start, this_end)
        prev_week = _fetch_period_kpi(conn, platform, prev_start, prev_end)
        results[platform] = {
            "this_week": this_week,
            "prev_week": prev_week,
            "diff": _calc_diff(this_week, prev_week),
        }

    conn.close()
    return results


def compare_ga4_traffic() -> dict:
    """GA4 SNS流入の今週 vs 前週比較"""
    conn = get_connection()
    this_start, this_end = _week_range(0)
    prev_start, prev_end = _week_range(-1)

    results = {}
    for platform in ["instagram", "threads", "x"]:
        this_week = _fetch_ga4_period(conn, platform, this_start, this_end)
        prev_week = _fetch_ga4_period(conn, platform, prev_start, prev_end)
        results[platform] = {
            "this_week": this_week,
            "prev_week": prev_week,
            "diff": _calc_diff(this_week, prev_week),
        }

    conn.close()
    return results


def compare_engagement_trend() -> dict:
    """直近4週間のエンゲージメント推移"""
    conn = get_connection()
    weeks = []
    for offset in range(-3, 1):  # -3, -2, -1, 0
        start, end = _week_range(offset)
        week_data = {}
        for platform in ["instagram", "threads", "x"]:
            kpi = _fetch_period_kpi(conn, platform, start, end)
            week_data[platform] = kpi
        week_data["label"] = f"{start[5:]}"  # MM-DD
        week_data["start"] = start
        week_data["end"] = end
        weeks.append(week_data)
    conn.close()
    return {"weeks": weeks}


def generate_comparison_insights(platform_comp: dict, ga4_comp: dict) -> list[str]:
    """前週比較からインサイトを自動生成"""
    insights = []

    for platform, data in platform_comp.items():
        diff = data["diff"]
        tw = data["this_week"]
        pw = data["prev_week"]

        # 投稿数変化
        if diff.get("posts_diff", 0) != 0:
            direction = "増加↑" if diff["posts_diff"] > 0 else "減少↓"
            insights.append(
                f"📊 {platform.upper()}: 今週の投稿数 {tw['posts']}件 "
                f"（前週比 {diff['posts_diff']:+d}件 {direction}）"
            )

        # いいね数変化
        if diff.get("likes_pct", 0) != 0 and pw.get("likes", 0) > 0:
            emoji = "🔥" if diff["likes_pct"] > 20 else ("⚠️" if diff["likes_pct"] < -20 else "📈")
            insights.append(
                f"{emoji} {platform.upper()}: いいね数 {tw['likes']}→{tw['likes']} "
                f"（前週比 {diff['likes_pct']:+.1f}%）"
            )

    # GA4流入変化
    for platform, data in ga4_comp.items():
        tw = data["this_week"]
        pw = data["prev_week"]
        diff = data["diff"]
        if (tw.get("sessions", 0) > 0 or pw.get("sessions", 0) > 0) and diff.get("sessions_pct", 0) != 0:
            emoji = "🔗" if diff["sessions_pct"] > 0 else "📉"
            insights.append(
                f"{emoji} {platform.upper()}: サイト流入 {pw.get('sessions', 0)}→{tw.get('sessions', 0)}sessions "
                f"（{diff['sessions_pct']:+.1f}%）"
            )

    return insights


def _fetch_period_kpi(conn, platform: str, start: str, end: str) -> dict:
    """期間内のKPIを取得"""
    row = conn.execute("""
        SELECT 
            COUNT(DISTINCT p.id) as posts,
            COALESCE(SUM(m.views), 0) as views,
            COALESCE(SUM(m.likes), 0) as likes,
            COALESCE(SUM(m.replies), 0) as replies,
            COALESCE(SUM(m.saves), 0) as saves,
            COALESCE(SUM(m.reposts), 0) as reposts,
            ROUND(AVG(CASE WHEN m.engagement_rate IS NOT NULL THEN m.engagement_rate END), 2) as avg_eng_rate
        FROM posts p
        LEFT JOIN post_metrics m ON p.id = m.post_id
        WHERE p.platform = ? AND p.posted_at >= ? AND p.posted_at < ?
    """, (platform, start, end + "T23:59:59")).fetchone()
    return dict(row) if row else {
        "posts": 0, "views": 0, "likes": 0, "replies": 0,
        "saves": 0, "reposts": 0, "avg_eng_rate": 0,
    }


def _fetch_ga4_period(conn, platform: str, start: str, end: str) -> dict:
    """期間内のGA4流入を取得"""
    source_map = {
        "instagram": ("instagram", "ig", "l.instagram.com"),
        "threads": ("threads", "threads.net"),
        "x": ("x", "twitter", "t.co", "x.com"),
    }
    sources = source_map.get(platform, (platform,))
    placeholders = ",".join("?" * len(sources))
    row = conn.execute(f"""
        SELECT 
            COALESCE(SUM(sessions), 0) as sessions,
            COALESCE(SUM(engaged_sessions), 0) as engaged,
            COALESCE(SUM(conversions), 0) as conversions,
            ROUND(AVG(avg_session_duration), 1) as avg_duration
        FROM ga4_sessions
        WHERE LOWER(source) IN ({placeholders}) AND date >= ? AND date <= ?
    """, (*[s.lower() for s in sources], start, end)).fetchone()
    return dict(row) if row else {"sessions": 0, "engaged": 0, "conversions": 0, "avg_duration": 0}


def _calc_diff(this_week: dict, prev_week: dict) -> dict:
    """差分・変化率を計算"""
    diff = {}
    for key in ["posts", "views", "likes", "replies", "saves", "reposts", "sessions", "engaged", "conversions"]:
        tw_val = this_week.get(key, 0) or 0
        pw_val = prev_week.get(key, 0) or 0
        diff[f"{key}_diff"] = tw_val - pw_val
        if pw_val > 0:
            diff[f"{key}_pct"] = round((tw_val - pw_val) / pw_val * 100, 1)
        elif tw_val > 0:
            diff[f"{key}_pct"] = 100.0
        else:
            diff[f"{key}_pct"] = 0.0
    return diff


def run_comparison_analysis() -> dict:
    """前週比較分析を実行"""
    print("📊 前週比較分析実行中...")

    platform_comp = compare_platform_kpi()
    ga4_comp = compare_ga4_traffic()
    trend = compare_engagement_trend()
    insights = generate_comparison_insights(platform_comp, ga4_comp)

    this_start, this_end = _week_range(0)
    prev_start, prev_end = _week_range(-1)

    results = {
        "this_week_range": f"{this_start} ~ {this_end}",
        "prev_week_range": f"{prev_start} ~ {prev_end}",
        "platform_comparison": platform_comp,
        "ga4_comparison": ga4_comp,
        "engagement_trend": trend,
        "comparison_insights": insights,
    }

    print(f"✅ 前週比較完了: {len(insights)}件のインサイト")
    return results


if __name__ == "__main__":
    import json, sys
    sys.path.insert(0, ".")
    results = run_comparison_analysis()

    print(f"\n期間: {results['this_week_range']} vs {results['prev_week_range']}")
    for p, d in results["platform_comparison"].items():
        tw, pw = d["this_week"], d["prev_week"]
        diff = d["diff"]
        print(f"\n  {p.upper()}:")
        print(f"    投稿: {pw['posts']}→{tw['posts']} ({diff.get('posts_diff', 0):+d})")
        print(f"    Like: {pw['likes']}→{tw['likes']} ({diff.get('likes_pct', 0):+.1f}%)")

    print("\nインサイト:")
    for ins in results["comparison_insights"]:
        print(f"  {ins}")
