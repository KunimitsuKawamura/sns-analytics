"""
CV貢献分析エンジン
投稿パフォーマンス × GA4流入 × CV相関を分析
"""
import sqlite3
from datetime import datetime, date, timedelta
from collections import defaultdict
from config import DB_PATH, UTM_SOURCE_MAP


def get_connection():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def analyze_platform_summary(days: int = 30) -> dict:
    """プラットフォーム別サマリーを生成"""
    conn = get_connection()
    cutoff = (date.today() - timedelta(days=days)).isoformat()

    results = {}
    for platform in ["instagram", "threads", "x"]:
        row = conn.execute("""
            SELECT 
                COUNT(DISTINCT p.id) as total_posts,
                COALESCE(SUM(m.views), 0) as total_views,
                COALESCE(SUM(m.likes), 0) as total_likes,
                COALESCE(SUM(m.replies), 0) as total_replies,
                COALESCE(SUM(m.reposts), 0) as total_reposts,
                COALESCE(SUM(m.saves), 0) as total_saves,
                ROUND(AVG(m.engagement_rate), 2) as avg_engagement_rate
            FROM posts p
            LEFT JOIN post_metrics m ON p.id = m.post_id
            WHERE p.platform = ? AND p.posted_at >= ?
        """, (platform, cutoff)).fetchone()

        results[platform] = dict(row) if row else {}

    conn.close()
    return results


def analyze_post_type_performance() -> list[dict]:
    """投稿タイプ別パフォーマンスを分析"""
    conn = get_connection()
    rows = conn.execute("""
        SELECT 
            p.platform,
            p.post_type,
            COUNT(*) as count,
            ROUND(AVG(m.views), 0) as avg_views,
            ROUND(AVG(m.likes), 1) as avg_likes,
            ROUND(AVG(m.replies), 1) as avg_replies,
            ROUND(AVG(m.saves), 1) as avg_saves,
            ROUND(AVG(m.engagement_rate), 2) as avg_eng_rate
        FROM posts p
        LEFT JOIN post_metrics m ON p.id = m.post_id
        GROUP BY p.platform, p.post_type
        ORDER BY avg_eng_rate DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def analyze_ga4_traffic_by_platform() -> dict:
    """GA4 SNS流入をプラットフォーム別に集計"""
    conn = get_connection()
    rows = conn.execute("""
        SELECT 
            source,
            SUM(sessions) as total_sessions,
            SUM(engaged_sessions) as total_engaged,
            SUM(conversions) as total_conversions,
            ROUND(AVG(avg_session_duration), 1) as avg_duration,
            ROUND(CAST(SUM(engaged_sessions) AS FLOAT) / NULLIF(SUM(sessions), 0) * 100, 1) as engagement_rate
        FROM ga4_sessions
        GROUP BY source
        ORDER BY total_sessions DESC
    """).fetchall()
    conn.close()

    # ソースをプラットフォームに変換して集約
    platform_data = defaultdict(lambda: {
        "sessions": 0, "engaged": 0, "conversions": 0,
        "avg_duration": 0, "sources": [],
    })

    for row in rows:
        row = dict(row)
        platform = _map_source(row["source"])
        pd = platform_data[platform]
        pd["sessions"] += row["total_sessions"]
        pd["engaged"] += row["total_engaged"]
        pd["conversions"] += row["total_conversions"]
        pd["sources"].append(row["source"])

    # エンゲージメント率計算
    for p, d in platform_data.items():
        d["engagement_rate"] = round(d["engaged"] / max(d["sessions"], 1) * 100, 1)

    return dict(platform_data)


def analyze_top_landing_pages() -> list[dict]:
    """SNS流入のトップLPを分析"""
    conn = get_connection()
    rows = conn.execute("""
        SELECT 
            landing_page,
            SUM(sessions) as total_sessions,
            SUM(engaged_sessions) as total_engaged,
            SUM(conversions) as total_conversions,
            ROUND(AVG(avg_session_duration), 1) as avg_duration
        FROM ga4_sessions
        GROUP BY landing_page
        ORDER BY total_sessions DESC
        LIMIT 10
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def analyze_utm_campaigns() -> list[dict]:
    """UTMキャンペーン別のパフォーマンスを分析"""
    conn = get_connection()
    rows = conn.execute("""
        SELECT 
            campaign,
            source,
            content,
            SUM(sessions) as total_sessions,
            SUM(engaged_sessions) as total_engaged,
            SUM(conversions) as total_conversions,
            ROUND(AVG(avg_session_duration), 1) as avg_duration
        FROM ga4_sessions
        WHERE campaign != '(referral)' AND campaign != '(not set)'
        GROUP BY campaign, source, content
        ORDER BY total_sessions DESC
        LIMIT 15
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def analyze_top_posts(limit: int = 10) -> list[dict]:
    """エンゲージメントが高い投稿 TOP N"""
    conn = get_connection()
    rows = conn.execute("""
        SELECT 
            p.platform,
            p.post_type,
            SUBSTR(p.content, 1, 80) as content_preview,
            p.permalink,
            p.posted_at,
            m.views,
            m.likes,
            m.replies,
            m.reposts,
            m.saves,
            m.engagement_rate
        FROM posts p
        JOIN post_metrics m ON p.id = m.post_id
        WHERE m.views > 0 OR m.likes > 0
        ORDER BY (m.likes + m.replies + m.reposts + m.saves) DESC
        LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def analyze_daily_trend(days: int = 30) -> list[dict]:
    """日別のSNS流入トレンド"""
    conn = get_connection()
    rows = conn.execute("""
        SELECT 
            date,
            SUM(sessions) as total_sessions,
            SUM(engaged_sessions) as total_engaged,
            SUM(conversions) as total_conversions
        FROM ga4_sessions
        GROUP BY date
        ORDER BY date
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def analyze_posting_time_performance() -> list[dict]:
    """投稿時間帯別パフォーマンス"""
    conn = get_connection()
    rows = conn.execute("""
        SELECT 
            CAST(SUBSTR(p.posted_at, 12, 2) AS INTEGER) as hour,
            p.platform,
            COUNT(*) as post_count,
            ROUND(AVG(m.likes), 1) as avg_likes,
            ROUND(AVG(m.views), 0) as avg_views,
            ROUND(AVG(m.engagement_rate), 2) as avg_eng_rate
        FROM posts p
        JOIN post_metrics m ON p.id = m.post_id
        WHERE p.posted_at IS NOT NULL AND LENGTH(p.posted_at) >= 13
        GROUP BY hour, p.platform
        HAVING post_count >= 2
        ORDER BY avg_eng_rate DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def generate_insights(summary: dict, ga4_traffic: dict, 
                      top_posts: list, post_types: list) -> list[str]:
    """分析結果からアクショナブルなインサイトを自動生成"""
    insights = []

    # 1. プラットフォーム比較
    if summary:
        best_platform = max(summary.items(),
                           key=lambda x: x[1].get("avg_engagement_rate") or 0)
        rate = best_platform[1].get("avg_engagement_rate") or 0
        if rate > 0:
            insights.append(
                f"📊 エンゲージメント率が最も高いプラットフォームは "
                f"**{best_platform[0].upper()}**（平均 {best_platform[1]['avg_engagement_rate']}%）です。"
            )

    # 2. 投稿タイプ分析
    if post_types:
        best_type = max(post_types, key=lambda x: x.get("avg_eng_rate") or 0)
        if (best_type.get("avg_eng_rate") or 0) > 0:
            insights.append(
                f"💡 最も反応が良い投稿タイプは **{best_type['platform'].upper()} の "
                f"{best_type['post_type']}**（平均エンゲージメント率 {best_type['avg_eng_rate']}%）。"
                f"このタイプを増やすことを推奨します。"
            )

    # 3. GA4流入分析
    if ga4_traffic:
        best_source = max(ga4_traffic.items(), key=lambda x: x[1]["sessions"])
        insights.append(
            f"🔗 サイト流入が最も多いSNSは "
            f"**{best_source[0]}**（{best_source[1]['sessions']}セッション、"
            f"エンゲージメント率 {best_source[1]['engagement_rate']}%）。"
        )

    # 4. コンバージョン分析
    total_cv = sum(d.get("conversions", 0) for d in ga4_traffic.values())
    total_sessions = sum(d.get("sessions", 0) for d in ga4_traffic.values())
    if total_cv > 0:
        insights.append(
            f"🎯 SNS流入経由のCV: {int(total_cv)}件 "
            f"（CVR {round(total_cv/max(total_sessions,1)*100, 2)}%）"
        )
    else:
        insights.append(
            f"⚠️ 過去30日間のSNS経由CVは0件。UTMパラメータ付きリンクの"
            f"投稿を増やし、流入→CV導線を強化してください。"
            f"（現在のSNS流入: {total_sessions}セッション）"
        )

    # 5. TOP投稿からの学び
    if top_posts and len(top_posts) >= 3:
        top3_platforms = [p["platform"] for p in top_posts[:3]]
        platform_count = defaultdict(int)
        for p in top3_platforms:
            platform_count[p] += 1
        dominant = max(platform_count.items(), key=lambda x: x[1])
        insights.append(
            f"🏆 TOP3投稿のうち{dominant[1]}件が **{dominant[0].upper()}** です。"
            f"このプラットフォームのコンテンツ戦略が最も成果に結びついています。"
        )

    return insights


def _map_source(source: str) -> str:
    """GA4ソースをプラットフォーム名に変換"""
    source_lower = source.lower()
    for platform, sources in UTM_SOURCE_MAP.items():
        if source_lower in [s.lower() for s in sources]:
            return platform
    # facebook系
    if "facebook" in source_lower:
        return "facebook"
    return source_lower


def run_full_analysis() -> dict:
    """全分析を実行して結果を返す"""
    print("🔍 CV貢献分析エンジン実行中...")

    results = {
        "generated_at": datetime.now().isoformat(),
        "platform_summary": analyze_platform_summary(),
        "post_type_performance": analyze_post_type_performance(),
        "ga4_traffic": analyze_ga4_traffic_by_platform(),
        "top_landing_pages": analyze_top_landing_pages(),
        "utm_campaigns": analyze_utm_campaigns(),
        "top_posts": analyze_top_posts(),
        "daily_trend": analyze_daily_trend(),
        "posting_time": analyze_posting_time_performance(),
    }

    results["insights"] = generate_insights(
        results["platform_summary"],
        results["ga4_traffic"],
        results["top_posts"],
        results["post_type_performance"],
    )

    print(f"✅ 分析完了: {len(results['insights'])}件のインサイト生成")
    return results


if __name__ == "__main__":
    import json
    results = run_full_analysis()
    
    print("\n" + "=" * 60)
    print("📊 分析サマリー")
    print("=" * 60)
    
    print("\n【プラットフォーム別】")
    for platform, data in results["platform_summary"].items():
        print(f"  {platform.upper()}: {data['total_posts']}投稿, "
              f"Views={data['total_views']}, Likes={data['total_likes']}, "
              f"EngRate={data['avg_engagement_rate']}%")
    
    print("\n【GA4流入】")
    for source, data in results["ga4_traffic"].items():
        print(f"  {source}: {data['sessions']}sessions, "
              f"engaged={data['engaged']}, CV={data['conversions']}")
    
    print("\n【自動生成インサイト】")
    for i, insight in enumerate(results["insights"], 1):
        print(f"  {i}. {insight}")
