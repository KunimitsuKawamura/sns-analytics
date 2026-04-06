"""
SNS Performance Pipeline - 分析→レポート生成
"""
import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from analyzers.cv_attribution import run_full_analysis
from analyzers.post_pattern import run_pattern_analysis
from analyzers.week_comparison import run_comparison_analysis
from reporters.weekly_report import generate_weekly_report
from exporters.looker_export import run_full_export
from exporters.dashboard_generator import generate_dashboard
from config import REPORT_OUTPUT_DIR, STAFF_ASSIGNMENTS


def build_staff_reports(results: dict) -> dict:
    """担当者別のKPIサマリーを構築"""
    staff_data = {}
    for staff_name, config in STAFF_ASSIGNMENTS.items():
        platforms = config["platforms"]
        role = config["role"]

        # プラットフォーム別KPIを担当者ごとに集約
        total_posts = 0
        total_views = 0
        total_likes = 0
        total_replies = 0
        platform_details = {}

        for p in platforms:
            ps = results.get("platform_summary", {}).get(p, {})
            total_posts += ps.get("total_posts", 0) or 0
            total_views += ps.get("total_views", 0) or 0
            total_likes += ps.get("total_likes", 0) or 0
            total_replies += ps.get("total_replies", 0) or 0
            platform_details[p] = ps

        # GA4流入を担当プラットフォームごとに集約
        ga4_sessions = 0
        ga4_engaged = 0
        ga4_cv = 0
        for p in platforms:
            ga4 = results.get("ga4_traffic", {}).get(p, {})
            ga4_sessions += ga4.get("sessions", 0) or 0
            ga4_engaged += ga4.get("engaged", 0) or 0
            ga4_cv += ga4.get("conversions", 0) or 0

        # 前週比較
        comp_insights = []
        platform_comp = results.get("platform_comparison", {})
        for p in platforms:
            comp = platform_comp.get(p, {})
            diff = comp.get("diff", {})
            tw = comp.get("this_week", {})
            pw = comp.get("prev_week", {})
            if tw.get("posts", 0) > 0 or pw.get("posts", 0) > 0:
                comp_insights.append(
                    f"{p.upper()}: 投稿 {pw.get('posts',0)}→{tw.get('posts',0)} "
                    f"({diff.get('posts_diff',0):+d}), "
                    f"Like {pw.get('likes',0)}→{tw.get('likes',0)} "
                    f"({diff.get('likes_pct',0):+.1f}%)"
                )

        # 勝ちパターン（担当プラットフォームのもの）
        staff_patterns = [
            wp for wp in results.get("winning_patterns", [])
            if wp["platform"] in platforms or wp["platform"] == "全体"
        ]

        staff_data[staff_name] = {
            "role": role,
            "platforms": platforms,
            "total_posts": total_posts,
            "total_views": total_views,
            "total_likes": total_likes,
            "total_replies": total_replies,
            "ga4_sessions": ga4_sessions,
            "ga4_engaged": ga4_engaged,
            "ga4_cv": ga4_cv,
            "platform_details": platform_details,
            "comparison": comp_insights,
            "winning_patterns": staff_patterns[:3],
        }

    return staff_data


def main():
    print("=" * 60)
    print("📊 SNS Performance 分析 & レポート生成")
    print("=" * 60)

    # 1. CV貢献分析
    results = run_full_analysis()

    # 2. 勝ちパターン分析
    pattern_results = run_pattern_analysis()
    results["winning_patterns"] = pattern_results["winning_patterns"]
    results["content_length_analysis"] = pattern_results["content_length"]
    results["cta_impact"] = pattern_results["cta_impact"]
    results["hashtag_impact"] = pattern_results["hashtag_impact"]
    results["hook_patterns"] = pattern_results["hook_patterns"]
    results["theme_performance"] = pattern_results["theme_performance"]
    results["posting_time"] = pattern_results["posting_time"]
    results["engagement_velocity"] = pattern_results["engagement_velocity"]

    # 3. 前週比較分析
    comparison = run_comparison_analysis()
    results["this_week_range"] = comparison["this_week_range"]
    results["prev_week_range"] = comparison["prev_week_range"]
    results["platform_comparison"] = comparison["platform_comparison"]
    results["ga4_comparison"] = comparison["ga4_comparison"]
    results["engagement_trend"] = comparison["engagement_trend"]
    results["comparison_insights"] = comparison["comparison_insights"]

    # 4. 担当者別レポート構築
    staff_reports = build_staff_reports(results)
    results["staff_reports"] = staff_reports
    print(f"👤 担当者別レポート生成: {len(staff_reports)}名分")

    # 5. 分析結果をJSON保存
    REPORT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    json_path = REPORT_OUTPUT_DIR / "latest_analysis.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2, default=str)
    print(f"💾 分析結果JSON: {json_path}")

    # 6. HTMLレポート生成
    report_path = generate_weekly_report(results)

    # 7. Looker Studio向けCSVエクスポート
    looker_paths = run_full_export()

    # 8. インタラクティブダッシュボード生成
    dashboard_path = generate_dashboard(results)

    # 9. サマリー出力
    print("\n" + "=" * 60)
    print("📋 分析サマリー")
    print("=" * 60)

    print("\n【プラットフォーム別】")
    for platform, data in results["platform_summary"].items():
        print(f"  {platform.upper():12s} | {data['total_posts']:3d}投稿 | "
              f"Views: {data['total_views']:>6,} | Likes: {data['total_likes']:>4,} | "
              f"EngRate: {data['avg_engagement_rate']}%")

    print(f"\n【前週比較】 {results['this_week_range']} vs {results['prev_week_range']}")
    for ins in results["comparison_insights"][:3]:
        print(f"  {ins}")

    print("\n【⏰ 投稿タイミング】")
    best = results.get("posting_time", {}).get("best_timing", {})
    bc = best.get("best_combo")
    if bc:
        print(f"  ベストタイミング: {bc['weekday_name']}曜日 × {bc['slot_name']} (avg {bc['avg_likes']} likes, {bc['count']}件)")
    else:
        bw = best.get("best_weekday")
        bs = best.get("best_hour_slot")
        if bw:
            print(f"  ベスト曜日: {bw['name']}曜日 (avg {bw['avg_likes']} likes, {bw['count']}件)")
        if bs:
            print(f"  ベスト時間帯: {bs['name']} (avg {bs['avg_likes']} likes, {bs['count']}件)")

    vi = results.get("engagement_velocity", {}).get("velocity_insight")
    if vi:
        print(f"\n【🚀 初速分析】")
        print(f"  {vi['interpretation']}")

    print("\n【🎯 勝ちパターンTOP3】")
    for i, p in enumerate(results["winning_patterns"][:3], 1):
        print(f"  {i}. [{p['platform'].upper()}] {p['category']}: {p['pattern']}")

    print(f"\n📄 HTMLレポート: {report_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()

