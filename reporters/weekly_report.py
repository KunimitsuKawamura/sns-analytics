"""
週次レポート生成
分析結果をHTMLレポートとして出力
"""
import json
from datetime import datetime, date
from pathlib import Path

from config import REPORT_OUTPUT_DIR


def generate_weekly_report(analysis_results: dict) -> str:
    """分析結果からHTML週次レポートを生成"""
    REPORT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    report_date = date.today().strftime("%Y%m%d")
    filename = f"weekly_report_{report_date}.html"
    filepath = REPORT_OUTPUT_DIR / filename

    html = _build_html(analysis_results)
    filepath.write_text(html, encoding="utf-8")
    print(f"📄 レポート生成: {filepath}")
    return str(filepath)


def _build_html(data: dict) -> str:
    """HTMLを構築"""
    summary = data.get("platform_summary", {})
    ga4 = data.get("ga4_traffic", {})
    top_posts = data.get("top_posts", [])
    post_types = data.get("post_type_performance", [])
    insights = data.get("insights", [])
    utm_campaigns = data.get("utm_campaigns", [])
    top_pages = data.get("top_landing_pages", [])
    winning_patterns = data.get("winning_patterns", [])
    theme_perf = data.get("theme_performance", {})
    cta_impact = data.get("cta_impact", {})
    hook_patterns = data.get("hook_patterns", {})
    platform_comparison = data.get("platform_comparison", {})
    ga4_comparison = data.get("ga4_comparison", {})
    comparison_insights = data.get("comparison_insights", [])
    staff_reports = data.get("staff_reports", {})
    this_week_range = data.get("this_week_range", "")
    prev_week_range = data.get("prev_week_range", "")

    # プラットフォームサマリーHTML
    platform_rows = ""
    for p in ["instagram", "threads", "x"]:
        d = summary.get(p, {})
        platform_rows += f"""
        <tr>
            <td class="platform-{p}">{p.upper()}</td>
            <td>{d.get('total_posts', 0)}</td>
            <td>{d.get('total_views', 0):,}</td>
            <td>{d.get('total_likes', 0):,}</td>
            <td>{d.get('total_replies', 0):,}</td>
            <td>{d.get('total_reposts', 0):,}</td>
            <td>{d.get('total_saves', 0):,}</td>
            <td>{d.get('avg_engagement_rate', 0)}%</td>
        </tr>"""

    # GA4流入HTML
    ga4_rows = ""
    total_sessions = 0
    for source, d in sorted(ga4.items(), key=lambda x: x[1]["sessions"], reverse=True):
        total_sessions += d["sessions"]
        ga4_rows += f"""
        <tr>
            <td>{source}</td>
            <td>{d['sessions']:,}</td>
            <td>{d['engaged']:,}</td>
            <td>{d['engagement_rate']}%</td>
            <td>{int(d['conversions'])}</td>
        </tr>"""

    # TOP投稿HTML
    top_posts_rows = ""
    for i, post in enumerate(top_posts[:10], 1):
        content = post.get("content_preview", "")[:60]
        top_posts_rows += f"""
        <tr>
            <td>{i}</td>
            <td class="platform-{post['platform']}">{post['platform'].upper()}</td>
            <td>{post.get('post_type', '')}</td>
            <td class="content-cell">{content}</td>
            <td>{post.get('views', 0):,}</td>
            <td>{post.get('likes', 0)}</td>
            <td>{post.get('replies', 0)}</td>
            <td>{post.get('engagement_rate', 0)}%</td>
        </tr>"""

    # 投稿タイプ別HTML
    type_rows = ""
    for pt in post_types:
        type_rows += f"""
        <tr>
            <td class="platform-{pt['platform']}">{pt['platform'].upper()}</td>
            <td>{pt['post_type']}</td>
            <td>{pt['count']}</td>
            <td>{pt.get('avg_views', 0):,.0f}</td>
            <td>{pt.get('avg_likes', 0)}</td>
            <td>{pt.get('avg_eng_rate', 0)}%</td>
        </tr>"""

    # UTMキャンペーンHTML
    utm_rows = ""
    for u in utm_campaigns:
        utm_rows += f"""
        <tr>
            <td>{u['campaign']}</td>
            <td>{u['source']}</td>
            <td>{u.get('content', '')}</td>
            <td>{u['total_sessions']}</td>
            <td>{u['total_engaged']}</td>
            <td>{int(u['total_conversions'])}</td>
        </tr>"""

    # インサイトHTML
    insights_html = ""
    for insight in insights:
        insights_html += f"<li>{insight}</li>\n"

    # トップLPのHTML
    lp_rows = ""
    for lp in top_pages[:5]:
        lp_rows += f"""
        <tr>
            <td>{lp['landing_page']}</td>
            <td>{lp['total_sessions']:,}</td>
            <td>{lp['total_engaged']:,}</td>
            <td>{int(lp['total_conversions'])}</td>
        </tr>"""

    # 勝ちパターンHTML
    patterns_html = ""
    colors = ["#6366f1", "#22c55e", "#f59e0b", "#ef4444", "#8b5cf6", "#06b6d4", "#ec4899", "#14b8a6", "#f97316"]
    for i, wp in enumerate(winning_patterns):
        color = colors[i % len(colors)]
        platform_label = wp['platform'].upper() if wp['platform'] != '全体' else '全体'
        patterns_html += f"""
        <div style="background: linear-gradient(135deg, {color}15, {color}05); border-left: 4px solid {color}; border-radius: 8px; padding: 1rem 1.2rem; margin-bottom: 0.8rem;">
            <div style="display: flex; align-items: center; gap: 0.5rem; margin-bottom: 0.4rem;">
                <span style="background: {color}30; color: {color}; padding: 2px 8px; border-radius: 4px; font-size: 0.75rem; font-weight: 700;">{platform_label}</span>
                <span style="background: rgba(255,255,255,0.1); padding: 2px 8px; border-radius: 4px; font-size: 0.75rem;">{wp['category']}</span>
                <span style="font-weight: 700;">{wp['pattern']}</span>
            </div>
            <div style="font-size: 0.85rem; color: var(--text-muted);">💡 {wp['recommendation']}</div>
        </div>"""

    # テーマ別HTML
    theme_rows = ""
    for theme, d in sorted(theme_perf.items(), key=lambda x: x[1].get("avg_likes", 0), reverse=True):
        platforms_str = ", ".join(f"{k.upper()}: {v}" for k, v in d.get("platforms", {}).items())
        theme_rows += f"""
        <tr>
            <td style="font-weight: 600;">{theme}</td>
            <td>{d.get('count', 0)}</td>
            <td>{d.get('avg_likes', 0)}</td>
            <td>{d.get('avg_replies', 0)}</td>
            <td>{d.get('avg_engagement', 0)}%</td>
            <td style="font-size: 0.8rem;">{platforms_str}</td>
        </tr>"""

    # CTA効果HTML
    cta_html = ""
    for platform, d in cta_impact.items():
        wc = d.get("with_cta", {})
        woc = d.get("without_cta", {})
        lift = d.get("cta_lift", 0)
        lift_color = "#22c55e" if lift > 0 else "#ef4444"
        cta_html += f"""
        <tr>
            <td class="platform-{platform}">{platform.upper()}</td>
            <td>{wc.get('count', 0)}</td><td>{wc.get('avg_likes', 0)}</td>
            <td>{woc.get('count', 0)}</td><td>{woc.get('avg_likes', 0)}</td>
            <td style="color: {lift_color}; font-weight: 700;">{lift:+.1f}%</td>
        </tr>"""

    # フック表現HTML
    hook_html = ""
    for platform, hooks in hook_patterns.items():
        for name, d in sorted(hooks.items(), key=lambda x: x[1].get("avg_likes", 0), reverse=True):
            hook_html += f"""
        <tr>
            <td class="platform-{platform}">{platform.upper()}</td>
            <td>{name}</td>
            <td>{d.get('count', 0)}</td>
            <td>{d.get('avg_likes', 0)}</td>
            <td>{d.get('avg_replies', 0)}</td>
            <td>{d.get('avg_engagement', 0)}%</td>
        </tr>"""

    # 前週比較HTML
    def _arrow(val):
        if val > 0: return f'<span style="color:#22c55e;">▲ {val:+.1f}%</span>'
        if val < 0: return f'<span style="color:#ef4444;">▼ {val:+.1f}%</span>'
        return '<span style="color:var(--text-muted);">→ 0%</span>'

    comp_rows = ""
    for p in ["instagram", "threads", "x"]:
        comp = platform_comparison.get(p, {})
        tw = comp.get("this_week", {})
        pw = comp.get("prev_week", {})
        diff = comp.get("diff", {})
        comp_rows += f"""
        <tr>
            <td class="platform-{p}">{p.upper()}</td>
            <td>{pw.get('posts', 0)}</td><td>{tw.get('posts', 0)}</td><td>{_arrow(diff.get('posts_pct', 0))}</td>
            <td>{pw.get('likes', 0)}</td><td>{tw.get('likes', 0)}</td><td>{_arrow(diff.get('likes_pct', 0))}</td>
            <td>{pw.get('views', 0):,}</td><td>{tw.get('views', 0):,}</td><td>{_arrow(diff.get('views_pct', 0))}</td>
        </tr>"""

    ga4_comp_rows = ""
    for p in ["instagram", "threads", "x"]:
        comp = ga4_comparison.get(p, {})
        tw = comp.get("this_week", {})
        pw = comp.get("prev_week", {})
        diff = comp.get("diff", {})
        ga4_comp_rows += f"""
        <tr>
            <td class="platform-{p}">{p.upper()}</td>
            <td>{pw.get('sessions', 0)}</td><td>{tw.get('sessions', 0)}</td><td>{_arrow(diff.get('sessions_pct', 0))}</td>
            <td>{pw.get('engaged', 0)}</td><td>{tw.get('engaged', 0)}</td><td>{_arrow(diff.get('engaged_pct', 0))}</td>
        </tr>"""

    comp_insights_html = ""
    for ins in comparison_insights:
        comp_insights_html += f"<li>{ins}</li>\n"

    # 担当者別HTML（担当者名なし）
    staff_html = ""
    staff_colors = ["#D4956B", "#6BA3A5", "#C18D5D", "#5D9B9D", "#B8845A"]
    for i, (name, sd) in enumerate(staff_reports.items()):
        color = staff_colors[i % len(staff_colors)]
        platforms_str = " / ".join(p.upper() for p in sd.get("platforms", []))

        # 前週比較行
        comp_lines = ""
        for cl in sd.get("comparison", []):
            comp_lines += f'<div style="font-size: 0.85rem; color: var(--text-muted); padding: 2px 0;">📊 {cl}</div>'

        # 勝ちパターン行
        pattern_lines = ""
        for wp in sd.get("winning_patterns", []):
            pattern_lines += f'<div style="font-size: 0.85rem; padding: 2px 0;">🎯 {wp["category"]}: {wp["pattern"]}</div>'

        staff_html += f"""
        <div style="background: linear-gradient(135deg, {color}15, {color}08); border: 1px solid {color}50; border-radius: 12px; padding: 1.2rem; margin-bottom: 1rem;">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.8rem;">
                <div>
                    <span style="background: {color}35; color: {color}; padding: 3px 12px; border-radius: 6px; font-size: 0.85rem; font-weight: 600;">{sd.get('role', '')}</span>
                </div>
                <span style="font-size: 0.8rem; color: var(--text-muted);">{platforms_str}</span>
            </div>
            <div class="kpi-grid" style="margin-bottom: 0.8rem;">
                <div class="kpi" style="padding: 0.6rem;"><div class="value" style="font-size: 1.3rem;">{sd.get('total_posts', 0)}</div><div class="label">投稿数</div></div>
                <div class="kpi" style="padding: 0.6rem;"><div class="value" style="font-size: 1.3rem;">{sd.get('total_likes', 0)}</div><div class="label">いいね</div></div>
                <div class="kpi" style="padding: 0.6rem;"><div class="value" style="font-size: 1.3rem;">{sd.get('total_views', 0):,}</div><div class="label">閲覧数</div></div>
                <div class="kpi" style="padding: 0.6rem;"><div class="value" style="font-size: 1.3rem;">{sd.get('ga4_sessions', 0)}</div><div class="label">サイト流入</div></div>
            </div>
            {comp_lines}
            {pattern_lines}
        </div>"""

    return f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>SNS効果測定 週次レポート - {date.today()}</title>
    <style>
        :root {{
            --primary: #D4956B;
            --primary-cool: #6BA3A5;
            --success: #5D9B7A;
            --warning: #D4956B;
            --danger: #C4655A;
            --bg: #1A1D23;
            --card-bg: #22262E;
            --text: #F0ECE6;
            --text-muted: #A09B93;
            --border: #353840;
            --accent-warm: #FFE5C7;
            --accent-cool: #C1E7E8;
        }}
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: 'Inter', 'Hiragino Sans', sans-serif;
            background: var(--bg);
            color: var(--text);
            line-height: 1.6;
            padding: 2rem;
        }}
        .header {{
            text-align: center;
            margin-bottom: 2rem;
            padding: 2rem;
            background: linear-gradient(135deg, #FFE5C7 0%, #C1E7E8 100%);
            border-radius: 16px;
            color: #2A2520;
        }}
        .header h1 {{ font-size: 1.8rem; margin-bottom: 0.5rem; color: #2A2520; }}
        .header p {{ color: #5A524A; }}
        .section {{ margin-bottom: 2rem; }}
        .section h2 {{
            font-size: 1.3rem;
            margin-bottom: 1rem;
            padding-bottom: 0.5rem;
            border-bottom: 2px solid;
            border-image: linear-gradient(90deg, var(--accent-warm), var(--accent-cool)) 1;
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }}
        .card {{
            background: var(--card-bg);
            border-radius: 12px;
            padding: 1.5rem;
            margin-bottom: 1rem;
            border: 1px solid var(--border);
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 0.9rem;
        }}
        th {{
            background: linear-gradient(90deg, rgba(255,229,199,0.12), rgba(193,231,232,0.12));
            padding: 0.75rem;
            text-align: left;
            font-weight: 600;
            border-bottom: 2px solid var(--border);
            color: var(--accent-warm);
        }}
        td {{
            padding: 0.6rem 0.75rem;
            border-bottom: 1px solid var(--border);
        }}
        tr:hover {{ background: rgba(255, 229, 199, 0.04); }}
        .platform-instagram {{ color: #e1306c; font-weight: 700; }}
        .platform-threads {{ color: #6BA3A5; font-weight: 700; }}
        .platform-x {{ color: #D4956B; font-weight: 700; }}
        .content-cell {{ font-size: 0.8rem; max-width: 300px; }}
        .insights {{ background: linear-gradient(135deg, var(--card-bg), var(--bg)); }}
        .insights li {{
            padding: 0.75rem 0;
            border-bottom: 1px solid var(--border);
            list-style: none;
        }}
        .insights li:last-child {{ border: none; }}
        .kpi-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 1rem;
            margin-bottom: 1.5rem;
        }}
        .kpi {{
            background: var(--card-bg);
            border-radius: 10px;
            padding: 1rem;
            text-align: center;
            border: 1px solid var(--border);
        }}
        .kpi .value {{ font-size: 1.8rem; font-weight: 700; background: linear-gradient(135deg, var(--accent-warm), var(--accent-cool)); -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text; }}
        .kpi .label {{ font-size: 0.8rem; color: var(--text-muted); margin-top: 0.3rem; }}
        .footer {{
            text-align: center;
            color: var(--text-muted);
            font-size: 0.8rem;
            padding-top: 2rem;
            border-top: 1px solid var(--border);
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>📊 SNS効果測定 週次レポート</h1>
        <p>ミートキャリア | 生成日: {datetime.now().strftime('%Y年%m月%d日 %H:%M')}</p>
    </div>

    <div class="kpi-grid">
        <div class="kpi">
            <div class="value">{sum(d.get('total_posts',0) for d in summary.values())}</div>
            <div class="label">総投稿数</div>
        </div>
        <div class="kpi">
            <div class="value">{sum(d.get('total_views',0) for d in summary.values()):,}</div>
            <div class="label">総閲覧数</div>
        </div>
        <div class="kpi">
            <div class="value">{sum(d.get('total_likes',0) for d in summary.values()):,}</div>
            <div class="label">総いいね数</div>
        </div>
        <div class="kpi">
            <div class="value">{total_sessions:,}</div>
            <div class="label">SNS→サイト流入</div>
        </div>
    </div>

    <div class="section">
        <h2>📅 前週比較 ({this_week_range} vs {prev_week_range})</h2>
        <div class="card">
            <h3 style="font-size: 1rem; margin-bottom: 0.8rem; color: var(--text-muted);">SNS投稿KPI</h3>
            <table>
                <thead>
                    <tr><th>Platform</th><th>前週 投稿</th><th>今週 投稿</th><th>変化</th><th>前週 Like</th><th>今週 Like</th><th>変化</th><th>前週 Views</th><th>今週 Views</th><th>変化</th></tr>
                </thead>
                <tbody>{comp_rows}</tbody>
            </table>
        </div>
        <div class="card" style="margin-top: 1rem;">
            <h3 style="font-size: 1rem; margin-bottom: 0.8rem; color: var(--text-muted);">GA4 SNS流入</h3>
            <table>
                <thead>
                    <tr><th>Platform</th><th>前週 セッション</th><th>今週 セッション</th><th>変化</th><th>前週 エンゲージ</th><th>今週 エンゲージ</th><th>変化</th></tr>
                </thead>
                <tbody>{ga4_comp_rows}</tbody>
            </table>
        </div>
        <div class="card insights" style="margin-top: 1rem;">
            <ul>{comp_insights_html if comp_insights_html else '<li style="color: var(--text-muted);">比較データなし</li>'}</ul>
        </div>
    </div>

    <div class="section">
        <h2>👤 担当者別レポート</h2>
        {staff_html if staff_html else '<div class="card"><p style="color: var(--text-muted);">担当者が未設定です（config.pyのSTAFF_ASSIGNMENTSを設定してください）</p></div>'}
    </div>

    <div class="section">
        <h2>🎯 勝ちパターン（自動抽出）</h2>
        <div class="card">
            {patterns_html if patterns_html else '<p style="color: var(--text-muted);">データ不足のため勝ちパターンを特定できません</p>'}
        </div>
    </div>

    <div class="section">
        <h2>💡 自動生成インサイト</h2>
        <div class="card insights">
            <ul>{insights_html}</ul>
        </div>
    </div>

    <div class="section">
        <h2>📈 プラットフォーム別パフォーマンス</h2>
        <div class="card">
            <table>
                <thead>
                    <tr><th>Platform</th><th>投稿数</th><th>閲覧</th><th>いいね</th><th>リプライ</th><th>RT/リポスト</th><th>保存</th><th>Eng率</th></tr>
                </thead>
                <tbody>{platform_rows}</tbody>
            </table>
        </div>
    </div>

    <div class="section">
        <h2>📝 テーマ別パフォーマンス</h2>
        <div class="card">
            <table>
                <thead>
                    <tr><th>テーマ</th><th>件数</th><th>平均Like</th><th>平均Reply</th><th>平均Eng率</th><th>プラットフォーム分布</th></tr>
                </thead>
                <tbody>{theme_rows}</tbody>
            </table>
        </div>
    </div>

    <div class="section">
        <h2>📣 CTA効果分析</h2>
        <div class="card">
            <table>
                <thead>
                    <tr><th>Platform</th><th>CTA有 件数</th><th>CTA有 avg Like</th><th>CTA無 件数</th><th>CTA無 avg Like</th><th>リフト率</th></tr>
                </thead>
                <tbody>{cta_html}</tbody>
            </table>
        </div>
    </div>

    <div class="section">
        <h2>🪝 フック表現パターン分析</h2>
        <div class="card">
            <table>
                <thead>
                    <tr><th>Platform</th><th>フック型</th><th>件数</th><th>avg Like</th><th>avg Reply</th><th>avg Eng率</th></tr>
                </thead>
                <tbody>{hook_html}</tbody>
            </table>
        </div>
    </div>

    <div class="section">
        <h2>🔗 GA4 SNS流入分析</h2>
        <div class="card">
            <table>
                <thead>
                    <tr><th>ソース</th><th>セッション</th><th>エンゲージセッション</th><th>Eng率</th><th>CV</th></tr>
                </thead>
                <tbody>{ga4_rows}</tbody>
            </table>
        </div>
    </div>

    <div class="section">
        <h2>🏆 TOP投稿（エンゲージメント順）</h2>
        <div class="card">
            <table>
                <thead>
                    <tr><th>#</th><th>Platform</th><th>タイプ</th><th>内容</th><th>Views</th><th>Like</th><th>Reply</th><th>Eng率</th></tr>
                </thead>
                <tbody>{top_posts_rows}</tbody>
            </table>
        </div>
    </div>

    <div class="section">
        <h2>📋 投稿タイプ別分析</h2>
        <div class="card">
            <table>
                <thead>
                    <tr><th>Platform</th><th>タイプ</th><th>件数</th><th>平均Views</th><th>平均Like</th><th>Eng率</th></tr>
                </thead>
                <tbody>{type_rows}</tbody>
            </table>
        </div>
    </div>

    <div class="section">
        <h2>🎯 UTMキャンペーン別成果</h2>
        <div class="card">
            <table>
                <thead>
                    <tr><th>キャンペーン</th><th>ソース</th><th>コンテンツ</th><th>セッション</th><th>エンゲージ</th><th>CV</th></tr>
                </thead>
                <tbody>{utm_rows}</tbody>
            </table>
        </div>
    </div>

    <div class="section">
        <h2>📄 トップランディングページ</h2>
        <div class="card">
            <table>
                <thead>
                    <tr><th>ページ</th><th>セッション</th><th>エンゲージセッション</th><th>CV</th></tr>
                </thead>
                <tbody>{lp_rows}</tbody>
            </table>
        </div>
    </div>

    <div class="footer">
        <p>SNS Performance Pipeline | 自動生成レポート | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    </div>
</body>
</html>"""


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from analyzers.cv_attribution import run_full_analysis
    results = run_full_analysis()
    path = generate_weekly_report(results)
    print(f"レポート: {path}")
