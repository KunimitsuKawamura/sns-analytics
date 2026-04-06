"""
Looker Studio風インタラクティブダッシュボード生成
Chart.js + HTML/JSで4ページのダッシュボードを自動生成
"""
import json
import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import REPORT_OUTPUT_DIR


WEEKDAY_NAMES = ["月", "火", "水", "木", "金", "土", "日"]


def _build_timing_section(posting_time: dict, velocity: dict) -> str:
    """タイミング分析ページのHTML生成"""
    import json as _json

    all_data = posting_time.get("all", {})
    best = posting_time.get("best_timing", {})
    slot_names = posting_time.get("slot_names", [])
    by_platform = posting_time.get("by_platform", {})

    # --- ベストタイミング ---
    bc = best.get("best_combo")
    bw = best.get("best_weekday", {})
    bs = best.get("best_hour_slot", {})
    best_html = '<div class="best-timing">'
    if bc:
        # 実績ベースのベストコンボを表示
        best_html += f'''
        <div class="best-item">
            <div class="bi-label">🏆 ベスト曜日</div>
            <div class="bi-val">{bc.get("weekday_name", "?")}曜日</div>
            <div class="bi-likes">avg {bc.get("avg_likes", 0)} likes ({bc.get("count", 0)}件)</div>
        </div>
        <div class="best-item">
            <div class="bi-label">🏆 ベスト時間帯</div>
            <div class="bi-val">{bc.get("slot_name", "?")}</div>
            <div class="bi-likes">avg {bc.get("avg_likes", 0)} likes ({bc.get("count", 0)}件)</div>
        </div>'''
    else:
        # フォールバック: 独立した1位（コンボデータがない場合）
        if bw:
            best_html += f'''
        <div class="best-item">
            <div class="bi-label">🏆 ベスト曜日</div>
            <div class="bi-val">{bw.get("name", "?")}曜日</div>
            <div class="bi-likes">avg {bw.get("avg_likes", 0)} likes ({bw.get("count", 0)}件)</div>
        </div>'''
        if bs:
            best_html += f'''
        <div class="best-item">
            <div class="bi-label">🏆 ベスト時間帯</div>
            <div class="bi-val">{bs.get("name", "?")}</div>
            <div class="bi-likes">avg {bs.get("avg_likes", 0)} likes ({bs.get("count", 0)}件)</div>
        </div>'''
    best_html += '</div>'

    # --- ヒートマップ（全体） ---
    heatmap = all_data.get("heatmap", [])
    hm_html = '<div class="heatmap">'
    # ヘッダー行
    hm_html += '<div class="hm-header"></div>'  # 左上空セル
    for sn in slot_names:
        short = sn.split("(")[0] if "(" in sn else sn[:4]
        hm_html += f'<div class="hm-header">{short}</div>'
    # データ行
    max_likes = 1
    for row in heatmap:
        for cell in row:
            if cell.get("avg_likes", 0) > max_likes:
                max_likes = cell["avg_likes"]

    for day_idx, row in enumerate(heatmap):
        hm_html += f'<div class="hm-label">{WEEKDAY_NAMES[day_idx]}</div>'
        for cell in row:
            avg = cell.get("avg_likes", 0)
            cnt = cell.get("count", 0)
            # 色の強度を計算（0-1）
            intensity = avg / max_likes if max_likes > 0 else 0
            if cnt == 0:
                bg = "rgba(46,51,60,0.5)"
                text_color = "var(--muted)"
            elif intensity > 0.7:
                bg = f"rgba(93,155,122,{0.3 + intensity * 0.6})"
                text_color = "#fff"
            elif intensity > 0.3:
                bg = f"rgba(212,149,107,{0.2 + intensity * 0.5})"
                text_color = "#fff"
            else:
                bg = f"rgba(107,163,165,{0.15 + intensity * 0.3})"
                text_color = "var(--text)"
            hm_html += f'<div class="hm-cell" style="background:{bg}; color:{text_color};">'
            if cnt > 0:
                hm_html += f'<div class="hm-val">{avg}</div><div class="hm-cnt">{cnt}件</div>'
            else:
                hm_html += '<div class="hm-val" style="opacity:0.3;">-</div>'
            hm_html += '</div>'
    hm_html += '</div>'

    # --- 曜日別チャートデータ ---
    wd_data = all_data.get("by_weekday", {})
    wd_labels = _json.dumps(WEEKDAY_NAMES, ensure_ascii=False)
    wd_likes = _json.dumps([wd_data.get(d, {}).get("avg_likes", 0) for d in WEEKDAY_NAMES])
    wd_counts = _json.dumps([wd_data.get(d, {}).get("count", 0) for d in WEEKDAY_NAMES])

    # --- 時間帯別チャートデータ ---
    hs_data = all_data.get("by_hour_slot", {})
    def _slot_label(sn):
        """'早朝(5-8時)' → '早朝（5:00〜）'"""
        import re
        m = re.match(r'(.+?)\((\d+)', sn)
        if m:
            return f"{m.group(1)}（{m.group(2)}:00〜）"
        return sn
    hs_labels = _json.dumps([_slot_label(sn) for sn in slot_names], ensure_ascii=False)
    hs_likes = _json.dumps([hs_data.get(sn, {}).get("avg_likes", 0) for sn in slot_names])
    hs_counts = _json.dumps([hs_data.get(sn, {}).get("count", 0) for sn in slot_names])

    # --- プラットフォーム別曜日データ ---
    plat_wd_datasets = ""
    plat_colors = {"instagram": "'#e1306c'", "threads": "accent2", "x": "accent1"}
    plat_alphas = {"instagram": "rgba(225,48,108,0.7)", "threads": "rgba(107,163,165,0.7)", "x": "rgba(212,149,107,0.7)"}
    for plat in ["instagram", "threads", "x"]:
        pd = by_platform.get(plat, {}).get("by_weekday", {})
        plat_vals = _json.dumps([pd.get(d, {}).get("avg_likes", 0) for d in WEEKDAY_NAMES])
        plat_wd_datasets += f"{{ label: '{plat.upper()}', data: {plat_vals}, backgroundColor: '{plat_alphas.get(plat, 'rgba(150,150,150,0.7)')}', borderRadius: 4 }},\n"

    # --- エンゲージメント初速 ---
    vel_all = velocity.get("all", {})
    bucket_names = list(vel_all.keys())
    vel_labels = _json.dumps(bucket_names, ensure_ascii=False)
    vel_likes = _json.dumps([vel_all.get(b, {}).get("avg_likes", 0) for b in bucket_names])
    vel_counts = _json.dumps([vel_all.get(b, {}).get("count", 0) for b in bucket_names])
    vel_eng = _json.dumps([vel_all.get(b, {}).get("avg_engagement", 0) for b in bucket_names])

    vel_insight = velocity.get("velocity_insight")
    vel_insight_html = ""
    if vel_insight:
        vel_insight_html = f'''
        <div style="margin-top:1rem; padding:0.8rem; background:var(--card2); border-radius:8px; border-left:3px solid var(--accent1);">
            <div style="font-size:0.85rem;">{vel_insight.get("interpretation", "")}</div>
        </div>'''

    vel_pairs_html = ""
    pairs_summary = velocity.get("velocity_pairs_summary")
    if pairs_summary:
        vel_pairs_html = f'''
        <div style="margin-top:0.8rem; padding:0.6rem; background:var(--card2); border-radius:6px; font-size:0.8rem; color:var(--muted);">
            📊 {pairs_summary}
        </div>'''

    return f'''
    {best_html}

    <div class="grid grid-2">
      <div class="card">
        <div class="card-title">🗓 曜日×時間帯 エンゲージメントヒートマップ（全体 avg Like）</div>
        {hm_html}
      </div>
      <div class="card">
        <div class="card-title">📊 曜日別 平均いいね（全体）</div>
        <div class="chart-container"><canvas id="weekdayChart"></canvas></div>
      </div>
    </div>

    <div class="grid grid-2" style="margin-top:1rem;">
      <div class="card">
        <div class="card-title">⏰ 時間帯別 平均いいね（全体）</div>
        <div class="chart-container"><canvas id="hourSlotChart"></canvas></div>
      </div>
      <div class="card">
        <div class="card-title">📊 プラットフォーム別 曜日エンゲージメント</div>
        <div class="chart-container"><canvas id="platWeekdayChart"></canvas></div>
      </div>
    </div>

    <div class="grid grid-2" style="margin-top:1rem;">
      <div class="card">
        <div class="card-title">🚀 エンゲージメント初速（経過日数別 avg Like）</div>
        <div class="chart-container"><canvas id="velocityChart"></canvas></div>
        {vel_insight_html}
        {vel_pairs_html}
      </div>
      <div class="card">
        <div class="card-title">📈 経過日数別 エンゲージメント率</div>
        <div class="chart-container"><canvas id="velocityEngChart"></canvas></div>
      </div>
    </div>

    <script>
    // Timing Charts
    new Chart(document.getElementById('weekdayChart'), {{
      type: 'bar',
      data: {{
        labels: {wd_labels},
        datasets: [{{ label: '平均いいね', data: {wd_likes}, backgroundColor: 'rgba(212,149,107,0.7)', borderRadius: 6 }}]
      }},
      options: {{ responsive:true, maintainAspectRatio:false, plugins:{{ legend:{{ display:false }}, tooltip:{{ callbacks:{{ afterLabel: function(ctx) {{ var counts = {wd_counts}; return counts[ctx.dataIndex] + '件'; }} }} }} }} }}
    }});

    new Chart(document.getElementById('hourSlotChart'), {{
      type: 'bar',
      data: {{
        labels: {hs_labels},
        datasets: [{{ label: '平均いいね', data: {hs_likes}, backgroundColor: 'rgba(107,163,165,0.7)', borderRadius: 6 }}]
      }},
      options: {{ responsive:true, maintainAspectRatio:false, plugins:{{ legend:{{ display:false }}, tooltip:{{ callbacks:{{ afterLabel: function(ctx) {{ var counts = {hs_counts}; return counts[ctx.dataIndex] + '件'; }} }} }} }} }}
    }});

    new Chart(document.getElementById('platWeekdayChart'), {{
      type: 'bar',
      data: {{
        labels: {wd_labels},
        datasets: [{plat_wd_datasets}]
      }},
      options: {{ responsive:true, maintainAspectRatio:false, plugins:{{ legend:{{ position:'bottom' }} }} }}
    }});

    new Chart(document.getElementById('velocityChart'), {{
      type: 'bar',
      data: {{
        labels: {vel_labels},
        datasets: [{{ label: '平均いいね', data: {vel_likes}, backgroundColor: ['rgba(93,155,122,0.8)', 'rgba(107,163,165,0.7)', 'rgba(212,149,107,0.7)', 'rgba(196,101,90,0.6)'], borderRadius: 6 }}]
      }},
      options: {{ responsive:true, maintainAspectRatio:false, plugins:{{ legend:{{ display:false }}, tooltip:{{ callbacks:{{ afterLabel: function(ctx) {{ var counts = {vel_counts}; return counts[ctx.dataIndex] + '件'; }} }} }} }} }}
    }});

    new Chart(document.getElementById('velocityEngChart'), {{
      type: 'line',
      data: {{
        labels: {vel_labels},
        datasets: [{{ label: '平均Eng率(%)', data: {vel_eng}, borderColor: '#D4956B', backgroundColor: 'rgba(212,149,107,0.1)', fill:true, tension:0.4, pointRadius:6, pointHoverRadius:8 }}]
      }},
      options: {{ responsive:true, maintainAspectRatio:false, plugins:{{ legend:{{ display:false }} }} }}
    }});
    </script>
    '''


def generate_dashboard(analysis_data: dict = None) -> Path:
    """インタラクティブダッシュボードHTMLを生成"""
    if analysis_data is None:
        json_path = REPORT_OUTPUT_DIR / "latest_analysis.json"
        if not json_path.exists():
            print("⚠️ latest_analysis.json が見つかりません")
            return None
        with open(json_path, "r", encoding="utf-8") as f:
            analysis_data = json.load(f)

    html = _build_dashboard(analysis_data)
    output_path = REPORT_OUTPUT_DIR / "dashboard.html"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"📊 ダッシュボード: {output_path}")
    return output_path


def _build_dashboard(data: dict) -> str:
    """ダッシュボードHTMLを構築"""
    summary = data.get("platform_summary", {})
    ga4 = data.get("ga4_traffic", {})
    winning = data.get("winning_patterns", [])
    themes = data.get("theme_performance", {})
    cta = data.get("cta_impact", {})
    hooks = data.get("hook_patterns", {})
    comparison = data.get("platform_comparison", {})
    ga4_comp = data.get("ga4_comparison", {})
    trend = data.get("engagement_trend", {})
    top_posts = data.get("top_posts", [])
    comp_insights = data.get("comparison_insights", [])
    insights = data.get("insights", [])
    staff = data.get("staff_reports", {})
    posting_time = data.get("posting_time", {})
    engagement_velocity = data.get("engagement_velocity", {})

    # ---- データをJSオブジェクトに変換 ----
    # 週次トレンド
    weeks = trend.get("weeks", [])
    week_labels = json.dumps([w.get("label", "") for w in weeks], ensure_ascii=False)
    ig_likes = json.dumps([w.get("instagram", {}).get("likes", 0) or 0 for w in weeks])
    th_likes = json.dumps([w.get("threads", {}).get("likes", 0) or 0 for w in weeks])
    x_likes = json.dumps([w.get("x", {}).get("likes", 0) or 0 for w in weeks])
    ig_posts = json.dumps([w.get("instagram", {}).get("posts", 0) or 0 for w in weeks])
    th_posts = json.dumps([w.get("threads", {}).get("posts", 0) or 0 for w in weeks])
    x_posts = json.dumps([w.get("x", {}).get("posts", 0) or 0 for w in weeks])

    # GA4セッション
    ga4_labels = json.dumps(list(ga4.keys()), ensure_ascii=False)
    ga4_sessions = json.dumps([d.get("sessions", 0) for d in ga4.values()])
    ga4_engaged = json.dumps([d.get("engaged", 0) for d in ga4.values()])

    # テーマ分析
    theme_names = sorted(themes.keys(), key=lambda k: themes[k].get("avg_likes", 0), reverse=True)
    theme_labels = json.dumps(theme_names[:8], ensure_ascii=False)
    theme_likes = json.dumps([themes[t].get("avg_likes", 0) for t in theme_names[:8]])
    theme_counts = json.dumps([themes[t].get("count", 0) for t in theme_names[:8]])

    # CTA効果
    cta_platforms = list(cta.keys())
    cta_labels = json.dumps([p.upper() for p in cta_platforms], ensure_ascii=False)
    cta_with = json.dumps([cta[p].get("with_cta", {}).get("avg_likes", 0) for p in cta_platforms])
    cta_without = json.dumps([cta[p].get("without_cta", {}).get("avg_likes", 0) for p in cta_platforms])

    # フック表現（プラットフォーム横断で集計）
    hook_data = {}
    for platform, h in hooks.items():
        for name, d in h.items():
            if name not in hook_data:
                hook_data[name] = {"count": 0, "total_likes": 0}
            hook_data[name]["count"] += d.get("count", 0)
            hook_data[name]["total_likes"] += d.get("avg_likes", 0) * d.get("count", 1)
    hook_sorted = sorted(hook_data.items(), key=lambda x: x[1]["total_likes"] / max(x[1]["count"], 1), reverse=True)
    hook_labels = json.dumps([h[0] for h in hook_sorted[:6]], ensure_ascii=False)
    hook_avg_likes = json.dumps([round(h[1]["total_likes"] / max(h[1]["count"], 1), 1) for h in hook_sorted[:6]])

    # KPIサマリー
    total_posts = sum(d.get("total_posts", 0) or 0 for d in summary.values())
    total_views = sum(d.get("total_views", 0) or 0 for d in summary.values())
    total_likes = sum(d.get("total_likes", 0) or 0 for d in summary.values())
    total_sessions = sum(d.get("sessions", 0) or 0 for d in ga4.values())

    # 前週比較
    def _diff_str(comp_data, key):
        diff = comp_data.get("diff", {})
        pct = diff.get(f"{key}_pct", 0)
        if pct > 0: return f'<span class="up">▲{pct:+.1f}%</span>'
        if pct < 0: return f'<span class="down">▼{pct:+.1f}%</span>'
        return '<span class="flat">→0%</span>'

    # 勝ちパターンHTML
    patterns_html = ""
    for i, wp in enumerate(winning[:6]):
        p_upper = wp["platform"].upper() if wp["platform"] != "全体" else "全体"
        patterns_html += f'''
        <div class="pattern-card">
            <div class="pattern-header">
                <span class="badge">{p_upper}</span>
                <span class="badge secondary">{wp["category"]}</span>
            </div>
            <div class="pattern-title">{wp["pattern"]}</div>
            <div class="pattern-desc">{wp["recommendation"]}</div>
        </div>'''

    # インサイトHTML
    all_insights = comp_insights + insights
    insights_html = ""
    for ins in all_insights[:8]:
        insights_html += f'<div class="insight-item">{ins}</div>\n'

    # TOP投稿HTML
    top_posts_rows = ""
    for i, post in enumerate(top_posts[:8], 1):
        content = (post.get("content_preview", "") or "")[:50]
        top_posts_rows += f'''
        <tr>
            <td>{i}</td>
            <td class="p-{post['platform']}">{post['platform'].upper()}</td>
            <td class="content-cell">{content}</td>
            <td>{post.get('likes', 0)}</td>
            <td>{post.get('replies', 0)}</td>
            <td>{post.get('views', 0):,}</td>
        </tr>'''

    # 担当者カードHTML
    staff_cards = ""
    for name, sd in staff.items():
        plats = " / ".join(p.upper() for p in sd.get("platforms", []))
        comps = "".join(f'<div class="staff-comp">{c}</div>' for c in sd.get("comparison", []))
        pats = "".join(f'<div class="staff-pattern">🎯 {w["category"]}: {w["pattern"]}</div>' for w in sd.get("winning_patterns", [])[:2])
        staff_cards += f'''
        <div class="staff-card">
            <div class="staff-header">
                <span class="badge">{sd.get('role', '')}</span>
                <span class="staff-plat">{plats}</span>
            </div>
            <div class="staff-kpis">
                <div class="staff-kpi"><div class="sk-val">{sd.get('total_posts',0)}</div><div class="sk-label">投稿</div></div>
                <div class="staff-kpi"><div class="sk-val">{sd.get('total_likes',0)}</div><div class="sk-label">Like</div></div>
                <div class="staff-kpi"><div class="sk-val">{sd.get('total_views',0):,}</div><div class="sk-label">Views</div></div>
                <div class="staff-kpi"><div class="sk-val">{sd.get('ga4_sessions',0)}</div><div class="sk-label">流入</div></div>
            </div>
            {comps}{pats}
        </div>'''

    now = datetime.now().strftime('%Y年%m月%d日 %H:%M')

    return f'''<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SNS Analytics Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
* {{ margin:0; padding:0; box-sizing:border-box; }}
:root {{
  --warm: #FFE5C7; --cool: #C1E7E8;
  --bg: #111318; --card: #1A1E26; --card2: #21262F;
  --text: #EDE9E3; --muted: #9A958D; --border: #2E333C;
  --accent1: #D4956B; --accent2: #6BA3A5; --danger: #C4655A; --success: #5D9B7A;
}}
body {{ font-family:'Inter','Hiragino Sans',sans-serif; background:var(--bg); color:var(--text); }}
.dashboard {{ max-width:1400px; margin:0 auto; padding:1rem; }}

/* Header */
.dash-header {{ background:linear-gradient(135deg, var(--warm), var(--cool)); border-radius:16px; padding:1.5rem 2rem; margin-bottom:1.5rem; color:#2A2520; display:flex; justify-content:space-between; align-items:center; }}
.dash-header h1 {{ font-size:1.5rem; }}
.dash-header .subtitle {{ color:#5A524A; font-size:0.85rem; }}

/* Tabs */
.tabs {{ display:flex; gap:0.5rem; margin-bottom:1.5rem; background:var(--card); border-radius:10px; padding:0.3rem; }}
.tab {{ padding:0.6rem 1.2rem; border-radius:8px; cursor:pointer; font-size:0.85rem; font-weight:600; color:var(--muted); transition:all 0.3s; border:none; background:none; }}
.tab:hover {{ color:var(--text); }}
.tab.active {{ background:linear-gradient(135deg, var(--warm), var(--cool)); color:#2A2520; }}
.page {{ display:none; }}
.page.active {{ display:block; }}

/* Grid */
.grid {{ display:grid; gap:1rem; }}
.grid-2 {{ grid-template-columns:1fr 1fr; }}
.grid-3 {{ grid-template-columns:1fr 1fr 1fr; }}
.grid-4 {{ grid-template-columns:repeat(4,1fr); }}
@media(max-width:900px) {{ .grid-2,.grid-3,.grid-4 {{ grid-template-columns:1fr; }} }}

/* Cards */
.card {{ background:var(--card); border:1px solid var(--border); border-radius:12px; padding:1.2rem; }}
.card-title {{ font-size:0.9rem; color:var(--muted); margin-bottom:0.8rem; display:flex; align-items:center; gap:0.4rem; }}

/* KPI */
.kpi-card {{ text-align:center; padding:1rem; }}
.kpi-val {{ font-size:2rem; font-weight:700; background:linear-gradient(135deg,var(--warm),var(--cool)); -webkit-background-clip:text; -webkit-text-fill-color:transparent; background-clip:text; }}
.kpi-label {{ font-size:0.75rem; color:var(--muted); margin-top:0.2rem; }}
.kpi-diff {{ font-size:0.75rem; margin-top:0.3rem; }}

/* Table */
table {{ width:100%; border-collapse:collapse; font-size:0.85rem; }}
th {{ background:linear-gradient(90deg,rgba(255,229,199,0.1),rgba(193,231,232,0.1)); padding:0.6rem; text-align:left; color:var(--warm); border-bottom:2px solid var(--border); }}
td {{ padding:0.5rem 0.6rem; border-bottom:1px solid var(--border); }}
tr:hover {{ background:rgba(255,229,199,0.03); }}
.p-instagram {{ color:#e1306c; font-weight:700; }}
.p-threads {{ color:var(--accent2); font-weight:700; }}
.p-x {{ color:var(--accent1); font-weight:700; }}
.content-cell {{ font-size:0.78rem; max-width:250px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}

/* Badges */
.badge {{ background:rgba(212,149,107,0.25); color:var(--accent1); padding:2px 10px; border-radius:5px; font-size:0.75rem; font-weight:600; }}
.badge.secondary {{ background:rgba(107,163,165,0.2); color:var(--accent2); }}

/* Patterns */
.pattern-card {{ background:var(--card2); border-radius:8px; padding:0.8rem; margin-bottom:0.6rem; border-left:3px solid var(--accent1); }}
.pattern-header {{ display:flex; gap:0.4rem; margin-bottom:0.3rem; }}
.pattern-title {{ font-weight:600; font-size:0.9rem; margin-bottom:0.2rem; }}
.pattern-desc {{ font-size:0.8rem; color:var(--muted); }}

/* Insights */
.insight-item {{ padding:0.6rem 0; border-bottom:1px solid var(--border); font-size:0.85rem; }}
.insight-item:last-child {{ border:none; }}

/* Staff */
.staff-card {{ background:var(--card2); border-radius:10px; padding:1rem; margin-bottom:0.8rem; border:1px solid var(--border); }}
.staff-header {{ display:flex; justify-content:space-between; align-items:center; margin-bottom:0.6rem; }}
.staff-plat {{ font-size:0.75rem; color:var(--muted); }}
.staff-kpis {{ display:grid; grid-template-columns:repeat(4,1fr); gap:0.5rem; margin-bottom:0.5rem; }}
.staff-kpi {{ text-align:center; background:var(--card); border-radius:6px; padding:0.4rem; }}
.sk-val {{ font-size:1.1rem; font-weight:700; color:var(--accent1); }}
.sk-label {{ font-size:0.7rem; color:var(--muted); }}
.staff-comp {{ font-size:0.8rem; color:var(--muted); padding:1px 0; }}
.staff-pattern {{ font-size:0.8rem; padding:1px 0; }}

/* Arrows */
.up {{ color:var(--success); font-weight:600; }}
.down {{ color:var(--danger); font-weight:600; }}
.flat {{ color:var(--muted); }}

/* Chart containers */
.chart-container {{ position:relative; height:280px; }}
.chart-container-sm {{ position:relative; height:220px; }}

/* Heatmap */
.heatmap {{ display:grid; grid-template-columns: 50px repeat(6, 1fr); gap:3px; }}
.hm-header {{ font-size:0.7rem; color:var(--muted); text-align:center; padding:4px 2px; font-weight:600; }}
.hm-label {{ font-size:0.8rem; color:var(--text); display:flex; align-items:center; font-weight:600; }}
.hm-cell {{ border-radius:5px; text-align:center; padding:6px 2px; font-size:0.75rem; min-height:38px; display:flex; flex-direction:column; justify-content:center; align-items:center; transition:transform 0.15s; }}
.hm-cell:hover {{ transform:scale(1.08); z-index:1; }}
.hm-cell .hm-val {{ font-weight:700; font-size:0.85rem; }}
.hm-cell .hm-cnt {{ font-size:0.6rem; color:rgba(255,255,255,0.7); }}

/* Velocity */
.velocity-card {{ background:var(--card2); border-radius:10px; padding:1rem; text-align:center; border:1px solid var(--border); }}
.velocity-val {{ font-size:1.8rem; font-weight:700; }}
.velocity-label {{ font-size:0.8rem; color:var(--muted); margin-top:0.2rem; }}
.velocity-sub {{ font-size:0.75rem; color:var(--muted); margin-top:0.3rem; }}

/* Best timing highlight */
.best-timing {{ background:linear-gradient(135deg, rgba(212,149,107,0.15), rgba(107,163,165,0.15)); border:1px solid var(--accent1); border-radius:10px; padding:1rem; margin-bottom:1rem; display:flex; gap:2rem; justify-content:center; }}
.best-item {{ text-align:center; }}
.best-item .bi-val {{ font-size:1.5rem; font-weight:700; background:linear-gradient(135deg,var(--warm),var(--cool)); -webkit-background-clip:text; -webkit-text-fill-color:transparent; background-clip:text; }}
.best-item .bi-label {{ font-size:0.8rem; color:var(--muted); }}
.best-item .bi-likes {{ font-size:0.75rem; color:var(--accent1); margin-top:2px; }}
</style>
</head>
<body>
<div class="dashboard">
  <div class="dash-header">
    <div>
      <h1>📊 SNS Analytics Dashboard</h1>
      <div class="subtitle">ミートキャリア | {now}</div>
    </div>
    <div style="text-align:right;">
      <div style="font-size:0.8rem; color:#5A524A;">データ期間</div>
      <div style="font-weight:600; color:#2A2520;">過去30日間</div>
    </div>
  </div>

  <div class="tabs">
    <button class="tab active" onclick="showPage('overview')">📈 オーバービュー</button>
    <button class="tab" onclick="showPage('content')">📝 コンテンツ分析</button>
    <button class="tab" onclick="showPage('timing')">⏰ タイミング分析</button>
    <button class="tab" onclick="showPage('traffic')">🔗 流入分析</button>
    <button class="tab" onclick="showPage('team')">👤 チーム・インサイト</button>
  </div>

  <!-- ===== Page 1: Overview ===== -->
  <div class="page active" id="page-overview">
    <div class="grid grid-4" style="margin-bottom:1rem;">
      <div class="card kpi-card">
        <div class="kpi-val">{total_posts}</div>
        <div class="kpi-label">総投稿数</div>
      </div>
      <div class="card kpi-card">
        <div class="kpi-val">{total_views:,}</div>
        <div class="kpi-label">総閲覧数</div>
      </div>
      <div class="card kpi-card">
        <div class="kpi-val">{total_likes:,}</div>
        <div class="kpi-label">総いいね</div>
      </div>
      <div class="card kpi-card">
        <div class="kpi-val">{total_sessions:,}</div>
        <div class="kpi-label">サイト流入</div>
      </div>
    </div>

    <div class="grid grid-2">
      <div class="card">
        <div class="card-title">📈 週次いいねトレンド</div>
        <div class="chart-container"><canvas id="trendChart"></canvas></div>
      </div>
      <div class="card">
        <div class="card-title">📊 週次投稿数トレンド</div>
        <div class="chart-container"><canvas id="postsChart"></canvas></div>
      </div>
    </div>

    <div class="card" style="margin-top:1rem;">
      <div class="card-title">📅 前週比較</div>
      <table>
        <thead><tr><th>Platform</th><th>前週投稿</th><th>今週投稿</th><th>変化</th><th>前週Like</th><th>今週Like</th><th>変化</th></tr></thead>
        <tbody>
        {"".join(f'''<tr>
          <td class="p-{p}">{p.upper()}</td>
          <td>{comparison.get(p,{}).get("prev_week",{}).get("posts",0)}</td>
          <td>{comparison.get(p,{}).get("this_week",{}).get("posts",0)}</td>
          <td>{_diff_str(comparison.get(p,{}), "posts")}</td>
          <td>{comparison.get(p,{}).get("prev_week",{}).get("likes",0)}</td>
          <td>{comparison.get(p,{}).get("this_week",{}).get("likes",0)}</td>
          <td>{_diff_str(comparison.get(p,{}), "likes")}</td>
        </tr>''' for p in ["instagram","threads","x"])}
        </tbody>
      </table>
    </div>

    <div class="card" style="margin-top:1rem;">
      <div class="card-title">🏆 TOP投稿</div>
      <table>
        <thead><tr><th>#</th><th>Platform</th><th>内容</th><th>Like</th><th>Reply</th><th>Views</th></tr></thead>
        <tbody>{top_posts_rows}</tbody>
      </table>
    </div>
  </div>

  <!-- ===== Page 2: Content Analysis ===== -->
  <div class="page" id="page-content">
    <div class="grid grid-2">
      <div class="card">
        <div class="card-title">📝 テーマ別 平均いいね</div>
        <div class="chart-container"><canvas id="themeChart"></canvas></div>
      </div>
      <div class="card">
        <div class="card-title">📣 CTA有無比較（avg Like）</div>
        <div class="chart-container"><canvas id="ctaChart"></canvas></div>
      </div>
    </div>
    <div class="grid grid-2" style="margin-top:1rem;">
      <div class="card">
        <div class="card-title">🪝 フック表現別 平均いいね</div>
        <div class="chart-container"><canvas id="hookChart"></canvas></div>
      </div>
      <div class="card">
        <div class="card-title">🎯 勝ちパターン</div>
        <div style="max-height:280px; overflow-y:auto;">{patterns_html}</div>
      </div>
    </div>
  </div>

  <!-- ===== Page 3: Timing Analysis ===== -->
  <div class="page" id="page-timing">
    {_build_timing_section(posting_time, engagement_velocity)}
  </div>

  <!-- ===== Page 4: Traffic ===== -->
  <div class="page" id="page-traffic">
    <div class="grid grid-2">
      <div class="card">
        <div class="card-title">🔗 プラットフォーム別セッション</div>
        <div class="chart-container"><canvas id="ga4PieChart"></canvas></div>
      </div>
      <div class="card">
        <div class="card-title">📊 セッション vs エンゲージド</div>
        <div class="chart-container"><canvas id="ga4BarChart"></canvas></div>
      </div>
    </div>
    <div class="card" style="margin-top:1rem;">
      <div class="card-title">📅 GA4前週比較</div>
      <table>
        <thead><tr><th>Platform</th><th>前週セッション</th><th>今週セッション</th><th>変化</th><th>前週エンゲージ</th><th>今週エンゲージ</th><th>変化</th></tr></thead>
        <tbody>
        {"".join(f'''<tr>
          <td class="p-{p}">{p.upper()}</td>
          <td>{ga4_comp.get(p,{}).get("prev_week",{}).get("sessions",0)}</td>
          <td>{ga4_comp.get(p,{}).get("this_week",{}).get("sessions",0)}</td>
          <td>{_diff_str(ga4_comp.get(p,{}), "sessions")}</td>
          <td>{ga4_comp.get(p,{}).get("prev_week",{}).get("engaged",0)}</td>
          <td>{ga4_comp.get(p,{}).get("this_week",{}).get("engaged",0)}</td>
          <td>{_diff_str(ga4_comp.get(p,{}), "engaged")}</td>
        </tr>''' for p in ["instagram","threads","x"])}
        </tbody>
      </table>
    </div>
  </div>

  <!-- ===== Page 5: Team & Insights ===== -->
  <div class="page" id="page-team">
    <div class="grid grid-2">
      <div class="card">
        <div class="card-title">👤 担当者別レポート</div>
        {staff_cards}
      </div>
      <div class="card">
        <div class="card-title">💡 自動生成インサイト</div>
        {insights_html}
      </div>
    </div>
  </div>
</div>

<script>
// Tab navigation
function showPage(name) {{
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.getElementById('page-'+name).classList.add('active');
  event.target.classList.add('active');
}}

// Chart defaults
Chart.defaults.color = '#9A958D';
Chart.defaults.borderColor = '#2E333C';
Chart.defaults.font.family = "'Inter','Hiragino Sans',sans-serif";

const warm = '#FFE5C7', cool = '#C1E7E8', accent1 = '#D4956B', accent2 = '#6BA3A5';

// 1. Weekly Likes Trend
new Chart(document.getElementById('trendChart'), {{
  type: 'line',
  data: {{
    labels: {week_labels},
    datasets: [
      {{ label: 'Instagram', data: {ig_likes}, borderColor: '#e1306c', backgroundColor: 'rgba(225,48,108,0.1)', fill: true, tension: 0.4 }},
      {{ label: 'Threads', data: {th_likes}, borderColor: accent2, backgroundColor: 'rgba(107,163,165,0.1)', fill: true, tension: 0.4 }},
      {{ label: 'X', data: {x_likes}, borderColor: accent1, backgroundColor: 'rgba(212,149,107,0.1)', fill: true, tension: 0.4 }}
    ]
  }},
  options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ position: 'bottom' }} }} }}
}});

// 2. Weekly Posts
new Chart(document.getElementById('postsChart'), {{
  type: 'bar',
  data: {{
    labels: {week_labels},
    datasets: [
      {{ label: 'Instagram', data: {ig_posts}, backgroundColor: 'rgba(225,48,108,0.7)' }},
      {{ label: 'Threads', data: {th_posts}, backgroundColor: 'rgba(107,163,165,0.7)' }},
      {{ label: 'X', data: {x_posts}, backgroundColor: 'rgba(212,149,107,0.7)' }}
    ]
  }},
  options: {{ responsive: true, maintainAspectRatio: false, scales: {{ x: {{ stacked: true }}, y: {{ stacked: true }} }}, plugins: {{ legend: {{ position: 'bottom' }} }} }}
}});

// 3. Theme Bar
new Chart(document.getElementById('themeChart'), {{
  type: 'bar',
  data: {{
    labels: {theme_labels},
    datasets: [{{ label: '平均いいね', data: {theme_likes}, backgroundColor: 'rgba(212,149,107,0.7)', borderRadius: 6 }}]
  }},
  options: {{ responsive: true, maintainAspectRatio: false, indexAxis: 'y', plugins: {{ legend: {{ display: false }} }} }}
}});

// 4. CTA comparison
new Chart(document.getElementById('ctaChart'), {{
  type: 'bar',
  data: {{
    labels: {cta_labels},
    datasets: [
      {{ label: 'CTA有', data: {cta_with}, backgroundColor: 'rgba(93,155,122,0.7)', borderRadius: 6 }},
      {{ label: 'CTA無', data: {cta_without}, backgroundColor: 'rgba(196,101,90,0.5)', borderRadius: 6 }}
    ]
  }},
  options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ position: 'bottom' }} }} }}
}});

// 5. Hook patterns
new Chart(document.getElementById('hookChart'), {{
  type: 'bar',
  data: {{
    labels: {hook_labels},
    datasets: [{{ label: '平均いいね', data: {hook_avg_likes}, backgroundColor: 'rgba(107,163,165,0.7)', borderRadius: 6 }}]
  }},
  options: {{ responsive: true, maintainAspectRatio: false, indexAxis: 'y', plugins: {{ legend: {{ display: false }} }} }}
}});

// 6. GA4 Pie
new Chart(document.getElementById('ga4PieChart'), {{
  type: 'doughnut',
  data: {{
    labels: {ga4_labels},
    datasets: [{{ data: {ga4_sessions}, backgroundColor: ['#e1306c','rgba(107,163,165,0.8)','rgba(212,149,107,0.8)','rgba(93,155,122,0.8)','rgba(142,128,180,0.8)'], borderWidth: 0 }}]
  }},
  options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ position: 'bottom' }} }}, cutout: '60%' }}
}});

// 7. GA4 Bar
new Chart(document.getElementById('ga4BarChart'), {{
  type: 'bar',
  data: {{
    labels: {ga4_labels},
    datasets: [
      {{ label: 'セッション', data: {ga4_sessions}, backgroundColor: 'rgba(212,149,107,0.7)', borderRadius: 6 }},
      {{ label: 'エンゲージド', data: {ga4_engaged}, backgroundColor: 'rgba(107,163,165,0.7)', borderRadius: 6 }}
    ]
  }},
  options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ position: 'bottom' }} }} }}
}});
</script>
</body>
</html>'''


if __name__ == "__main__":
    path = generate_dashboard()
    if path:
        print(f"\n✅ ダッシュボード生成完了: {path}")
