"""
Looker Studio連携エクスポーター
SQLiteデータをLooker Studio対応のCSV + Googleスプレッドシートに出力

使い方:
  1. python exporters/looker_export.py                → CSV出力
  2. python exporters/looker_export.py --sheets        → Google Sheets書き込み
  3. Looker Studioでスプレッドシートをデータソースに指定
"""
import csv
import sqlite3
import json
import sys
from pathlib import Path
from datetime import datetime, date, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DB_PATH, REPORT_OUTPUT_DIR, STAFF_ASSIGNMENTS

EXPORT_DIR = REPORT_OUTPUT_DIR / "looker_data"


def get_connection():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def export_post_performance() -> Path:
    """投稿パフォーマンス（非正規化フラットテーブル）"""
    conn = get_connection()
    rows = conn.execute("""
        SELECT 
            p.platform,
            p.post_type,
            p.posted_at,
            DATE(p.posted_at) as post_date,
            CAST(SUBSTR(p.posted_at, 12, 2) AS INTEGER) as post_hour,
            CASE 
                WHEN CAST(strftime('%w', DATE(p.posted_at)) AS INTEGER) IN (0,6) THEN '休日'
                ELSE '平日'
            END as day_type,
            strftime('%W', DATE(p.posted_at)) as week_number,
            LENGTH(p.content) as content_length,
            CASE 
                WHEN LENGTH(p.content) <= 100 THEN '~100字'
                WHEN LENGTH(p.content) <= 200 THEN '101~200字'
                WHEN LENGTH(p.content) <= 300 THEN '201~300字'
                ELSE '301字~'
            END as length_bucket,
            p.permalink,
            SUBSTR(p.content, 1, 100) as content_preview,
            COALESCE(m.views, 0) as views,
            COALESCE(m.likes, 0) as likes,
            COALESCE(m.replies, 0) as replies,
            COALESCE(m.reposts, 0) as reposts,
            COALESCE(m.saves, 0) as saves,
            COALESCE(m.engagement_rate, 0) as engagement_rate,
            (COALESCE(m.likes, 0) + COALESCE(m.replies, 0) + COALESCE(m.reposts, 0) + COALESCE(m.saves, 0)) as total_engagement
        FROM posts p
        LEFT JOIN post_metrics m ON p.id = m.post_id
        ORDER BY p.posted_at DESC
    """).fetchall()
    conn.close()

    path = EXPORT_DIR / "post_performance.csv"
    _write_csv(path, rows)
    print(f"  📄 投稿パフォーマンス: {len(rows)}行 → {path.name}")
    return path


def export_ga4_traffic() -> Path:
    """GA4 SNS流入（日次フラット）"""
    conn = get_connection()
    rows = conn.execute("""
        SELECT 
            date,
            source,
            medium,
            campaign,
            content,
            landing_page,
            sessions,
            engaged_sessions,
            ROUND(CAST(engaged_sessions AS FLOAT) / NULLIF(sessions, 0) * 100, 1) as engagement_rate,
            avg_session_duration,
            conversions,
            ROUND(CAST(conversions AS FLOAT) / NULLIF(sessions, 0) * 100, 2) as conversion_rate,
            CASE 
                WHEN LOWER(source) IN ('instagram', 'ig', 'l.instagram.com') THEN 'INSTAGRAM'
                WHEN LOWER(source) IN ('threads', 'threads.net') THEN 'THREADS'
                WHEN LOWER(source) IN ('t.co', 'x.com', 'twitter.com', 'x') THEN 'X'
                ELSE UPPER(source)
            END as platform
        FROM ga4_sessions
        ORDER BY date DESC
    """).fetchall()
    conn.close()

    path = EXPORT_DIR / "ga4_traffic.csv"
    _write_csv(path, rows)
    print(f"  📄 GA4流入データ: {len(rows)}行 → {path.name}")
    return path


def export_weekly_summary() -> Path:
    """週次サマリー（Looker Studioのスコアカード用）"""
    conn = get_connection()

    # 直近8週分のデータを生成
    rows = []
    for offset in range(-7, 1):
        today = date.today()
        monday = today - timedelta(days=today.weekday())
        week_start = monday + timedelta(weeks=offset)
        week_end = week_start + timedelta(days=6)
        week_label = f"{week_start.strftime('%m/%d')}~{week_end.strftime('%m/%d')}"

        for platform in ["instagram", "threads", "x"]:
            ps = conn.execute("""
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
            """, (platform, week_start.isoformat(), (week_end + timedelta(days=1)).isoformat())).fetchone()

            ga4 = conn.execute("""
                SELECT 
                    COALESCE(SUM(sessions), 0) as sessions,
                    COALESCE(SUM(engaged_sessions), 0) as engaged,
                    COALESCE(SUM(conversions), 0) as conversions
                FROM ga4_sessions
                WHERE date >= ? AND date <= ?
                AND LOWER(source) IN (
                    CASE ? 
                        WHEN 'instagram' THEN 'instagram'
                        WHEN 'threads' THEN 'threads'
                        WHEN 'x' THEN 't.co'
                    END,
                    CASE ? 
                        WHEN 'instagram' THEN 'ig'
                        WHEN 'threads' THEN 'threads.net'
                        WHEN 'x' THEN 'x.com'
                    END,
                    CASE ? 
                        WHEN 'instagram' THEN 'l.instagram.com'
                        WHEN 'threads' THEN 'threads'
                        WHEN 'x' THEN 'x'
                    END
                )
            """, (week_start.isoformat(), week_end.isoformat(), platform, platform, platform)).fetchone()

            rows.append({
                "week_start": week_start.isoformat(),
                "week_end": week_end.isoformat(),
                "week_label": week_label,
                "platform": platform.upper(),
                "posts": ps["posts"] if ps else 0,
                "views": ps["views"] if ps else 0,
                "likes": ps["likes"] if ps else 0,
                "replies": ps["replies"] if ps else 0,
                "saves": ps["saves"] if ps else 0,
                "reposts": ps["reposts"] if ps else 0,
                "avg_eng_rate": ps["avg_eng_rate"] if ps else 0,
                "ga4_sessions": ga4["sessions"] if ga4 else 0,
                "ga4_engaged": ga4["engaged"] if ga4 else 0,
                "ga4_conversions": ga4["conversions"] if ga4 else 0,
            })

    conn.close()

    path = EXPORT_DIR / "weekly_summary.csv"
    _write_csv_from_dicts(path, rows)
    print(f"  📄 週次サマリー: {len(rows)}行 → {path.name}")
    return path


def export_analysis_insights() -> Path:
    """分析インサイト（勝ちパターン + 比較インサイト）"""
    json_path = REPORT_OUTPUT_DIR / "latest_analysis.json"
    if not json_path.exists():
        print("  ⚠️ latest_analysis.json が見つかりません。先に run_analysis.py を実行してください。")
        return None

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    rows = []

    # 勝ちパターン
    for i, wp in enumerate(data.get("winning_patterns", []), 1):
        rows.append({
            "type": "winning_pattern",
            "rank": i,
            "platform": wp.get("platform", "").upper(),
            "category": wp.get("category", ""),
            "pattern": wp.get("pattern", ""),
            "recommendation": wp.get("recommendation", ""),
            "metric_value": wp.get("metric_value", ""),
        })

    # 自動インサイト
    for i, ins in enumerate(data.get("insights", []), 1):
        rows.append({
            "type": "auto_insight",
            "rank": i,
            "platform": "",
            "category": "インサイト",
            "pattern": ins,
            "recommendation": "",
            "metric_value": "",
        })

    # 比較インサイト
    for i, ins in enumerate(data.get("comparison_insights", []), 1):
        rows.append({
            "type": "comparison_insight",
            "rank": i,
            "platform": "",
            "category": "前週比較",
            "pattern": ins,
            "recommendation": "",
            "metric_value": "",
        })

    path = EXPORT_DIR / "analysis_insights.csv"
    _write_csv_from_dicts(path, rows)
    print(f"  📄 分析インサイト: {len(rows)}行 → {path.name}")
    return path


def export_theme_analysis() -> Path:
    """テーマ別・CTA・フック分析（Looker Studioチャート用）"""
    json_path = REPORT_OUTPUT_DIR / "latest_analysis.json"
    if not json_path.exists():
        return None

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    rows = []

    # テーマ別
    for theme, d in data.get("theme_performance", {}).items():
        rows.append({
            "analysis_type": "テーマ別",
            "dimension": theme,
            "platform": "全体",
            "count": d.get("count", 0),
            "avg_likes": d.get("avg_likes", 0),
            "avg_replies": d.get("avg_replies", 0),
            "avg_engagement": d.get("avg_engagement", 0),
        })

    # CTA効果
    for platform, d in data.get("cta_impact", {}).items():
        wc = d.get("with_cta", {})
        woc = d.get("without_cta", {})
        rows.append({
            "analysis_type": "CTA有",
            "dimension": "CTA",
            "platform": platform.upper(),
            "count": wc.get("count", 0),
            "avg_likes": wc.get("avg_likes", 0),
            "avg_replies": 0,
            "avg_engagement": 0,
        })
        rows.append({
            "analysis_type": "CTA無",
            "dimension": "CTA",
            "platform": platform.upper(),
            "count": woc.get("count", 0),
            "avg_likes": woc.get("avg_likes", 0),
            "avg_replies": 0,
            "avg_engagement": 0,
        })

    # フック表現
    for platform, hooks in data.get("hook_patterns", {}).items():
        for name, d in hooks.items():
            rows.append({
                "analysis_type": "フック表現",
                "dimension": name,
                "platform": platform.upper(),
                "count": d.get("count", 0),
                "avg_likes": d.get("avg_likes", 0),
                "avg_replies": d.get("avg_replies", 0),
                "avg_engagement": d.get("avg_engagement", 0),
            })

    path = EXPORT_DIR / "content_analysis.csv"
    _write_csv_from_dicts(path, rows)
    print(f"  📄 コンテンツ分析: {len(rows)}行 → {path.name}")
    return path


def _write_csv(path: Path, rows):
    """sqlite3.Row リストをCSVに書き出す"""
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    keys = rows[0].keys()
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for row in rows:
            writer.writerow(dict(row))


def _write_csv_from_dicts(path: Path, rows: list[dict]):
    """dict リストをCSVに書き出す"""
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    keys = rows[0].keys()
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def generate_sheets_setup_script() -> Path:
    """Google SheetsへのインポートとLooker Studio接続手順を生成"""
    path = EXPORT_DIR / "LOOKER_STUDIO_SETUP.md"
    path.parent.mkdir(parents=True, exist_ok=True)

    csv_files = list(EXPORT_DIR.glob("*.csv"))
    file_list = "\n".join(f"   - `{f.name}`" for f in csv_files)

    content = f"""# Looker Studio セットアップ手順

## 生成日: {datetime.now().strftime('%Y-%m-%d %H:%M')}

---

## Step 1: CSVをGoogleスプレッドシートにインポート

1. [Google Drive](https://drive.google.com) を開く
2. 「新規」→「フォルダ」→「SNS_Performance_Data」を作成
3. 以下のCSVファイルをそれぞれアップロード:
{file_list}
4. 各CSVを右クリック →「アプリで開く」→「Google スプレッドシート」

> 💡 **TIP**: 1つのスプレッドシートに複数シートとしてインポートすると管理が楽です。
> ファイル → インポート → アップロード → 「新しいシートとして挿入」

---

## Step 2: Looker Studioでデータソースを追加

1. [Looker Studio](https://lookerstudio.google.com) を開く
2. 「空のレポート」→「データを追加」
3. コネクタ: **Google スプレッドシート** を選択
4. 以下の各シートを追加:

| データソース名 | シート/ファイル | 用途 |
|--|--|--|
| `投稿パフォーマンス` | post_performance | 投稿別の詳細分析 |
| `GA4流入` | ga4_traffic | SNS→サイト流入分析 |
| `週次サマリー` | weekly_summary | 週次KPIトレンド |
| `コンテンツ分析` | content_analysis | テーマ・CTA・フック分析 |
| `分析インサイト` | analysis_insights | 勝ちパターン表示 |

---

## Step 3: 推奨ダッシュボード構成

### ページ1: KPIオーバービュー
- **スコアカード**: 今週の投稿数 / いいね数 / 閲覧数 / サイト流入
  - データソース: `週次サマリー`
  - フィルタ: `week_start` = 最新週
- **時系列チャート**: 週次KPI推移
  - X軸: `week_label`、Y軸: `likes`, `views`
  - 分割: `platform`

### ページ2: 投稿パフォーマンス
- **テーブル**: 投稿一覧（ソート: total_engagement DESC）
  - データソース: `投稿パフォーマンス`
- **棒グラフ**: プラットフォーム別エンゲージメント
  - X軸: `platform`、Y軸: SUM(`likes`)
- **ヒートマップ**: 投稿時間帯 × 曜日
  - データソース: `投稿パフォーマンス`
  - X軸: `post_hour`、色: AVG(`engagement_rate`)

### ページ3: GA4流入分析
- **円グラフ**: プラットフォーム別セッション割合
  - データソース: `GA4流入`
  - ディメンション: `platform`、指標: SUM(`sessions`)
- **テーブル**: LP別セッション数
  - データソース: `GA4流入`

### ページ4: コンテンツ分析
- **棒グラフ**: テーマ別平均いいね
  - データソース: `コンテンツ分析`
  - フィルタ: `analysis_type` = "テーマ別"
- **棒グラフ**: CTA有無比較
  - フィルタ: `dimension` = "CTA"
- **テーブル**: 勝ちパターン一覧
  - データソース: `分析インサイト`
  - フィルタ: `type` = "winning_pattern"

---

## Step 4: 自動更新の設定

### 週次バッチ更新（推奨）
```bash
# cronで毎週月曜に実行
0 9 * * 1 cd {Path(__file__).parent.parent} && source venv/bin/activate && python run_collection.py --ga4-data data/ga4_sns_sessions.json && python run_analysis.py && python exporters/looker_export.py
```

### Google Sheetsの自動更新
- スプレッドシートを開き、更新CSVをインポート（「現在のシートを置換」）
- Looker Studioのデータソースは自動的に最新データを反映

---

## データスキーマ

### post_performance.csv
| カラム | 型 | 説明 |
|--|--|--|
| platform | TEXT | instagram / threads / x |
| post_type | TEXT | IMAGE / CAROUSEL / TEXT_POST 等 |
| post_date | DATE | 投稿日 |
| post_hour | INT | 投稿時間（0-23） |
| content_length | INT | 文字数 |
| length_bucket | TEXT | 文字数帯 |
| views / likes / replies / reposts / saves | INT | 各指標 |
| engagement_rate | FLOAT | エンゲージメント率 |
| total_engagement | INT | いいね+リプ+RT+保存 |

### weekly_summary.csv
| カラム | 型 | 説明 |
|--|--|--|
| week_start / week_end | DATE | 週の範囲 |
| week_label | TEXT | 表示用ラベル |
| platform | TEXT | INSTAGRAM / THREADS / X |
| posts / views / likes | INT | 週次KPI |
| ga4_sessions / ga4_conversions | INT | GA4指標 |
"""

    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"  📋 セットアップ手順書: {path.name}")
    return path


def run_full_export() -> dict:
    """全エクスポートを実行"""
    print("=" * 60)
    print("📤 Looker Studio向けデータエクスポート")
    print("=" * 60)

    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    paths = {
        "post_performance": export_post_performance(),
        "ga4_traffic": export_ga4_traffic(),
        "weekly_summary": export_weekly_summary(),
        "analysis_insights": export_analysis_insights(),
        "content_analysis": export_theme_analysis(),
        "setup_guide": generate_sheets_setup_script(),
    }

    print(f"\n✅ エクスポート完了: {EXPORT_DIR}")
    print(f"   CSVファイル: {len([p for p in paths.values() if p and p.suffix == '.csv'])}個")
    print(f"\n📋 次のステップ:")
    print(f"   1. {EXPORT_DIR}/LOOKER_STUDIO_SETUP.md を参照")
    print(f"   2. CSVをGoogleスプレッドシートにインポート")
    print(f"   3. Looker Studioでデータソースに接続")

    return paths


if __name__ == "__main__":
    run_full_export()
