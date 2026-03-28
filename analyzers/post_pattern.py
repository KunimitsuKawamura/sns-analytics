"""
勝ちパターン抽出エンジン
CV貢献度の高い投稿の構成要素を分解し、再現可能なパターンを特定
"""
import re
import sqlite3
from datetime import date, datetime, timedelta, timezone
from collections import defaultdict, Counter
from config import DB_PATH

# タイムゾーン定数
JST = timezone(timedelta(hours=9))

# 曜日名（日本語）
WEEKDAY_NAMES = ["月", "火", "水", "木", "金", "土", "日"]

# 時間帯区分
HOUR_SLOTS = {
    "早朝(5-8時)": (5, 8),
    "午前(9-11時)": (9, 11),
    "午後(12-14時)": (12, 14),
    "夕方(15-17時)": (15, 17),
    "夜(18-21時)": (18, 21),
    "深夜(22-4時)": (22, 4),  # 跨ぎ注意
}


# === パターン定義 ===

# フック表現（投稿冒頭のパターン）
HOOK_PATTERNS = {
    "共感呼びかけ型": [
        r"(?:こんな|そんな|あんな)(?:経験|悩み|モヤモヤ|不安|こと)",
        r"(?:ありませんか|ないですか|ありますよね)",
        r"(?:わかります|気持ち|共感)",
    ],
    "問いかけ型": [
        r"^(?:「|『|＼).*(?:？|\?|／)",
        r"(?:知っていますか|ご存知ですか|気づいていますか)",
        r"(?:どう(?:思い|感じ|考え))",
    ],
    "数字・実績型": [
        r"(?:\d+[%％件名人万])",
        r"(?:満足度|実績|相談件数|フォロワー)",
        r"(?:TOP|ランキング|No\.?\s*\d)",
    ],
    "ノウハウ提供型": [
        r"(?:\d+(?:つの|個の|選|ステップ|STEP))",
        r"(?:方法|コツ|ポイント|秘訣|テクニック)",
        r"(?:まとめ|解説|紹介|ガイド)",
    ],
    "緊急・限定型": [
        r"(?:明日|本日|今日|今週|今月|期間限定)",
        r"(?:締切|残り|ラスト|最後|急いで)",
        r"(?:お見逃しなく|開催|募集)",
    ],
    "ストーリー型": [
        r"(?:実は|じつは|正直|ぶっちゃけ)",
        r"(?:私が|私は|わたしが|僕が)",
        r"(?:体験|経験談|実話|リアル)",
    ],
    "お悩み相談型": [
        r"(?:お悩み|相談|モヤモヤ|DM)",
        r"(?:募集|回答|お答え|アドバイス)",
        r"(?:＼DMで|\\DMで|お気軽に)",
    ],
}

# CTA表現
CTA_PATTERNS = [
    r"プロフィール(?:から|リンク|へ)",
    r"(?:ご予約|予約|申し込み|お申込)",
    r"(?:無料相談|無料カウンセリング|無料体験)",
    r"(?:リンク(?:から|は|を)|URL)",
    r"(?:DMで|DMへ|DM受付)",
    r"(?:詳しくは|詳細は|チェック)",
    r"@\w+",  # メンション
    r"👉|➡|→|▶",  # 矢印系
]

# テーマキーワード
THEME_KEYWORDS = {
    "育休・復職": ["育休", "復職", "復帰", "産休", "育児休業"],
    "転職": ["転職", "キャリアチェンジ", "転職活動", "退職"],
    "キャリア相談": ["相談", "カウンセリング", "メンター", "コーチング", "キャリア相談"],
    "強み・自己分析": ["強み", "自己分析", "適性", "適職", "長所", "スキル"],
    "仕事の悩み": ["モヤモヤ", "悩み", "向いていない", "辛い", "ストレス", "不安"],
    "ワーママ": ["ワーママ", "ワーキングマザー", "仕事と育児", "両立", "時短"],
    "小1の壁": ["小1の壁", "小一の壁", "学童", "放課後"],
    "働き方": ["働き方", "リモート", "フレックス", "副業", "パート"],
    "セミナー・イベント": ["セミナー", "イベント", "開催", "参加", "ウェビナー"],
}


def get_connection():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def analyze_content_length_vs_engagement() -> dict:
    """文字数帯 × エンゲージメント率の相関"""
    conn = get_connection()
    rows = conn.execute("""
        SELECT p.platform, LENGTH(p.content) as chars,
               m.likes, m.replies, m.saves, m.views, m.engagement_rate
        FROM posts p JOIN post_metrics m ON p.id = m.post_id
        WHERE p.content IS NOT NULL AND LENGTH(p.content) > 0
    """).fetchall()
    conn.close()

    # 文字数帯に分類
    buckets = {
        "~50字": (0, 50),
        "51~100字": (51, 100),
        "101~200字": (101, 200),
        "201~300字": (201, 300),
        "301字~": (301, 9999),
    }

    results = {}
    for platform in ["instagram", "threads", "x"]:
        platform_rows = [r for r in rows if r["platform"] == platform]
        if not platform_rows:
            continue

        bucket_data = {}
        for bucket_name, (low, high) in buckets.items():
            matching = [r for r in platform_rows if low <= (r["chars"] or 0) <= high]
            if matching:
                total_eng = sum((r["likes"] or 0) + (r["replies"] or 0) + (r["saves"] or 0) for r in matching)
                bucket_data[bucket_name] = {
                    "count": len(matching),
                    "avg_likes": round(sum(r["likes"] or 0 for r in matching) / len(matching), 1),
                    "avg_engagement": round(
                        sum(r["engagement_rate"] or 0 for r in matching) / len(matching), 2
                    ),
                    "total_engagement": total_eng,
                }
        results[platform] = bucket_data

    return results


def analyze_cta_impact() -> dict:
    """CTA有無 × パフォーマンス"""
    conn = get_connection()
    rows = conn.execute("""
        SELECT p.platform, p.content,
               m.likes, m.replies, m.saves, m.views, m.engagement_rate
        FROM posts p JOIN post_metrics m ON p.id = m.post_id
        WHERE p.content IS NOT NULL
    """).fetchall()
    conn.close()

    results = {}
    for platform in ["instagram", "threads", "x"]:
        platform_rows = [r for r in rows if r["platform"] == platform]
        if not platform_rows:
            continue

        with_cta = []
        without_cta = []
        for r in platform_rows:
            has_cta = any(re.search(pat, r["content"] or "", re.IGNORECASE) for pat in CTA_PATTERNS)
            if has_cta:
                with_cta.append(r)
            else:
                without_cta.append(r)

        results[platform] = {
            "with_cta": _aggregate_group(with_cta),
            "without_cta": _aggregate_group(without_cta),
            "cta_lift": _calc_lift(with_cta, without_cta),
        }

    return results


def analyze_hashtag_impact() -> dict:
    """ハッシュタグ数 × パフォーマンス"""
    conn = get_connection()
    rows = conn.execute("""
        SELECT p.platform, p.content,
               m.likes, m.replies, m.saves, m.views, m.engagement_rate
        FROM posts p JOIN post_metrics m ON p.id = m.post_id
        WHERE p.content IS NOT NULL
    """).fetchall()
    conn.close()

    results = {}
    for platform in ["instagram", "threads", "x"]:
        platform_rows = [r for r in rows if r["platform"] == platform]
        if not platform_rows:
            continue

        buckets = {"0個": [], "1~3個": [], "4~7個": [], "8個~": []}
        for r in platform_rows:
            tag_count = len(re.findall(r'#\w+', r["content"] or ""))
            if tag_count == 0:
                buckets["0個"].append(r)
            elif tag_count <= 3:
                buckets["1~3個"].append(r)
            elif tag_count <= 7:
                buckets["4~7個"].append(r)
            else:
                buckets["8個~"].append(r)

        results[platform] = {k: _aggregate_group(v) for k, v in buckets.items() if v}

    return results


def analyze_hook_patterns() -> dict:
    """投稿冒頭のフック表現パターン × パフォーマンス"""
    conn = get_connection()
    rows = conn.execute("""
        SELECT p.platform, p.content,
               m.likes, m.replies, m.saves, m.views, m.engagement_rate
        FROM posts p JOIN post_metrics m ON p.id = m.post_id
        WHERE p.content IS NOT NULL AND LENGTH(p.content) > 10
    """).fetchall()
    conn.close()

    results = {}
    for platform in ["instagram", "threads", "x"]:
        platform_rows = [r for r in rows if r["platform"] == platform]
        if not platform_rows:
            continue

        pattern_groups = defaultdict(list)
        for r in platform_rows:
            content = r["content"] or ""
            # 最初の100文字でフック判定
            head = content[:100]
            matched = False
            for pattern_name, regexes in HOOK_PATTERNS.items():
                if any(re.search(rx, head, re.IGNORECASE) for rx in regexes):
                    pattern_groups[pattern_name].append(r)
                    matched = True
                    break
            if not matched:
                pattern_groups["その他"].append(r)

        results[platform] = {
            name: {**_aggregate_group(group), "count": len(group)}
            for name, group in pattern_groups.items()
            if group
        }

    return results


def analyze_theme_performance() -> dict:
    """テーマ（キーワード） × パフォーマンス"""
    conn = get_connection()
    rows = conn.execute("""
        SELECT p.platform, p.content,
               m.likes, m.replies, m.saves, m.views, m.engagement_rate
        FROM posts p JOIN post_metrics m ON p.id = m.post_id
        WHERE p.content IS NOT NULL
    """).fetchall()
    conn.close()

    theme_groups = defaultdict(list)
    for r in rows:
        content = r["content"] or ""
        for theme, keywords in THEME_KEYWORDS.items():
            if any(kw in content for kw in keywords):
                theme_groups[theme].append(r)

    results = {}
    for theme, group in sorted(theme_groups.items(), key=lambda x: len(x[1]), reverse=True):
        if len(group) >= 2:  # 2件以上あるテーマのみ
            platform_breakdown = Counter(r["platform"] for r in group)
            results[theme] = {
                **_aggregate_group(group),
                "count": len(group),
                "platforms": dict(platform_breakdown),
            }

    return results


def analyze_emoji_impact() -> dict:
    """絵文字使用 × パフォーマンス"""
    conn = get_connection()
    rows = conn.execute("""
        SELECT p.platform, p.content,
               m.likes, m.replies, m.saves, m.views, m.engagement_rate
        FROM posts p JOIN post_metrics m ON p.id = m.post_id
        WHERE p.content IS NOT NULL
    """).fetchall()
    conn.close()

    emoji_pattern = re.compile(
        "["
        "\U0001F300-\U0001F9FF"
        "\U00002702-\U000027B0"
        "\U0001FA00-\U0001FA6F"
        "\U0001FA70-\U0001FAFF"
        "\U00002600-\U000026FF"
        "]+", flags=re.UNICODE
    )

    results = {}
    for platform in ["instagram", "threads", "x"]:
        platform_rows = [r for r in rows if r["platform"] == platform]
        if not platform_rows:
            continue

        buckets = {"なし": [], "少（1~3）": [], "中（4~8）": [], "多（9~）": []}
        for r in platform_rows:
            emoji_count = len(emoji_pattern.findall(r["content"] or ""))
            if emoji_count == 0:
                buckets["なし"].append(r)
            elif emoji_count <= 3:
                buckets["少（1~3）"].append(r)
            elif emoji_count <= 8:
                buckets["中（4~8）"].append(r)
            else:
                buckets["多（9~）"].append(r)

        results[platform] = {k: _aggregate_group(v) for k, v in buckets.items() if v}

    return results


def generate_winning_patterns(all_results: dict) -> list[dict]:
    """全分析結果から勝ちパターンを自動抽出"""
    patterns = []

    # 1. 文字数の最適帯を特定
    for platform, buckets in all_results.get("content_length", {}).items():
        if not buckets:
            continue
        best_bucket = max(buckets.items(),
                         key=lambda x: x[1].get("avg_likes", 0))
        if best_bucket[1]["count"] >= 2:
            patterns.append({
                "platform": platform,
                "category": "文字数",
                "pattern": f"{best_bucket[0]}が最もいいね獲得",
                "avg_likes": best_bucket[1]["avg_likes"],
                "sample_size": best_bucket[1]["count"],
                "recommendation": f"{platform.upper()}: {best_bucket[0]}の文字数帯が最もエンゲージメントが高い。この範囲を意識して投稿してください。",
            })

    # 2. CTA効果
    for platform, data in all_results.get("cta_impact", {}).items():
        lift = data.get("cta_lift", 0)
        if lift > 0:
            patterns.append({
                "platform": platform,
                "category": "CTA",
                "pattern": f"CTA付きの方が{lift:+.0f}%高い",
                "avg_likes": data["with_cta"].get("avg_likes", 0),
                "sample_size": data["with_cta"].get("count", 0),
                "recommendation": f"{platform.upper()}: CTAを含む投稿はいいねが{lift:+.0f}%向上。毎投稿にCTAを入れましょう。",
            })

    # 3. ハッシュタグの最適数
    for platform, buckets in all_results.get("hashtag_impact", {}).items():
        if not buckets:
            continue
        best_bucket = max(buckets.items(),
                         key=lambda x: x[1].get("avg_likes", 0))
        if best_bucket[1]["count"] >= 2:
            patterns.append({
                "platform": platform,
                "category": "ハッシュタグ",
                "pattern": f"{best_bucket[0]}がベスト",
                "avg_likes": best_bucket[1]["avg_likes"],
                "sample_size": best_bucket[1]["count"],
                "recommendation": f"{platform.upper()}: ハッシュタグは{best_bucket[0]}が最もパフォーマンスが高い。",
            })

    # 4. フック表現
    for platform, hooks in all_results.get("hook_patterns", {}).items():
        if not hooks:
            continue
        # 「その他」を除外
        named_hooks = {k: v for k, v in hooks.items() if k != "その他"}
        if named_hooks:
            best_hook = max(named_hooks.items(),
                           key=lambda x: x[1].get("avg_likes", 0))
            if best_hook[1]["count"] >= 2:
                patterns.append({
                    "platform": platform,
                    "category": "フック表現",
                    "pattern": f"「{best_hook[0]}」が最効果",
                    "avg_likes": best_hook[1]["avg_likes"],
                    "sample_size": best_hook[1]["count"],
                    "recommendation": f"{platform.upper()}: 投稿冒頭は「{best_hook[0]}」パターンが最もエンゲージメントが高い。",
                })

    # 5. テーマ
    themes = all_results.get("theme_performance", {})
    if themes:
        best_theme = max(themes.items(),
                        key=lambda x: x[1].get("avg_likes", 0))
        patterns.append({
            "platform": "全体",
            "category": "テーマ",
            "pattern": f"「{best_theme[0]}」が最もリアクション高",
            "avg_likes": best_theme[1]["avg_likes"],
            "sample_size": best_theme[1]["count"],
            "recommendation": f"テーマ「{best_theme[0]}」が最もいいねを獲得。このテーマの投稿頻度を増やしましょう。",
        })

    # 6. 投稿タイミング
    timing = all_results.get("posting_time", {})
    best_timing = timing.get("best_timing", {})
    best_wd = best_timing.get("best_weekday")
    best_hs = best_timing.get("best_hour_slot")
    if best_wd and best_wd.get("count", 0) >= 2:
        patterns.append({
            "platform": "全体",
            "category": "投稿曜日",
            "pattern": f"{best_wd['name']}曜日が最もエンゲージメント高",
            "avg_likes": best_wd["avg_likes"],
            "sample_size": best_wd["count"],
            "recommendation": f"{best_wd['name']}曜日の投稿が平均いいね{best_wd['avg_likes']}で最高。この曜日に重要な投稿を集中しましょう。",
        })
    if best_hs and best_hs.get("count", 0) >= 2:
        patterns.append({
            "platform": "全体",
            "category": "投稿時間帯",
            "pattern": f"{best_hs['name']}がベストタイム",
            "avg_likes": best_hs["avg_likes"],
            "sample_size": best_hs["count"],
            "recommendation": f"{best_hs['name']}の投稿が平均いいね{best_hs['avg_likes']}で最高。この時間帯に投稿しましょう。",
        })

    # スコア順にソート
    patterns.sort(key=lambda x: x.get("avg_likes", 0), reverse=True)
    return patterns


def _aggregate_group(rows: list) -> dict:
    """行グループの集約統計"""
    if not rows:
        return {"count": 0, "avg_likes": 0, "avg_engagement": 0, "total_engagement": 0}
    n = len(rows)
    return {
        "count": n,
        "avg_likes": round(sum(r["likes"] or 0 for r in rows) / n, 1),
        "avg_replies": round(sum(r["replies"] or 0 for r in rows) / n, 1),
        "avg_saves": round(sum(r["saves"] or 0 for r in rows) / n, 1),
        "avg_engagement": round(sum(r["engagement_rate"] or 0 for r in rows) / n, 2),
        "total_engagement": sum(
            (r["likes"] or 0) + (r["replies"] or 0) + (r["saves"] or 0)
            for r in rows
        ),
    }


def _calc_lift(with_group: list, without_group: list) -> float:
    """CTA有りvs無しのリフト率（%）"""
    if not with_group or not without_group:
        return 0.0
    avg_with = sum(r["likes"] or 0 for r in with_group) / len(with_group)
    avg_without = sum(r["likes"] or 0 for r in without_group) / len(without_group)
    if avg_without == 0:
        return 100.0 if avg_with > 0 else 0.0
    return round((avg_with - avg_without) / avg_without * 100, 1)


def _parse_posted_at_jst(posted_at_str: str) -> datetime | None:
    """posted_at文字列をJST datetimeに変換"""
    if not posted_at_str:
        return None
    try:
        # ISO8601フォーマット各種に対応
        s = posted_at_str.strip()
        # +0000 形式を +00:00 に正規化
        s = re.sub(r'([+-])(\d{2})(\d{2})$', r'\1\2:\3', s)
        dt = datetime.fromisoformat(s)
        # naiveならUTCとみなす
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(JST)
    except (ValueError, TypeError):
        return None


def _get_hour_slot(hour: int) -> str:
    """時刻（0-23）から時間帯名を返す"""
    for slot_name, (start, end) in HOUR_SLOTS.items():
        if start <= end:
            if start <= hour <= end:
                return slot_name
        else:  # 深夜帯（22-4）は跨ぎ
            if hour >= start or hour <= end:
                return slot_name
    return "深夜(22-4時)"


def analyze_posting_time_engagement() -> dict:
    """曜日/投稿時間 × エンゲージメント相関分析"""
    conn = get_connection()
    rows = conn.execute("""
        SELECT p.platform, p.posted_at,
               m.likes, m.replies, m.saves, m.views, m.engagement_rate
        FROM posts p JOIN post_metrics m ON p.id = m.post_id
        WHERE p.posted_at IS NOT NULL
    """).fetchall()
    conn.close()

    # --- 曜日別 ---
    weekday_data = defaultdict(lambda: defaultdict(list))  # {platform: {weekday_idx: [rows]}}
    # --- 時間帯別 ---
    hour_slot_data = defaultdict(lambda: defaultdict(list))
    # --- ヒートマップ: 曜日×時間帯 ---
    heatmap_data = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))  # {platform: {weekday: {slot: [rows]}}}
    # --- 全プラットフォーム集約 ---
    all_weekday = defaultdict(list)
    all_hour_slot = defaultdict(list)
    all_heatmap = defaultdict(lambda: defaultdict(list))  # {weekday: {slot: [rows]}}

    for r in rows:
        dt_jst = _parse_posted_at_jst(r["posted_at"])
        if dt_jst is None:
            continue
        weekday_idx = dt_jst.weekday()  # 0=月, 6=日
        hour = dt_jst.hour
        slot = _get_hour_slot(hour)
        platform = r["platform"]

        weekday_data[platform][weekday_idx].append(r)
        hour_slot_data[platform][slot].append(r)
        heatmap_data[platform][weekday_idx][slot].append(r)

        all_weekday[weekday_idx].append(r)
        all_hour_slot[slot].append(r)
        all_heatmap[weekday_idx][slot].append(r)

    # 集計
    results = {"by_platform": {}, "all": {}}

    for platform in ["instagram", "threads", "x"]:
        if platform not in weekday_data:
            continue
        wd = {}
        for idx in range(7):
            group = weekday_data[platform].get(idx, [])
            if group:
                wd[WEEKDAY_NAMES[idx]] = {**_aggregate_group(group), "weekday_idx": idx}
        hs = {}
        for slot_name in HOUR_SLOTS:
            group = hour_slot_data[platform].get(slot_name, [])
            if group:
                hs[slot_name] = _aggregate_group(group)

        # ヒートマップ (7曜日 × 6時間帯)
        hm = []
        slot_names = list(HOUR_SLOTS.keys())
        for day_idx in range(7):
            row_data = []
            for sn in slot_names:
                group = heatmap_data[platform].get(day_idx, {}).get(sn, [])
                avg_likes = round(sum(r["likes"] or 0 for r in group) / len(group), 1) if group else 0
                row_data.append({"count": len(group), "avg_likes": avg_likes})
            hm.append(row_data)

        results["by_platform"][platform] = {
            "by_weekday": wd,
            "by_hour_slot": hs,
            "heatmap": hm,
        }

    # 全体集約
    all_wd = {}
    for idx in range(7):
        group = all_weekday.get(idx, [])
        if group:
            all_wd[WEEKDAY_NAMES[idx]] = {**_aggregate_group(group), "weekday_idx": idx}
    all_hs = {}
    for slot_name in HOUR_SLOTS:
        group = all_hour_slot.get(slot_name, [])
        if group:
            all_hs[slot_name] = _aggregate_group(group)

    all_hm = []
    slot_names = list(HOUR_SLOTS.keys())
    for day_idx in range(7):
        row_data = []
        for sn in slot_names:
            group = all_heatmap.get(day_idx, {}).get(sn, [])
            avg_likes = round(sum(r["likes"] or 0 for r in group) / len(group), 1) if group else 0
            row_data.append({"count": len(group), "avg_likes": avg_likes})
        all_hm.append(row_data)

    results["all"] = {
        "by_weekday": all_wd,
        "by_hour_slot": all_hs,
        "heatmap": all_hm,
    }

    # ベストタイミング特定
    best = {"best_weekday": None, "best_hour_slot": None}
    if all_wd:
        best_day = max(all_wd.items(), key=lambda x: x[1].get("avg_likes", 0))
        best["best_weekday"] = {"name": best_day[0], "avg_likes": best_day[1]["avg_likes"], "count": best_day[1]["count"]}
    if all_hs:
        best_slot = max(all_hs.items(), key=lambda x: x[1].get("avg_likes", 0))
        best["best_hour_slot"] = {"name": best_slot[0], "avg_likes": best_slot[1]["avg_likes"], "count": best_slot[1]["count"]}
    results["best_timing"] = best

    results["slot_names"] = slot_names

    return results


def analyze_engagement_velocity() -> dict:
    """エンゲージメント初速分析 — 投稿からの経過日数とエンゲージメントの関係"""
    conn = get_connection()
    rows = conn.execute("""
        SELECT p.id, p.platform, p.posted_at,
               m.measured_at, m.likes, m.replies, m.saves, m.views,
               m.engagement_rate
        FROM posts p JOIN post_metrics m ON p.id = m.post_id
        WHERE p.posted_at IS NOT NULL AND m.measured_at IS NOT NULL
    """).fetchall()
    conn.close()

    # 経過日数バケット
    AGE_BUCKETS = {
        "新鮮(0-1日)": (0, 1),
        "中間(2-3日)": (2, 3),
        "成熟(4-7日)": (4, 7),
        "長期(8日+)": (8, 9999),
    }

    bucket_data = defaultdict(lambda: defaultdict(list))  # {platform: {bucket: [rows]}}
    all_bucket = defaultdict(list)

    # 同一投稿の複数計測を検出（将来対応）
    post_measurements = defaultdict(list)  # {post_id: [(measured_at, row), ...]}

    for r in rows:
        dt_posted = _parse_posted_at_jst(r["posted_at"])
        if dt_posted is None:
            continue
        try:
            measured = datetime.strptime(str(r["measured_at"]), "%Y-%m-%d").replace(tzinfo=JST)
        except (ValueError, TypeError):
            continue

        age_days = max(0, (measured.date() - dt_posted.date()).days)
        platform = r["platform"]

        post_measurements[r["id"]].append((r["measured_at"], r, age_days))

        for bucket_name, (low, high) in AGE_BUCKETS.items():
            if low <= age_days <= high:
                bucket_data[platform][bucket_name].append(r)
                all_bucket[bucket_name].append(r)
                break

    # 集計
    results = {"by_platform": {}, "all": {}}

    for platform in ["instagram", "threads", "x"]:
        if platform not in bucket_data:
            continue
        pd_result = {}
        for bname in AGE_BUCKETS:
            group = bucket_data[platform].get(bname, [])
            if group:
                pd_result[bname] = _aggregate_group(group)
        results["by_platform"][platform] = pd_result

    all_result = {}
    for bname in AGE_BUCKETS:
        group = all_bucket.get(bname, [])
        if group:
            all_result[bname] = _aggregate_group(group)
    results["all"] = all_result

    # 初速 vs 成熟 の比較分析
    fresh = all_bucket.get("新鮮(0-1日)", [])
    mature = [r for bname in ["成熟(4-7日)", "長期(8日+)"] for r in all_bucket.get(bname, [])]

    velocity_insight = None
    if fresh and mature:
        avg_fresh = sum(r["likes"] or 0 for r in fresh) / len(fresh)
        avg_mature = sum(r["likes"] or 0 for r in mature) / len(mature)
        if avg_mature > 0:
            ratio = round(avg_fresh / avg_mature * 100, 1)
            velocity_insight = {
                "fresh_avg_likes": round(avg_fresh, 1),
                "mature_avg_likes": round(avg_mature, 1),
                "fresh_to_mature_ratio": ratio,
                "interpretation": (
                    f"投稿初日のいいね平均は{round(avg_fresh,1)}、"
                    f"成熟期は{round(avg_mature,1)}。"
                    f"初速は最終値の約{ratio}%水準。"
                ),
            }
    results["velocity_insight"] = velocity_insight

    # 同一投稿に複数計測がある場合の時系列分析
    multi_measured = {pid: ms for pid, ms in post_measurements.items() if len(ms) >= 2}
    velocity_pairs = []
    for pid, measurements in multi_measured.items():
        sorted_m = sorted(measurements, key=lambda x: x[0])
        first = sorted_m[0][1]
        last = sorted_m[-1][1]
        first_likes = first["likes"] or 0
        last_likes = last["likes"] or 0
        if last_likes > 0:
            velocity_pairs.append({
                "post_id": pid,
                "platform": first["platform"],
                "first_likes": first_likes,
                "last_likes": last_likes,
                "ratio": round(first_likes / last_likes * 100, 1),
                "days_span": sorted_m[-1][2] - sorted_m[0][2],
            })
    results["velocity_pairs"] = velocity_pairs
    if velocity_pairs:
        avg_ratio = round(sum(v["ratio"] for v in velocity_pairs) / len(velocity_pairs), 1)
        results["avg_velocity_ratio"] = avg_ratio
        results["velocity_pairs_summary"] = (
            f"{len(velocity_pairs)}件の投稿で時系列計測あり。"
            f"初回計測は最終計測の平均{avg_ratio}%。"
        )

    return results


def run_pattern_analysis() -> dict:
    """全パターン分析を実行"""
    print("🔍 勝ちパターン分析エンジン実行中...")

    results = {
        "content_length": analyze_content_length_vs_engagement(),
        "cta_impact": analyze_cta_impact(),
        "hashtag_impact": analyze_hashtag_impact(),
        "hook_patterns": analyze_hook_patterns(),
        "theme_performance": analyze_theme_performance(),
        "emoji_impact": analyze_emoji_impact(),
        "posting_time": analyze_posting_time_engagement(),
        "engagement_velocity": analyze_engagement_velocity(),
    }

    results["winning_patterns"] = generate_winning_patterns(results)

    print(f"✅ {len(results['winning_patterns'])}件の勝ちパターンを特定")
    return results


if __name__ == "__main__":
    import json

    results = run_pattern_analysis()

    print("\n" + "=" * 60)
    print("🏆 勝ちパターン分析結果")
    print("=" * 60)

    print("\n【文字数 × エンゲージメント】")
    for platform, buckets in results["content_length"].items():
        print(f"  {platform.upper()}:")
        for bucket, data in buckets.items():
            print(f"    {bucket:10s} | {data['count']:2d}件 | "
                  f"avg_likes={data['avg_likes']} | avg_eng={data['avg_engagement']}%")

    print("\n【CTA効果】")
    for platform, data in results["cta_impact"].items():
        wc = data["with_cta"]
        woc = data["without_cta"]
        print(f"  {platform.upper()}: CTA有={wc['count']}件(avg {wc['avg_likes']}like) "
              f"vs 無={woc['count']}件(avg {woc['avg_likes']}like) → 差{data['cta_lift']:+.1f}%")

    print("\n【テーマ別】")
    for theme, data in results["theme_performance"].items():
        print(f"  {theme:12s} | {data['count']}件 | "
              f"avg_likes={data['avg_likes']} | platforms={data['platforms']}")

    print("\n【フック表現パターン】")
    for platform, hooks in results["hook_patterns"].items():
        print(f"  {platform.upper()}:")
        for name, data in sorted(hooks.items(), key=lambda x: x[1].get("avg_likes", 0), reverse=True):
            print(f"    {name:12s} | {data['count']}件 | avg_likes={data['avg_likes']}")

    print("\n" + "=" * 60)
    print("🎯 抽出された勝ちパターン")
    print("=" * 60)
    for i, p in enumerate(results["winning_patterns"], 1):
        print(f"\n  {i}. [{p['platform'].upper()}] {p['category']}: {p['pattern']}")
        print(f"     → {p['recommendation']}")
        print(f"     (avg_likes={p['avg_likes']}, サンプル数={p['sample_size']})")
