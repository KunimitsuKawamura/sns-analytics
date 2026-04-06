"""
SNS Performance Pipeline - データ収集実行
"""
import sys
import json
from datetime import datetime, date, timedelta

# プロジェクトルートをパスに追加
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent))

from config import (
    SOCIALDATA_API_KEY, IG_ACCESS_TOKEN, THREADS_ACCESS_TOKEN,
    META_AD_ACCOUNT_ID, DB_PATH, REPORT_OUTPUT_DIR,
)
from storage.database import (
    get_connection, init_db, upsert_post, upsert_metrics,
    upsert_ga4_session, upsert_account_metrics,
    upsert_meta_ads_daily, upsert_meta_ads_creative,
    log_collection,
)


def collect_instagram():
    """Instagram投稿データを収集"""
    from collectors.instagram_collector import InstagramCollector
    print("\n📸 Instagram データ収集開始...")
    started = datetime.now()
    conn = get_connection()

    try:
        collector = InstagramCollector()
        results = collector.collect_all()

        for item in results:
            upsert_post(conn, item["post"])
            upsert_metrics(conn, item["metrics"])

        # アカウント指標
        account_info = collector.get_account_insights()
        upsert_account_metrics(conn, {
            "platform": "instagram",
            "date": date.today().isoformat(),
            "followers": account_info.get("followers_count", 0),
            "profile_views": account_info.get("profile_views", 0),
            "website_clicks": 0,
        })

        conn.commit()
        log_collection(conn, "instagram", started, "success", len(results))
        conn.commit()
        print(f"   ✅ {len(results)}件の投稿を収集完了")
        return len(results)

    except Exception as e:
        log_collection(conn, "instagram", started, "error", 0, str(e))
        conn.commit()
        print(f"   ❌ エラー: {e}")
        return 0
    finally:
        conn.close()


def collect_threads():
    """Threads投稿データを収集"""
    from collectors.threads_collector import ThreadsCollector
    print("\n🧵 Threads データ収集開始...")
    started = datetime.now()
    conn = get_connection()

    try:
        collector = ThreadsCollector()
        results = collector.collect_all()

        for item in results:
            upsert_post(conn, item["post"])
            upsert_metrics(conn, item["metrics"])

        # アカウント指標
        account_insights = collector.get_account_insights()
        upsert_account_metrics(conn, {
            "platform": "threads",
            "date": date.today().isoformat(),
            "followers": account_insights.get("followers_count", 0),
            "profile_views": 0,
            "website_clicks": 0,
        })

        conn.commit()
        log_collection(conn, "threads", started, "success", len(results))
        conn.commit()
        print(f"   ✅ {len(results)}件の投稿を収集完了")
        return len(results)

    except Exception as e:
        log_collection(conn, "threads", started, "error", 0, str(e))
        conn.commit()
        print(f"   ❌ エラー: {e}")
        return 0
    finally:
        conn.close()


def collect_x():
    """X(Twitter)投稿データを収集"""
    from collectors.x_collector import XCollector
    print("\n🐦 X データ収集開始...")
    started = datetime.now()
    conn = get_connection()

    try:
        collector = XCollector()
        if not collector.api_key:
            print("   ⚠️ SOCIALDATA_API_KEY が未設定 - スキップ")
            log_collection(conn, "x", started, "skipped", 0, "API key not set")
            conn.commit()
            return 0

        results = collector.collect_all()

        for item in results:
            upsert_post(conn, item["post"])
            upsert_metrics(conn, item["metrics"])

        conn.commit()
        log_collection(conn, "x", started, "success", len(results))
        conn.commit()
        print(f"   ✅ {len(results)}件の投稿を収集完了")
        return len(results)

    except Exception as e:
        log_collection(conn, "x", started, "error", 0, str(e))
        conn.commit()
        print(f"   ❌ エラー: {e}")
        return 0
    finally:
        conn.close()


def collect_ga4(report_data: dict = None):
    """GA4 SNS流入データを収集（直接API or MCPレポート）"""
    from collectors.ga4_collector import (
        parse_mcp_report, filter_sns_sessions, to_db_records,
    )
    print("\n📊 GA4 データ処理開始...")
    started = datetime.now()
    conn = get_connection()

    try:
        # 1. 直接API呼び出し (サービスアカウントがあれば自動)
        try:
            from collectors.ga4_direct import is_available, fetch_sns_sessions
            if is_available():
                print("   🔑 GA4 直接API モード")
                db_records = fetch_sns_sessions(days=30)
                for record in db_records:
                    upsert_ga4_session(conn, record)
                conn.commit()
                log_collection(conn, "ga4", started, "success", len(db_records))
                conn.commit()
                print(f"   ✅ {len(db_records)}件のSNSセッションデータを直接API取得完了")
                return len(db_records)
        except ImportError:
            pass

        # 2. MCPレポートデータ (フォールバック)
        if report_data is None:
            print("   ⚠️ GA4データが渡されていません - スキップ")
            print("   💡 GA4_PROPERTY_ID + サービスアカウントキーを設定するか、")
            print("      --ga4-data でJSONファイルを指定してください")
            log_collection(conn, "ga4", started, "skipped", 0, "No data provided")
            conn.commit()
            return 0

        records = parse_mcp_report(report_data)
        sns_records = filter_sns_sessions(records)
        db_records = to_db_records(sns_records)

        for record in db_records:
            upsert_ga4_session(conn, record)

        conn.commit()
        log_collection(conn, "ga4", started, "success", len(db_records))
        conn.commit()
        print(f"   ✅ {len(db_records)}件のSNSセッションデータを処理完了")
        return len(db_records)

    except Exception as e:
        log_collection(conn, "ga4", started, "error", 0, str(e))
        conn.commit()
        print(f"   ❌ エラー: {e}")
        return 0
    finally:
        conn.close()


def collect_meta_ads(since: str = None, until: str = None):
    """Meta広告パフォーマンスデータを収集"""
    from collectors.meta_ads_collector import MetaAdsCollector
    print("\n📢 Meta広告データ収集開始...")
    started = datetime.now()
    conn = get_connection()

    try:
        collector = MetaAdsCollector()
        result = collector.collect_all(since, until)

        # 日別キャンペーンデータをDB保存
        daily_records = collector.to_db_records(result["daily_insights"])
        for record in daily_records:
            upsert_meta_ads_daily(conn, record)

        # クリエイティブ別日別データをDB保存
        creative_count = 0
        for ci in result["creative_insights"]:
            link_clicks = 0
            for action in ci.get("actions", []):
                if action["action_type"] == "link_click":
                    link_clicks = int(action["value"])

            creative_data = {
                "date": ci.get("date_start", ""),
                "ad_id": ci.get("ad_id", ""),
                "ad_name": ci.get("ad_name", ""),
                "campaign_id": ci.get("campaign_id", ""),
                "campaign_name": ci.get("campaign_name", ""),
                "adset_id": ci.get("adset_id", ""),
                "adset_name": ci.get("adset_name", ""),
                "impressions": int(ci.get("impressions", 0)),
                "reach": int(ci.get("reach", 0)),
                "clicks": int(ci.get("clicks", 0)),
                "link_clicks": link_clicks,
                "spend": float(ci.get("spend", 0)),
                "cpc": float(ci.get("cpc", 0)) if ci.get("cpc") else 0,
                "ctr": float(ci.get("ctr", 0)) if ci.get("ctr") else 0,
                "frequency": float(ci.get("frequency", 0)) if ci.get("frequency") else 0,
            }
            upsert_meta_ads_creative(conn, creative_data)
            creative_count += 1

        conn.commit()
        total_records = len(daily_records) + creative_count
        log_collection(conn, "meta_ads", started, "success", total_records)
        conn.commit()

        # サマリーを表示
        s = result["summary"]
        print(f"   ✅ {total_records}件のレコードを収集完了")
        print(f"   💰 費用合計: ¥{s['total_spend']:,.0f}")
        print(f"   👁️ リーチ: {s['total_reach']:,} | Imp: {s['total_impressions']:,}")
        print(f"   🖱️ クリック: {s['total_clicks']:,} | CTR: {s['avg_ctr']:.2f}%")
        return total_records

    except Exception as e:
        log_collection(conn, "meta_ads", started, "error", 0, str(e))
        conn.commit()
        print(f"   ❌ エラー: {e}")
        return 0
    finally:
        conn.close()


def main(skip_x=False, skip_ads=False, ga4_data_path=None):
    """メインパイプライン実行"""
    print("=" * 60)
    print("🚀 SNS Performance データ収集パイプライン")
    print(f"   実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # DB初期化
    init_db()

    total = 0

    # Instagram
    if IG_ACCESS_TOKEN:
        total += collect_instagram()
    else:
        print("\n📸 Instagram: トークン未設定 - スキップ")

    # Threads
    if THREADS_ACCESS_TOKEN:
        total += collect_threads()
    else:
        print("\n🧵 Threads: トークン未設定 - スキップ")

    # X
    if not skip_x:
        total += collect_x()
    else:
        print("\n🐦 X: スキップ")

    # Meta Ads
    if not skip_ads and META_AD_ACCOUNT_ID:
        total += collect_meta_ads()
    elif not META_AD_ACCOUNT_ID:
        print("\n📢 Meta Ads: アカウントID未設定 - スキップ")
    else:
        print("\n📢 Meta Ads: スキップ")

    # GA4
    if ga4_data_path:
        with open(ga4_data_path) as f:
            ga4_data = json.load(f)
        total += collect_ga4(ga4_data)
    else:
        # 直接API or スキップ（collect_ga4内で自動判定）
        total += collect_ga4()

    print("\n" + "=" * 60)
    print(f"✅ 収集完了! 合計 {total} 件のレコード")
    print(f"   DB: {DB_PATH}")
    print("=" * 60)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="SNS Performance Data Collection")
    parser.add_argument("--skip-x", action="store_true", help="X収集をスキップ")
    parser.add_argument("--skip-ads", action="store_true", help="Meta広告収集をスキップ")
    parser.add_argument("--ga4-data", type=str, help="GA4データJSONファイルパス")
    parser.add_argument("--ads-only", action="store_true", help="Meta広告のみ収集")
    parser.add_argument("--since", type=str, help="開始日 (YYYY-MM-DD)")
    parser.add_argument("--until", type=str, help="終了日 (YYYY-MM-DD)")
    args = parser.parse_args()

    if args.ads_only:
        # Meta広告のみ収集モード
        from storage.database import init_db as _init_db
        _init_db()
        collect_meta_ads(since=args.since, until=args.until)
    else:
        main(skip_x=args.skip_x, skip_ads=args.skip_ads, ga4_data_path=args.ga4_data)
