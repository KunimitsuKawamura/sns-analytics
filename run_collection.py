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
    DB_PATH, REPORT_OUTPUT_DIR,
)
from storage.database import (
    get_connection, init_db, upsert_post, upsert_metrics,
    upsert_ga4_session, upsert_account_metrics, log_collection,
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


def main(skip_x=False, ga4_data_path=None):
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
    parser.add_argument("--ga4-data", type=str, help="GA4データJSONファイルパス")
    args = parser.parse_args()
    main(skip_x=args.skip_x, ga4_data_path=args.ga4_data)
