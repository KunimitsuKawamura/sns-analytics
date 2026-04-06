"""
SQLite Database Manager
"""
import sqlite3
from pathlib import Path
from datetime import datetime
from config import DB_PATH


def get_connection():
    """DB接続を取得"""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    """DBを初期化（テーブル作成）"""
    schema_path = Path(__file__).parent / "schema.sql"
    conn = get_connection()
    with open(schema_path) as f:
        conn.executescript(f.read())
    conn.close()
    print(f"✅ DB初期化完了: {DB_PATH}")


def upsert_post(conn, post_data: dict):
    """投稿をUPSERT"""
    conn.execute("""
        INSERT INTO posts (id, platform, post_type, content, permalink, link_url,
                          utm_campaign, utm_content, posted_at)
        VALUES (:id, :platform, :post_type, :content, :permalink, :link_url,
                :utm_campaign, :utm_content, :posted_at)
        ON CONFLICT(id) DO UPDATE SET
            content = excluded.content,
            link_url = excluded.link_url,
            utm_campaign = excluded.utm_campaign,
            utm_content = excluded.utm_content,
            collected_at = CURRENT_TIMESTAMP
    """, post_data)


def upsert_metrics(conn, metrics_data: dict):
    """投稿メトリクスをUPSERT"""
    conn.execute("""
        INSERT INTO post_metrics (post_id, measured_at, views, likes, replies,
                                  reposts, saves, shares, link_clicks, profile_visits, engagement_rate)
        VALUES (:post_id, :measured_at, :views, :likes, :replies,
                :reposts, :saves, :shares, :link_clicks, :profile_visits, :engagement_rate)
        ON CONFLICT(post_id, measured_at) DO UPDATE SET
            views = excluded.views,
            likes = excluded.likes,
            replies = excluded.replies,
            reposts = excluded.reposts,
            saves = excluded.saves,
            shares = excluded.shares,
            link_clicks = excluded.link_clicks,
            profile_visits = excluded.profile_visits,
            engagement_rate = excluded.engagement_rate
    """, metrics_data)


def upsert_ga4_session(conn, session_data: dict):
    """GA4セッションデータをUPSERT"""
    conn.execute("""
        INSERT INTO ga4_sessions (date, source, medium, campaign, content,
                                  landing_page, sessions, engaged_sessions,
                                  conversions, avg_session_duration)
        VALUES (:date, :source, :medium, :campaign, :content,
                :landing_page, :sessions, :engaged_sessions,
                :conversions, :avg_session_duration)
        ON CONFLICT(date, source, medium, campaign, content, landing_page) DO UPDATE SET
            sessions = excluded.sessions,
            engaged_sessions = excluded.engaged_sessions,
            conversions = excluded.conversions,
            avg_session_duration = excluded.avg_session_duration
    """, session_data)


def upsert_account_metrics(conn, data: dict):
    """アカウント指標をUPSERT"""
    conn.execute("""
        INSERT INTO account_metrics (platform, date, followers, profile_views, website_clicks)
        VALUES (:platform, :date, :followers, :profile_views, :website_clicks)
        ON CONFLICT(platform, date) DO UPDATE SET
            followers = excluded.followers,
            profile_views = excluded.profile_views,
            website_clicks = excluded.website_clicks
    """, data)


def upsert_meta_ads_daily(conn, data: dict):
    """Meta広告日別データをUPSERT"""
    conn.execute("""
        INSERT INTO meta_ads_daily (date, campaign_id, campaign_name,
                                     impressions, reach, clicks, link_clicks,
                                     landing_page_views, spend, cpc, cpm, ctr, frequency)
        VALUES (:date, :campaign_id, :campaign_name,
                :impressions, :reach, :clicks, :link_clicks,
                :landing_page_views, :spend, :cpc, :cpm, :ctr, :frequency)
        ON CONFLICT(date, campaign_id) DO UPDATE SET
            campaign_name = excluded.campaign_name,
            impressions = excluded.impressions,
            reach = excluded.reach,
            clicks = excluded.clicks,
            link_clicks = excluded.link_clicks,
            landing_page_views = excluded.landing_page_views,
            spend = excluded.spend,
            cpc = excluded.cpc,
            cpm = excluded.cpm,
            ctr = excluded.ctr,
            frequency = excluded.frequency
    """, data)


def upsert_meta_ads_creative(conn, data: dict):
    """Meta広告クリエイティブ日別データをUPSERT"""
    conn.execute("""
        INSERT INTO meta_ads_creatives (date, ad_id, ad_name,
                                         campaign_id, campaign_name,
                                         adset_id, adset_name,
                                         impressions, reach, clicks, link_clicks,
                                         spend, cpc, ctr, frequency)
        VALUES (:date, :ad_id, :ad_name,
                :campaign_id, :campaign_name,
                :adset_id, :adset_name,
                :impressions, :reach, :clicks, :link_clicks,
                :spend, :cpc, :ctr, :frequency)
        ON CONFLICT(date, ad_id) DO UPDATE SET
            ad_name = excluded.ad_name,
            campaign_name = excluded.campaign_name,
            adset_name = excluded.adset_name,
            impressions = excluded.impressions,
            reach = excluded.reach,
            clicks = excluded.clicks,
            link_clicks = excluded.link_clicks,
            spend = excluded.spend,
            cpc = excluded.cpc,
            ctr = excluded.ctr,
            frequency = excluded.frequency
    """, data)


def log_collection(conn, collector: str, started_at: datetime,
                   status: str, records: int, error: str = None):
    """収集ログを記録"""
    conn.execute("""
        INSERT INTO collection_log (collector, started_at, completed_at, status,
                                    records_collected, error_message)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (collector, started_at, datetime.now(), status, records, error))


if __name__ == "__main__":
    init_db()
