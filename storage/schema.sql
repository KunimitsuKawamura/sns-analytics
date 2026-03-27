-- SNS Performance Pipeline - Database Schema

-- 投稿データ（全プラットフォーム共通）
CREATE TABLE IF NOT EXISTS posts (
    id TEXT PRIMARY KEY,              -- {platform}_{native_id}
    platform TEXT NOT NULL,           -- x, instagram, threads
    post_type TEXT,                   -- tweet, reply, carousel, reel, story, thread
    content TEXT,
    permalink TEXT,
    link_url TEXT,                    -- 投稿内のリンク先URL
    utm_campaign TEXT,
    utm_content TEXT,
    posted_at DATETIME,
    collected_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 投稿パフォーマンス指標（時系列）
CREATE TABLE IF NOT EXISTS post_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    post_id TEXT REFERENCES posts(id),
    measured_at DATE,
    views INTEGER DEFAULT 0,
    likes INTEGER DEFAULT 0,
    replies INTEGER DEFAULT 0,
    reposts INTEGER DEFAULT 0,        -- RT / リポスト
    saves INTEGER DEFAULT 0,          -- IG保存 / Xブックマーク
    shares INTEGER DEFAULT 0,
    link_clicks INTEGER DEFAULT 0,
    profile_visits INTEGER DEFAULT 0,
    engagement_rate REAL DEFAULT 0,
    UNIQUE(post_id, measured_at)
);

-- GA4 流入データ（日次）
CREATE TABLE IF NOT EXISTS ga4_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date DATE,
    source TEXT,
    medium TEXT,
    campaign TEXT,
    content TEXT,                      -- utm_content → 投稿特定用
    landing_page TEXT,
    sessions INTEGER DEFAULT 0,
    engaged_sessions INTEGER DEFAULT 0,
    conversions INTEGER DEFAULT 0,
    avg_session_duration REAL DEFAULT 0,
    UNIQUE(date, source, medium, campaign, content, landing_page)
);

-- アカウント指標（日次）
CREATE TABLE IF NOT EXISTS account_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    platform TEXT,
    date DATE,
    followers INTEGER DEFAULT 0,
    profile_views INTEGER DEFAULT 0,
    website_clicks INTEGER DEFAULT 0,
    UNIQUE(platform, date)
);

-- データ収集ログ
CREATE TABLE IF NOT EXISTS collection_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    collector TEXT,                    -- x, instagram, threads, ga4
    started_at DATETIME,
    completed_at DATETIME,
    status TEXT,                       -- success, error
    records_collected INTEGER DEFAULT 0,
    error_message TEXT
);
