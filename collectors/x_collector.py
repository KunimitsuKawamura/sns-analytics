"""
SocialData API Collector (X/Twitter)
@info_meetcareer の投稿データを取得
"""
import requests
from datetime import datetime, date
from config import SOCIALDATA_API_KEY, SOCIALDATA_API_BASE, ACCOUNTS


class XCollector:
    def __init__(self):
        self.api_key = SOCIALDATA_API_KEY
        self.base_url = SOCIALDATA_API_BASE
        self.screen_name = ACCOUNTS["x"]["screen_name"]

    def _request(self, endpoint: str, params: dict = None) -> dict:
        """SocialData APIリクエスト"""
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
        }
        url = f"{self.base_url}{endpoint}"
        response = requests.get(url, headers=headers, params=params or {})
        response.raise_for_status()
        return response.json()

    def get_user_tweets(self, cursor: str = None) -> dict:
        """ユーザーのツイート一覧を取得（検索API経由）"""
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
        }
        params = {
            "query": f"from:{self.screen_name}",
            "type": "Latest",
        }
        if cursor:
            params["cursor"] = cursor
        url = f"{self.base_url}/twitter/search"
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()

    def get_tweet_detail(self, tweet_id: str) -> dict:
        """個別ツイートの詳細を取得"""
        return self._request(f"/twitter/statuses/{tweet_id}")

    def collect_all(self, max_pages: int = 3) -> list[dict]:
        """全投稿を収集してDB格納用に変換"""
        results = []
        cursor = None

        for _ in range(max_pages):
            data = self.get_user_tweets(cursor=cursor)
            tweets = data.get("tweets", [])

            if not tweets:
                break

            for tweet in tweets:
                # 投稿タイプ判定
                is_reply = bool(tweet.get("in_reply_to_status_id_str"))
                is_retweet = bool(tweet.get("retweeted_status"))
                is_quote = bool(tweet.get("is_quote_status"))

                if is_retweet:
                    continue  # RTはスキップ

                post_type = "reply" if is_reply else "quote" if is_quote else "tweet"

                # テキストからURLとUTM抽出
                text = tweet.get("full_text", tweet.get("text", ""))
                link_url = self._extract_url(tweet)
                utm_campaign, utm_content = self._extract_utm(link_url)

                post_data = {
                    "id": f"x_{tweet['id_str']}",
                    "platform": "x",
                    "post_type": post_type,
                    "content": text[:500],
                    "permalink": f"https://x.com/{self.screen_name}/status/{tweet['id_str']}",
                    "link_url": link_url,
                    "utm_campaign": utm_campaign,
                    "utm_content": utm_content,
                    "posted_at": self._parse_created_at(
                        tweet.get("tweet_created_at") or tweet.get("created_at", "")
                    ),
                }

                # メトリクス
                views = tweet.get("views_count", tweet.get("ext_views", {}).get("count", 0))
                likes = tweet.get("favorite_count", 0)
                replies = tweet.get("reply_count", 0)
                retweets = tweet.get("retweet_count", 0)
                bookmarks = tweet.get("bookmark_count", 0)

                total_eng = likes + replies + retweets + bookmarks
                eng_rate = round(total_eng / max(views, 1) * 100, 2) if views else 0

                metrics_data = {
                    "post_id": f"x_{tweet['id_str']}",
                    "measured_at": date.today().isoformat(),
                    "views": views or 0,
                    "likes": likes,
                    "replies": replies,
                    "reposts": retweets,
                    "saves": bookmarks,
                    "shares": 0,
                    "link_clicks": 0,
                    "profile_visits": 0,
                    "engagement_rate": eng_rate,
                }

                results.append({"post": post_data, "metrics": metrics_data})

            cursor = data.get("next_cursor")
            if not cursor:
                break

        return results

    @staticmethod
    def _extract_url(tweet: dict) -> str:
        """ツイートからURLを抽出（t.coを展開）"""
        urls = tweet.get("entities", {}).get("urls", [])
        for url_entity in urls:
            expanded = url_entity.get("expanded_url", "")
            if expanded and "twitter.com" not in expanded and "x.com" not in expanded:
                return expanded
        return ""

    @staticmethod
    def _extract_utm(url: str) -> tuple[str, str]:
        from urllib.parse import urlparse, parse_qs
        if not url:
            return "", ""
        try:
            parsed = urlparse(url)
            params = parse_qs(parsed.query)
            return (
                params.get("utm_campaign", [""])[0],
                params.get("utm_content", [""])[0],
            )
        except Exception:
            return "", ""

    @staticmethod
    def _parse_created_at(created_at: str) -> str:
        """Twitter日時形式をISO8601に変換"""
        if not created_at:
            return ""
        # SocialData API: ISO8601形式 (2026-03-27T04:27:05.000000Z)
        if "T" in created_at and (created_at.endswith("Z") or "+" in created_at):
            try:
                dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                return dt.isoformat()
            except ValueError:
                pass
        # Twitter v1形式: "Wed Oct 10 20:19:24 +0000 2018"
        try:
            dt = datetime.strptime(created_at, "%a %b %d %H:%M:%S %z %Y")
            return dt.isoformat()
        except ValueError:
            return created_at


def test_connection():
    """接続テスト"""
    collector = XCollector()
    if not collector.api_key:
        print("⚠️ SOCIALDATA_API_KEY が未設定です")
        return False
    try:
        data = collector.get_user_tweets()
        tweets = data.get("tweets", [])
        print(f"✅ X(SocialData)接続OK: @{collector.screen_name}")
        print(f"   取得ツイート数: {len(tweets)}")
        return True
    except Exception as e:
        print(f"❌ X接続エラー: {e}")
        return False


if __name__ == "__main__":
    test_connection()
