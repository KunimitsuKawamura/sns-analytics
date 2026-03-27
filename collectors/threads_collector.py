"""
Threads API Collector
@meetcareer_official の投稿とインサイトを取得
"""
import requests
from datetime import datetime, date
from config import THREADS_ACCESS_TOKEN, THREADS_API_BASE


class ThreadsCollector:
    def __init__(self):
        self.token = THREADS_ACCESS_TOKEN
        self.base_url = THREADS_API_BASE
        self.user_id = None

    def _request(self, endpoint: str, params: dict = None) -> dict:
        """Threads APIリクエスト"""
        params = params or {}
        params["access_token"] = self.token
        url = f"{self.base_url}/{endpoint}"
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def _get_user_id(self) -> str:
        """Threads User IDを取得（キャッシュ）"""
        if not self.user_id:
            data = self._request("me", params={"fields": "id,username"})
            self.user_id = data["id"]
        return self.user_id

    def get_recent_posts(self, limit: int = 50) -> list[dict]:
        """最近の投稿一覧を取得"""
        user_id = self._get_user_id()
        data = self._request(
            f"{user_id}/threads",
            params={
                "fields": "id,text,timestamp,permalink,media_type,"
                         "shortcode,is_quote_post",
                "limit": limit,
            },
        )
        return data.get("data", [])

    def get_post_insights(self, media_id: str) -> dict:
        """個別投稿のインサイトを取得"""
        try:
            data = self._request(
                f"{media_id}/insights",
                params={
                    "metric": "views,likes,replies,reposts,quotes,shares",
                },
            )
            metrics = {}
            for item in data.get("data", []):
                metrics[item["name"]] = item["values"][0]["value"]
            return metrics
        except requests.exceptions.HTTPError:
            return {}

    def get_account_insights(self, period: str = "day") -> dict:
        """アカウントインサイトを取得"""
        user_id = self._get_user_id()
        try:
            data = self._request(
                f"{user_id}/threads_insights",
                params={
                    "metric": "views,likes,replies,reposts,quotes,"
                             "followers_count,follower_demographics",
                    "period": period,
                },
            )
            metrics = {}
            for item in data.get("data", []):
                if item.get("total_value"):
                    metrics[item["name"]] = item["total_value"]["value"]
                elif item.get("values"):
                    metrics[item["name"]] = item["values"][-1]["value"]
            return metrics
        except requests.exceptions.HTTPError:
            return {}

    def collect_all(self) -> list[dict]:
        """全投稿 + インサイトを収集してDB格納用に変換"""
        posts = self.get_recent_posts()
        results = []

        for post in posts:
            text = post.get("text", "") or ""
            link_url = self._extract_url(text)
            utm_campaign, utm_content = self._extract_utm(link_url)

            # 投稿タイプ判定
            media_type = post.get("media_type", "").lower()
            is_quote = post.get("is_quote_post", False)
            post_type = "quote" if is_quote else {
                "text_post": "thread",
                "image": "thread_image",
                "video": "thread_video",
                "carousel_album": "thread_carousel",
            }.get(media_type, "thread")

            # インサイト取得
            insights = self.get_post_insights(post["id"])

            post_data = {
                "id": f"threads_{post['id']}",
                "platform": "threads",
                "post_type": post_type,
                "content": text[:500],
                "permalink": post.get("permalink", ""),
                "link_url": link_url,
                "utm_campaign": utm_campaign,
                "utm_content": utm_content,
                "posted_at": post.get("timestamp", ""),
            }

            total_engagement = (
                insights.get("likes", 0)
                + insights.get("replies", 0)
                + insights.get("reposts", 0)
                + insights.get("quotes", 0)
            )
            views = insights.get("views", 1)

            metrics_data = {
                "post_id": f"threads_{post['id']}",
                "measured_at": date.today().isoformat(),
                "views": views,
                "likes": insights.get("likes", 0),
                "replies": insights.get("replies", 0),
                "reposts": insights.get("reposts", 0),
                "saves": insights.get("quotes", 0),  # Threadsには保存がないのでquotesを代用
                "shares": insights.get("shares", 0),
                "link_clicks": 0,
                "profile_visits": 0,
                "engagement_rate": round(total_engagement / max(views, 1) * 100, 2),
            }

            results.append({"post": post_data, "metrics": metrics_data})

        return results

    @staticmethod
    def _extract_url(text: str) -> str:
        import re
        urls = re.findall(r'https?://[^\s\)]+', text)
        return urls[0] if urls else ""

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


def test_connection():
    """接続テスト"""
    collector = ThreadsCollector()
    user_id = collector._get_user_id()
    print(f"✅ Threads接続OK: User ID {user_id}")
    posts = collector.get_recent_posts(limit=3)
    print(f"   最新投稿数: {len(posts)}")
    return True


if __name__ == "__main__":
    test_connection()
