"""
Instagram Graph API Collector
@meetcareer_official の投稿とインサイトを取得
"""
import requests
from datetime import datetime, date
from config import IG_ACCESS_TOKEN, IG_BUSINESS_ACCOUNT_ID, GRAPH_API_BASE


class InstagramCollector:
    def __init__(self):
        self.token = IG_ACCESS_TOKEN
        self.account_id = IG_BUSINESS_ACCOUNT_ID
        self.base_url = GRAPH_API_BASE

    def _request(self, endpoint: str, params: dict = None) -> dict:
        """Graph APIリクエスト"""
        params = params or {}
        params["access_token"] = self.token
        url = f"{self.base_url}/{endpoint}"
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def get_recent_posts(self, limit: int = 50) -> list[dict]:
        """最近の投稿一覧を取得"""
        data = self._request(
            f"{self.account_id}/media",
            params={
                "fields": "id,caption,media_type,media_url,thumbnail_url,"
                         "timestamp,permalink,like_count,comments_count",
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
                    "metric": "reach,saved,shares,total_interactions,"
                             "ig_reels_avg_watch_time,ig_reels_video_view_total_time,"
                             "plays,views",
                },
            )
            metrics = {}
            for item in data.get("data", []):
                metrics[item["name"]] = item["values"][0]["value"]
            return metrics
        except requests.exceptions.HTTPError as e:
            # 一部メトリクスは投稿タイプによって未対応
            return {}

    def get_account_insights(self) -> dict:
        """アカウントインサイトを取得"""
        data = self._request(
            f"{self.account_id}",
            params={
                "fields": "id,username,followers_count,media_count",
            },
        )
        return data

    def collect_all(self) -> list[dict]:
        """全投稿 + インサイトを収集してDB格納用に変換"""
        posts = self.get_recent_posts()
        results = []

        for post in posts:
            # 投稿内リンクURL抽出
            caption = post.get("caption", "") or ""
            link_url = self._extract_url(caption)

            # UTM情報抽出
            utm_campaign, utm_content = self._extract_utm(link_url)

            # 投稿タイプ判定
            media_type = post.get("media_type", "").lower()
            post_type = {
                "image": "image",
                "video": "reel",
                "carousel_album": "carousel",
            }.get(media_type, "image")

            # インサイト取得
            insights = self.get_post_insights(post["id"])

            post_data = {
                "id": f"instagram_{post['id']}",
                "platform": "instagram",
                "post_type": post_type,
                "content": caption[:500],
                "permalink": post.get("permalink", ""),
                "link_url": link_url,
                "utm_campaign": utm_campaign,
                "utm_content": utm_content,
                "posted_at": post.get("timestamp", ""),
            }

            metrics_data = {
                "post_id": f"instagram_{post['id']}",
                "measured_at": date.today().isoformat(),
                "views": insights.get("views", insights.get("plays", 0)),
                "likes": post.get("like_count", 0),
                "replies": post.get("comments_count", 0),
                "reposts": 0,
                "saves": insights.get("saved", 0),
                "shares": insights.get("shares", 0),
                "link_clicks": 0,
                "profile_visits": 0,
                "engagement_rate": self._calc_engagement_rate(
                    post.get("like_count", 0),
                    post.get("comments_count", 0),
                    insights.get("saved", 0),
                    insights.get("shares", 0),
                    insights.get("reach", 1),
                ),
            }

            results.append({"post": post_data, "metrics": metrics_data})

        return results

    @staticmethod
    def _extract_url(text: str) -> str:
        """テキストからURLを抽出"""
        import re
        urls = re.findall(r'https?://[^\s\)]+', text)
        return urls[0] if urls else ""

    @staticmethod
    def _extract_utm(url: str) -> tuple[str, str]:
        """URLからUTMパラメータを抽出"""
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
    def _calc_engagement_rate(likes, comments, saves, shares, reach) -> float:
        """エンゲージメント率を算出"""
        if reach <= 0:
            return 0.0
        return round((likes + comments + saves + shares) / reach * 100, 2)


def test_connection():
    """接続テスト"""
    collector = InstagramCollector()
    info = collector.get_account_insights()
    print(f"✅ Instagram接続OK: @{info.get('username', 'N/A')}")
    print(f"   フォロワー: {info.get('followers_count', 0)}")
    print(f"   投稿数: {info.get('media_count', 0)}")
    return True


if __name__ == "__main__":
    test_connection()
