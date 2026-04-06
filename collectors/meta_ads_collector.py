"""
Meta Marketing API Collector
Instagram/Facebook広告キャンペーンのパフォーマンスデータを取得

取得データ:
  - キャンペーンレベル: 費用, リーチ, インプレッション, クリック, CTR, CPC, CPM
  - 広告セットレベル: ターゲティング別のパフォーマンス
  - 広告(クリエイティブ)レベル: クリエイティブ別のパフォーマンス, 画像URL, テキスト
  - 日別ブレークダウン: 日次トレンド分析用

必要な権限: ads_read (Standard access)
"""
import requests
from datetime import datetime, date, timedelta
from config import IG_ACCESS_TOKEN, GRAPH_API_BASE

# Keychain / 環境変数から取得
from config import _get_secret

META_AD_ACCOUNT_ID = _get_secret("META_AD_ACCOUNT_ID")


class MetaAdsCollector:
    """Meta Marketing API を使って広告パフォーマンスデータを収集"""

    def __init__(self):
        self.token = IG_ACCESS_TOKEN
        self.ad_account_id = META_AD_ACCOUNT_ID
        self.base_url = GRAPH_API_BASE
        if not self.ad_account_id:
            raise ValueError("META_AD_ACCOUNT_ID が未設定です")

    def _request(self, endpoint: str, params: dict = None) -> dict:
        """Graph APIリクエスト"""
        params = params or {}
        params["access_token"] = self.token
        url = f"{self.base_url}/{endpoint}"
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    # ─── キャンペーン一覧 ─────────────────────────

    def get_campaigns(self, status_filter: str = None) -> list[dict]:
        """広告キャンペーン一覧を取得

        Args:
            status_filter: "ACTIVE", "PAUSED", "ARCHIVED" 等。Noneで全件

        Returns:
            キャンペーン情報のリスト
        """
        params = {
            "fields": "id,name,status,objective,daily_budget,lifetime_budget,"
                      "start_time,stop_time,updated_time",
            "limit": 100,
        }
        if status_filter:
            params["filtering"] = f'[{{"field":"effective_status","operator":"IN","value":["{status_filter}"]}}]'

        data = self._request(f"{self.ad_account_id}/campaigns", params)
        return data.get("data", [])

    # ─── 広告セット一覧 ─────────────────────────

    def get_adsets(self, campaign_id: str = None) -> list[dict]:
        """広告セット一覧を取得"""
        endpoint = f"{campaign_id}/adsets" if campaign_id else f"{self.ad_account_id}/adsets"
        params = {
            "fields": "id,name,status,targeting,optimization_goal,"
                      "daily_budget,lifetime_budget,start_time,end_time",
            "limit": 100,
        }
        data = self._request(endpoint, params)
        return data.get("data", [])

    # ─── 広告（クリエイティブ）一覧 ─────────────

    def get_ads(self, adset_id: str = None) -> list[dict]:
        """広告（クリエイティブ）一覧を取得"""
        endpoint = f"{adset_id}/ads" if adset_id else f"{self.ad_account_id}/ads"
        params = {
            "fields": "id,name,status,creative{id,name,title,body,"
                      "image_url,thumbnail_url,object_story_spec},"
                      "adset_id,campaign_id",
            "limit": 100,
        }
        data = self._request(endpoint, params)
        return data.get("data", [])

    # ─── インサイト (パフォーマンスデータ) ─────────

    def get_campaign_insights(
        self,
        time_range_since: str = None,
        time_range_until: str = None,
        breakdowns: list[str] = None,
    ) -> list[dict]:
        """キャンペーンレベルのインサイトを取得

        Args:
            time_range_since: 開始日 (YYYY-MM-DD)
            time_range_until: 終了日 (YYYY-MM-DD)
            breakdowns: ブレークダウン ("age", "gender", "publisher_platform" 等)

        Returns:
            キャンペーンごとのインサイトデータ
        """
        if not time_range_since:
            time_range_since = (date.today() - timedelta(days=30)).isoformat()
        if not time_range_until:
            time_range_until = date.today().isoformat()

        params = {
            "fields": "campaign_id,campaign_name,impressions,reach,clicks,"
                      "cpc,cpm,ctr,spend,actions,cost_per_action_type,"
                      "frequency,unique_clicks,unique_ctr",
            "time_range": f'{{"since":"{time_range_since}","until":"{time_range_until}"}}',
            "level": "campaign",
            "limit": 100,
        }
        if breakdowns:
            params["breakdowns"] = ",".join(breakdowns)

        data = self._request(f"{self.ad_account_id}/insights", params)
        return data.get("data", [])

    def get_daily_insights(
        self,
        time_range_since: str = None,
        time_range_until: str = None,
        level: str = "campaign",
    ) -> list[dict]:
        """日別インサイトを取得（トレンド分析用）

        Args:
            time_range_since: 開始日
            time_range_until: 終了日
            level: "campaign", "adset", "ad"

        Returns:
            日別のインサイトデータ
        """
        if not time_range_since:
            time_range_since = (date.today() - timedelta(days=14)).isoformat()
        if not time_range_until:
            time_range_until = date.today().isoformat()

        params = {
            "fields": "campaign_id,campaign_name,impressions,reach,clicks,"
                      "cpc,cpm,ctr,spend,actions,frequency",
            "time_range": f'{{"since":"{time_range_since}","until":"{time_range_until}"}}',
            "time_increment": 1,  # 日別
            "level": level,
            "limit": 500,
        }

        data = self._request(f"{self.ad_account_id}/insights", params)
        return data.get("data", [])

    def get_ad_creative_insights(
        self,
        time_range_since: str = None,
        time_range_until: str = None,
    ) -> list[dict]:
        """広告（クリエイティブ）レベルの日別インサイトを取得

        IG paid のクリエイティブ比較に最適
        """
        if not time_range_since:
            time_range_since = (date.today() - timedelta(days=14)).isoformat()
        if not time_range_until:
            time_range_until = date.today().isoformat()

        params = {
            "fields": "ad_id,ad_name,campaign_id,campaign_name,"
                      "adset_id,adset_name,"
                      "impressions,reach,clicks,cpc,cpm,ctr,spend,"
                      "actions,cost_per_action_type,frequency",
            "time_range": f'{{"since":"{time_range_since}","until":"{time_range_until}"}}',
            "time_increment": 1,
            "level": "ad",
            "limit": 500,
        }

        data = self._request(f"{self.ad_account_id}/insights", params)
        return data.get("data", [])

    def get_platform_breakdown(
        self,
        time_range_since: str = None,
        time_range_until: str = None,
    ) -> list[dict]:
        """配信面別（Feed/Stories/Reels）のブレークダウン"""
        if not time_range_since:
            time_range_since = (date.today() - timedelta(days=14)).isoformat()
        if not time_range_until:
            time_range_until = date.today().isoformat()

        params = {
            "fields": "campaign_id,campaign_name,impressions,reach,clicks,"
                      "cpc,cpm,ctr,spend,actions",
            "time_range": f'{{"since":"{time_range_since}","until":"{time_range_until}"}}',
            "breakdowns": "publisher_platform,platform_position",
            "level": "campaign",
            "limit": 100,
        }

        data = self._request(f"{self.ad_account_id}/insights", params)
        return data.get("data", [])

    def get_demographic_breakdown(
        self,
        time_range_since: str = None,
        time_range_until: str = None,
    ) -> list[dict]:
        """年齢・性別ブレークダウン"""
        if not time_range_since:
            time_range_since = (date.today() - timedelta(days=14)).isoformat()
        if not time_range_until:
            time_range_until = date.today().isoformat()

        params = {
            "fields": "campaign_id,campaign_name,impressions,reach,clicks,"
                      "cpc,cpm,ctr,spend,actions",
            "time_range": f'{{"since":"{time_range_since}","until":"{time_range_until}"}}',
            "breakdowns": "age,gender",
            "level": "campaign",
            "limit": 100,
        }

        data = self._request(f"{self.ad_account_id}/insights", params)
        return data.get("data", [])

    # ─── 統合収集 ─────────────────────────────

    def collect_all(
        self,
        since: str = None,
        until: str = None,
    ) -> dict:
        """全データを統合収集してDB格納用に構造化

        Returns:
            {
                "campaigns": [...],
                "daily_insights": [...],
                "creative_insights": [...],
                "platform_breakdown": [...],
                "demographic_breakdown": [...],
                "summary": {...}
            }
        """
        if not since:
            since = (date.today() - timedelta(days=14)).isoformat()
        if not until:
            until = date.today().isoformat()

        print(f"   📅 期間: {since} ～ {until}")

        # キャンペーン一覧
        campaigns = self.get_campaigns()
        print(f"   📋 キャンペーン: {len(campaigns)}件")

        # キャンペーンインサイト
        campaign_insights = self.get_campaign_insights(since, until)
        print(f"   📊 キャンペーンインサイト: {len(campaign_insights)}件")

        # 日別インサイト
        daily_insights = self.get_daily_insights(since, until)
        print(f"   📈 日別インサイト: {len(daily_insights)}件")

        # クリエイティブ別インサイト
        creative_insights = self.get_ad_creative_insights(since, until)
        print(f"   🎨 クリエイティブインサイト: {len(creative_insights)}件")

        # 配信面別
        platform_breakdown = self.get_platform_breakdown(since, until)
        print(f"   📱 配信面別: {len(platform_breakdown)}件")

        # 年齢・性別
        demographic_breakdown = self.get_demographic_breakdown(since, until)
        print(f"   👥 デモグラフィック: {len(demographic_breakdown)}件")

        # サマリー計算
        total_spend = sum(float(i.get("spend", 0)) for i in campaign_insights)
        total_impressions = sum(int(i.get("impressions", 0)) for i in campaign_insights)
        total_reach = sum(int(i.get("reach", 0)) for i in campaign_insights)
        total_clicks = sum(int(i.get("clicks", 0)) for i in campaign_insights)

        summary = {
            "period": f"{since} ~ {until}",
            "total_campaigns": len(campaigns),
            "active_campaigns": len([c for c in campaigns if c.get("status") == "ACTIVE"]),
            "total_spend": round(total_spend, 2),
            "total_impressions": total_impressions,
            "total_reach": total_reach,
            "total_clicks": total_clicks,
            "avg_ctr": round(total_clicks / total_impressions * 100, 2) if total_impressions else 0,
            "avg_cpc": round(total_spend / total_clicks, 2) if total_clicks else 0,
            "avg_cpm": round(total_spend / total_impressions * 1000, 2) if total_impressions else 0,
        }

        return {
            "campaigns": campaigns,
            "campaign_insights": campaign_insights,
            "daily_insights": daily_insights,
            "creative_insights": creative_insights,
            "platform_breakdown": platform_breakdown,
            "demographic_breakdown": demographic_breakdown,
            "summary": summary,
        }

    # ─── DB格納用レコード変換 ─────────────────

    def to_db_records(self, daily_insights: list[dict]) -> list[dict]:
        """日別インサイトをDB格納用レコードに変換

        meta_ads_daily テーブルに保存するためのレコード形式
        """
        records = []
        for insight in daily_insights:
            # actions からリンククリック数を抽出
            link_clicks = 0
            landing_page_views = 0
            for action in insight.get("actions", []):
                if action["action_type"] == "link_click":
                    link_clicks = int(action["value"])
                elif action["action_type"] == "landing_page_view":
                    landing_page_views = int(action["value"])

            records.append({
                "date": insight.get("date_start", ""),
                "campaign_id": insight.get("campaign_id", ""),
                "campaign_name": insight.get("campaign_name", ""),
                "impressions": int(insight.get("impressions", 0)),
                "reach": int(insight.get("reach", 0)),
                "clicks": int(insight.get("clicks", 0)),
                "link_clicks": link_clicks,
                "landing_page_views": landing_page_views,
                "spend": float(insight.get("spend", 0)),
                "cpc": float(insight.get("cpc", 0)) if insight.get("cpc") else 0,
                "cpm": float(insight.get("cpm", 0)) if insight.get("cpm") else 0,
                "ctr": float(insight.get("ctr", 0)) if insight.get("ctr") else 0,
                "frequency": float(insight.get("frequency", 0)) if insight.get("frequency") else 0,
            })
        return records


# ─── テスト / CLI ────────────────────────────

def test_connection():
    """接続テスト"""
    collector = MetaAdsCollector()
    campaigns = collector.get_campaigns()
    active = [c for c in campaigns if c.get("status") == "ACTIVE"]
    paused = [c for c in campaigns if c.get("status") == "PAUSED"]
    print(f"✅ Meta Ads API接続OK")
    print(f"   広告アカウント: {collector.ad_account_id}")
    print(f"   キャンペーン数: {len(campaigns)} (Active: {len(active)}, Paused: {len(paused)})")
    for c in campaigns[:5]:
        status_emoji = "🟢" if c.get("status") == "ACTIVE" else "⏸️"
        print(f"   {status_emoji} {c['name']} ({c['status']})")
    return True


def print_report(since: str = None, until: str = None):
    """簡易レポートを出力"""
    collector = MetaAdsCollector()
    result = collector.collect_all(since, until)

    s = result["summary"]
    print(f"\n{'='*60}")
    print(f"📊 Meta Ads レポート ({s['period']})")
    print(f"{'='*60}")
    print(f"  キャンペーン数: {s['total_campaigns']} (稼働中: {s['active_campaigns']})")
    print(f"  費用合計: ${s['total_spend']:,.2f}")
    print(f"  インプレッション: {s['total_impressions']:,}")
    print(f"  リーチ: {s['total_reach']:,}")
    print(f"  クリック: {s['total_clicks']:,}")
    print(f"  平均CTR: {s['avg_ctr']:.2f}%")
    print(f"  平均CPC: ${s['avg_cpc']:.2f}")
    print(f"  平均CPM: ${s['avg_cpm']:.2f}")

    if result["campaign_insights"]:
        print(f"\n📋 キャンペーン別:")
        for ci in result["campaign_insights"]:
            print(f"  ・{ci.get('campaign_name', 'N/A')}")
            print(f"    費用: ${float(ci.get('spend', 0)):,.2f} | "
                  f"Imp: {int(ci.get('impressions', 0)):,} | "
                  f"Click: {int(ci.get('clicks', 0)):,} | "
                  f"CTR: {ci.get('ctr', 'N/A')}%")

    if result["creative_insights"]:
        print(f"\n🎨 クリエイティブ別日次:")
        for ci in result["creative_insights"][:10]:
            print(f"  ・{ci.get('date_start', '?')} | {ci.get('ad_name', 'N/A')}")
            print(f"    Imp: {int(ci.get('impressions', 0)):,} | "
                  f"Click: {int(ci.get('clicks', 0)):,} | "
                  f"CTR: {ci.get('ctr', 'N/A')}%")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Meta Ads Data Collector")
    parser.add_argument("--test", action="store_true", help="接続テスト")
    parser.add_argument("--report", action="store_true", help="レポート出力")
    parser.add_argument("--since", type=str, help="開始日 (YYYY-MM-DD)")
    parser.add_argument("--until", type=str, help="終了日 (YYYY-MM-DD)")
    args = parser.parse_args()

    if args.test:
        test_connection()
    elif args.report:
        print_report(args.since, args.until)
    else:
        test_connection()
