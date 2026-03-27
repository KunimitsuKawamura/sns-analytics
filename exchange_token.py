"""
Instagram Graph API: 短期トークン → 長期トークン変換スクリプト
実行: python exchange_token.py
"""
import os
import sys
import requests
from pathlib import Path

# .env ファイルから環境変数を読み込む
def load_env():
    env_path = Path(__file__).parent / ".env"
    env_vars = {}
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                env_vars[key.strip()] = value.strip()
    return env_vars

def exchange_ig_token(env_vars):
    """Instagram短期トークンを長期トークンに変換"""
    short_token = env_vars.get("IG_SHORT_LIVED_TOKEN", "")
    app_id = env_vars.get("IG_APP_ID", "")
    app_secret = env_vars.get("IG_APP_SECRET", "")

    if not short_token or short_token == "PASTE_YOUR_SHORT_LIVED_TOKEN_HERE":
        print("❌ .env の IG_SHORT_LIVED_TOKEN にトークンを貼り付けてください")
        return None

    print("🔄 Instagram 短期トークン → 長期トークン変換中...")
    url = "https://graph.facebook.com/v25.0/oauth/access_token"
    params = {
        "grant_type": "fb_exchange_token",
        "client_id": app_id,
        "client_secret": app_secret,
        "fb_exchange_token": short_token,
    }
    
    response = requests.get(url, params=params)
    data = response.json()

    if "access_token" in data:
        long_token = data["access_token"]
        expires_in = data.get("expires_in", "不明")
        print(f"✅ Instagram 長期トークン取得成功!")
        print(f"   有効期限: {expires_in}秒 (約{int(expires_in)//86400}日)")
        return long_token
    else:
        print(f"❌ エラー: {data}")
        return None

def update_env_file(key, value):
    """".env ファイルの指定キーを更新"""
    env_path = Path(__file__).parent / ".env"
    lines = env_path.read_text().splitlines()
    updated = False
    for i, line in enumerate(lines):
        if line.startswith(f"{key}="):
            lines[i] = f"{key}={value}"
            updated = True
            break
    if updated:
        env_path.write_text("\n".join(lines) + "\n")
        print(f"📝 .env の {key} を更新しました")

def test_ig_token(token, ig_account_id):
    """Instagramアカウント情報を取得してトークンをテスト"""
    print("\n🔍 Instagram API接続テスト...")
    url = f"https://graph.facebook.com/v25.0/{ig_account_id}"
    params = {
        "fields": "id,username,followers_count,media_count",
        "access_token": token,
    }
    response = requests.get(url, params=params)
    data = response.json()
    
    if "username" in data:
        print(f"✅ 接続成功!")
        print(f"   ユーザー名: {data['username']}")
        print(f"   フォロワー数: {data.get('followers_count', 'N/A')}")
        print(f"   投稿数: {data.get('media_count', 'N/A')}")
        return True
    else:
        print(f"❌ エラー: {data}")
        return False

def test_threads_token(env_vars):
    """Threadsアクセストークンをテスト"""
    token = env_vars.get("THREADS_ACCESS_TOKEN", "")
    if not token or token == "PASTE_YOUR_THREADS_TOKEN_HERE":
        print("\n⏭ Threads トークンが未設定のためスキップ")
        return False

    print("\n🔍 Threads API接続テスト...")
    url = "https://graph.threads.net/v1.0/me"
    params = {
        "fields": "id,username,threads_profile_picture_url",
        "access_token": token,
    }
    response = requests.get(url, params=params)
    data = response.json()

    if "username" in data:
        print(f"✅ Threads接続成功!")
        print(f"   ユーザー名: {data['username']}")
        print(f"   Threads User ID: {data['id']}")
        return True
    else:
        print(f"❌ エラー: {data}")
        return False

if __name__ == "__main__":
    env_vars = load_env()

    # 1. Instagram トークン変換
    long_token = exchange_ig_token(env_vars)
    if long_token:
        update_env_file("IG_ACCESS_TOKEN", long_token)
        
        # 2. Instagram 接続テスト
        ig_account_id = env_vars.get("IG_BUSINESS_ACCOUNT_ID", "")
        test_ig_token(long_token, ig_account_id)

    # 3. Threads 接続テスト
    test_threads_token(env_vars)

    print("\n" + "="*50)
    print("完了! 次のステップ:")
    print("1. .env の IG_ACCESS_TOKEN に長期トークンが保存されました")
    print("2. パイプラインの実装に進みます")
    print("="*50)
