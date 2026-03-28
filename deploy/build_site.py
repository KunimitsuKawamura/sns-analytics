"""
GitHub Pages デプロイ用サイトビルダー

1. dashboard.html + weekly_report.html を docs/ にコピー
2. noindex メタタグ注入
3. robots.txt 生成
4. staticrypt でパスワード保護
5. index.html (ナビゲーション) 生成
"""
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).parent.parent
REPORT_DIR = PROJECT_ROOT / "output" / "reports"
DOCS_DIR = PROJECT_ROOT / "docs"

# パスワードは環境変数 → Keychain → 未設定エラー
def _get_site_password():
    """サイトパスワードを安全に取得"""
    pw = os.getenv("SITE_PASSWORD")
    if pw:
        return pw
    # macOS Keychain フォールバック
    try:
        import subprocess
        result = subprocess.run(
            ["security", "find-generic-password", "-s", "sns-analytics", "-a", "SITE_PASSWORD", "-w"],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except Exception:
        pass
    print("⚠️  SITE_PASSWORD が未設定です。環境変数またはKeychainに設定してください。")
    print("   export SITE_PASSWORD='your_password'")
    print("   security add-generic-password -s sns-analytics -a SITE_PASSWORD -w 'your_password'")
    sys.exit(1)

SITE_PASSWORD = _get_site_password()

NOINDEX_META = '<meta name="robots" content="noindex, nofollow">'
ROBOTS_TXT = """User-agent: *
Disallow: /
"""


def clean_docs():
    """docs/ ディレクトリをクリーンアップ"""
    if DOCS_DIR.exists():
        shutil.rmtree(DOCS_DIR)
    DOCS_DIR.mkdir(parents=True)


def inject_noindex(html: str) -> str:
    """HTMLにnoindexメタタグを注入"""
    if NOINDEX_META in html:
        return html
    return html.replace("<head>", f"<head>\n{NOINDEX_META}", 1)


def build_index_page() -> str:
    """ナビページ生成"""
    now = datetime.now().strftime('%Y年%m月%d日 %H:%M')
    return f'''<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
{NOINDEX_META}
<title>SNS Analytics - MeetCareer</title>
<style>
* {{ margin:0; padding:0; box-sizing:border-box; }}
:root {{ --warm:#FFE5C7; --cool:#C1E7E8; --bg:#111318; --card:#1A1E26; --text:#EDE9E3; --muted:#9A958D; --border:#2E333C; }}
body {{ font-family:'Inter','Hiragino Sans',sans-serif; background:var(--bg); color:var(--text); min-height:100vh; display:flex; align-items:center; justify-content:center; }}
.container {{ max-width:600px; width:90%; }}
.header {{ background:linear-gradient(135deg,var(--warm),var(--cool)); border-radius:16px 16px 0 0; padding:2rem; color:#2A2520; text-align:center; }}
.header h1 {{ font-size:1.5rem; }}
.header p {{ color:#5A524A; font-size:0.85rem; margin-top:0.3rem; }}
.body {{ background:var(--card); border:1px solid var(--border); border-top:none; border-radius:0 0 16px 16px; padding:1.5rem; }}
.link-card {{ display:block; background:var(--bg); border:1px solid var(--border); border-radius:10px; padding:1.2rem; margin-bottom:0.8rem; text-decoration:none; color:var(--text); transition:all 0.3s; }}
.link-card:hover {{ border-color:var(--warm); transform:translateY(-2px); box-shadow:0 4px 20px rgba(255,229,199,0.1); }}
.link-card .icon {{ font-size:1.5rem; }}
.link-card .title {{ font-weight:600; margin:0.3rem 0; }}
.link-card .desc {{ font-size:0.8rem; color:var(--muted); }}
.footer {{ text-align:center; margin-top:1rem; font-size:0.75rem; color:var(--muted); }}
</style>
</head>
<body>
<div class="container">
  <div class="header">
    <h1>📊 SNS Analytics</h1>
    <p>ミートキャリア | 最終更新: {now}</p>
  </div>
  <div class="body">
    <a href="dashboard.html" class="link-card">
      <div class="icon">📈</div>
      <div class="title">インタラクティブダッシュボード</div>
      <div class="desc">KPIオーバービュー・コンテンツ分析・流入分析・チームインサイト</div>
    </a>
    <a href="report.html" class="link-card">
      <div class="icon">📄</div>
      <div class="title">週次レポート</div>
      <div class="desc">前週比較・担当者別KPI・勝ちパターン・推奨アクション</div>
    </a>
  </div>
  <div class="footer">© MeetCareer SNS Analytics</div>
</div>
</body>
</html>'''


def build_site():
    """サイトをビルド"""
    print("=" * 60)
    print("🏗️  GitHub Pages サイトビルド")
    print("=" * 60)

    # 1. Clean
    clean_docs()
    print("  ✅ docs/ クリーンアップ完了")

    # 2. Copy dashboard
    dashboard_src = REPORT_DIR / "dashboard.html"
    if dashboard_src.exists():
        html = dashboard_src.read_text(encoding="utf-8")
        html = inject_noindex(html)
        (DOCS_DIR / "dashboard.html").write_text(html, encoding="utf-8")
        print("  ✅ dashboard.html コピー + noindex注入")

    # 3. Copy latest weekly report
    reports = sorted(REPORT_DIR.glob("weekly_report_*.html"), reverse=True)
    if reports:
        html = reports[0].read_text(encoding="utf-8")
        html = inject_noindex(html)
        (DOCS_DIR / "report.html").write_text(html, encoding="utf-8")
        print(f"  ✅ {reports[0].name} → report.html + noindex注入")

    # 4. Index page
    (DOCS_DIR / "index.html").write_text(build_index_page(), encoding="utf-8")
    print("  ✅ index.html 生成")

    # 5. robots.txt
    (DOCS_DIR / "robots.txt").write_text(ROBOTS_TXT, encoding="utf-8")
    print("  ✅ robots.txt 生成 (全Disallow)")

    # 6. .nojekyll (GitHub Pages用)
    (DOCS_DIR / ".nojekyll").touch()
    print("  ✅ .nojekyll 生成")

    # 7. staticrypt でパスワード保護
    _encrypt_files()

    print(f"\n✅ サイトビルド完了: {DOCS_DIR}")
    print(f"   HTMLファイル: {len(list(DOCS_DIR.glob('*.html')))}個")


def _encrypt_files():
    """staticrypt でHTMLファイルをパスワード保護"""
    html_files = list(DOCS_DIR.glob("*.html"))
    if not html_files:
        return

    try:
        # npx staticrypt が使えるか確認
        result = subprocess.run(
            ["npx", "-y", "staticrypt", "--version"],
            capture_output=True, text=True, timeout=60
        )
        if result.returncode != 0:
            print("  ⚠️ staticrypt 不可 - パスワード保護スキップ")
            return
    except (FileNotFoundError, subprocess.TimeoutExpired):
        print("  ⚠️ npx 不可 - パスワード保護スキップ")
        return

    # 全HTMLを一括暗号化（出力先を docs/ に指定）
    file_paths = [str(f) for f in html_files]
    try:
        cmd = [
            "npx", "-y", "staticrypt",
            *file_paths,
            "-p", SITE_PASSWORD,
            "-d", str(DOCS_DIR),
            "--short",
            "--template-title", "SNS Analytics - ログイン",
            "--template-instructions", "パスワードを入力してください",
            "--template-button", "アクセス",
            "--template-color-primary", "#D4956B",
            "--template-color-secondary", "#1A1E26",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.returncode == 0:
            for f in html_files:
                print(f"  🔒 {f.name} パスワード保護完了")
        else:
            print(f"  ⚠️ staticrypt エラー: {result.stderr[:200]}")
    except Exception as e:
        print(f"  ⚠️ 暗号化エラー: {e}")

    # encrypted/ が別で生成された場合もクリーンアップ
    encrypted_dir = PROJECT_ROOT / "encrypted"
    if encrypted_dir.exists():
        import shutil as _shutil
        _shutil.rmtree(encrypted_dir)

    print(f"  🔐 パスワード保護: {SITE_PASSWORD[:2]}{'*' * (len(SITE_PASSWORD) - 2)}")


if __name__ == "__main__":
    build_site()
