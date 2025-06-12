import os
import json
import gspread
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials

# 認証（GitHub Secrets: GCP_SERVICE_ACCOUNT_KEY を使用）
service_account_info = json.loads(os.environ["GCP_SERVICE_ACCOUNT_KEY"])
creds = Credentials.from_service_account_info(
    service_account_info,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
gc = gspread.authorize(creds)

# スプレッドシートID設定
SOURCE_SPREADSHEET_ID = "1RglATeTbLU1SqlfXnNToJqhXLdNoHCdePldioKDQgU8"  # データ元（ニュース収集元）
DEST_SPREADSHEET_ID = "1IYUuwzvlR2OJC8r3FkaUvA44tc0XGqT2kxbAXiMgt2s"    # 出力先（Baseコピー先）

# 日付と時間範囲の定義
today = datetime.now()
today_str = today.strftime("%y%m%d")
yesterday_15 = datetime(today.year, today.month, today.day, 15) - timedelta(days=1)
today_15 = datetime(today.year, today.month, today.day, 15)

# スプレッドシート接続
source_book = gc.open_by_key(SOURCE_SPREADSHEET_ID)
dest_book = gc.open_by_key(DEST_SPREADSHEET_ID)

# 出力先シート作成（Baseコピー→日付リネーム）
try:
    dest_book.del_worksheet(dest_book.worksheet(today_str))
except:
    pass

base_ws = dest_book.worksheet("Base")
target_ws = dest_book.duplicate_sheet(base_ws.id, new_sheet_name=today_str)

# 処理順
SOURCE_ORDER = ["MSN", "Google", "Yahoo"]

# 日時パース関数（C列フォーマット：YYYY/MM/DD HH:MM）
def parse_datetime(s):
    try:
        return datetime.strptime(s.strip(), "%Y/%m/%d %H:%M")
    except:
        return None

# 貼付データ収集
all_rows = []

for source in SOURCE_ORDER:
    try:
        ws = source_book.worksheet(source)
        all_data = ws.get_all_values()
    except Exception as e:
        print(f"⚠️ シート '{source}' 読み込みエラー: {e}")
        continue

    for row in all_data[1:]:  # 1行目はヘッダー
        if len(row) < 4:
            continue
        dt = parse_datetime(row[2])
        if dt and yesterday_15 <= dt < today_15:
            # ["MSN", タイトル, URL, 投稿日時, ソース]
            all_rows.append([source] + row[:4])

# 一括貼付け（A2から）
if all_rows:
    target_ws.update(values=all_rows, range_name="A2")
    print(f"✅ {len(all_rows)} 件のニュースを貼り付けました。")
else:
    print("⚠️ 該当期間のニュースデータが見つかりませんでした。")
