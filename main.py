import os
import json
import gspread
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials

# 認証
service_account_info = json.loads(os.environ["GCP_SERVICE_ACCOUNT_KEY"])
creds = Credentials.from_service_account_info(
    service_account_info,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
gc = gspread.authorize(creds)

# スプレッドシートID
SOURCE_SPREADSHEET_ID = "1RglATeTbLU1SqlfXnNToJqhXLdNoHCdePldioKDQgU8"
DEST_SPREADSHEET_ID = "1IYUuwzvlR2OJC8r3FkaUvA44tc0XGqT2kxbAXiMgt2s"

# 日付設定
today = datetime.now()
today_str = today.strftime("%y%m%d")
yesterday_15 = datetime(today.year, today.month, today.day, 15) - timedelta(days=1)
today_15 = datetime(today.year, today.month, today.day, 15)

# 接続
source_book = gc.open_by_key(SOURCE_SPREADSHEET_ID)
dest_book = gc.open_by_key(DEST_SPREADSHEET_ID)

# 出力先シート作成
try:
    dest_book.del_worksheet(dest_book.worksheet(today_str))
except:
    pass
base_ws = dest_book.worksheet("Base")
target_ws = dest_book.duplicate_sheet(base_ws.id, new_sheet_name=today_str)

# ソース順と収集用リスト
SOURCE_ORDER = ["MSN", "Google", "Yahoo"]
all_rows = []

def parse_datetime(s):
    # フォーマット緩めに対応
    try:
        return datetime.strptime(s.strip(), "%Y/%m/%d %H:%M")
    except:
        return None

for source in SOURCE_ORDER:
    try:
        ws = source_book.worksheet(source)
        all_data = ws.get_all_values()
    except Exception as e:
        print(f"⚠️ シート '{source}' 読み込みエラー: {e}")
        continue

    print(f"📥 {source}: {len(all_data)-1}件のデータ行を処理中...")

    source_count = 0
    skipped = 0

    for row in all_data[1:]:  # ヘッダー除外
        if len(row) < 4:
            skipped += 1
            continue
        dt = parse_datetime(row[2])
        if dt and yesterday_15 <= dt < today_15:
            all_rows.append([source] + row[:4])
            source_count += 1
        else:
            skipped += 1

    print(f"✅ {source}: 貼付 {source_count} 件 / スキップ {skipped} 件")

# 一括貼付け
if all_rows:
    target_ws.update(values=all_rows, range_name="A2")
    print(f"✅ 合計 {len(all_rows)} 件を貼り付けました。")
else:
    print("⚠️ 該当するニュースが見つかりませんでした。")
