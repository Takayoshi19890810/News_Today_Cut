import os
import json
import gspread
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials

# ✅ Google認証（Secretsからサービスアカウントキーを取得）
service_account_info = json.loads(os.environ["GCP_SERVICE_ACCOUNT_KEY"])
creds = Credentials.from_service_account_info(
    service_account_info,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
gc = gspread.authorize(creds)

# ✅ スプレッドシートID
SOURCE_SPREADSHEET_ID = "1RglATeTbLU1SqlfXnNToJqhXLdNoHCdePldioKDQgU8"  # ← データ元（MSN, Google, Yahoo）
DEST_SPREADSHEET_ID = "1IYUuwzvlR2OJC8r3FkaUvA44tc0XGqT2kxbAXiMgt2s"   # ← 貼り付け先（Base コピー先）

# ✅ 日付関連
today = datetime.now()
today_str = today.strftime("%y%m%d")
yesterday_15 = datetime(today.year, today.month, today.day, 15) - timedelta(days=1)
today_15 = datetime(today.year, today.month, today.day, 15)

# ✅ スプレッドシート接続
source_book = gc.open_by_key(SOURCE_SPREADSHEET_ID)
dest_book = gc.open_by_key(DEST_SPREADSHEET_ID)

# ✅ 出力先シート作成（Baseをコピーして日付シートに）
try:
    dest_book.del_worksheet(dest_book.worksheet(today_str))
except:
    pass

base_ws = dest_book.worksheet("Base")
target_ws = dest_book.duplicate_sheet(base_ws.id, new_sheet_name=today_str)

# ✅ ソース順：MSN → Google → Yahoo
SOURCE_ORDER = ["MSN", "Google", "Yahoo"]
insert_row = 2  # A2 から貼り付け

def parse_datetime(s):
    try:
        return datetime.strptime(s.strip(), "%Y/%m/%d %H:%M")
    except:
        return None

# ✅ 各ニュースソースから抽出して貼り付け
for source in SOURCE_ORDER:
    try:
        ws = source_book.worksheet(source)
        all_data = ws.get_all_values()
    except Exception as e:
        print(f"⚠️ シート '{source}' 読み込みエラー: {e}")
        continue

    for row in all_data[1:]:  # 1行目はヘッダー
        if len(row) < 3:
            continue
        dt = parse_datetime(row[2])
        if dt and yesterday_15 <= dt < today_15:
            # A〜E列のみ貼り付け
            target_ws.update(f"A{insert_row}:E{insert_row}", [row[:5]])
            insert_row += 1
