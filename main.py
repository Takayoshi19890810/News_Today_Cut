import os
import json
import gspread
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials

# ✅ Google認証（GitHub ActionsのSecretsから）
service_account_info = json.loads(os.environ["GCP_SERVICE_ACCOUNT_KEY"])
creds = Credentials.from_service_account_info(
    service_account_info,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
gc = gspread.authorize(creds)

# ✅ スプレッドシート設定
SPREADSHEET_ID = "1IYUuwzvlR2OJC8r3FkaUvA44tc0XGqT2kxbAXiMgt2s"
sh = gc.open_by_key(SPREADSHEET_ID)

# ✅ 日付取得（例：250612）
today = datetime.now()
today_str = today.strftime("%y%m%d")
yesterday_15 = datetime(today.year, today.month, today.day, 15) - timedelta(days=1)
today_15 = datetime(today.year, today.month, today.day, 15)

# ✅ シート「Base」をコピー → 本日の日付にリネーム
try:
    sh.del_worksheet(sh.worksheet(today_str))
except:
    pass

base_ws = sh.worksheet("Base")
target_ws = sh.duplicate_sheet(base_ws.id, new_sheet_name=today_str)

# ✅ データ収集対象：MSN → Google → Yahoo（この順で貼り付け）
SOURCE_ORDER = ["MSN", "Google", "Yahoo"]
insert_row = 2  # A2から貼り付け開始

def parse_datetime(s):
    try:
        return datetime.strptime(s.strip(), "%Y/%m/%d %H:%M")
    except:
        return None

for source in SOURCE_ORDER:
    try:
        ws = sh.worksheet(source)
        all_data = ws.get_all_values()
    except Exception as e:
        print(f"⚠️ シート '{source}' の取得でエラー: {e}")
        continue

    for row in all_data[1:]:  # ヘッダーを除く
        if len(row) < 3:
            continue
        dt = parse_datetime(row[2])
        if dt and yesterday_15 <= dt < today_15:
            # A〜E列のみコピー
            target_ws.update(f"A{insert_row}:E{insert_row}", [row[:5]])
            insert_row += 1
