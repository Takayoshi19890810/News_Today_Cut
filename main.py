import gspread
from google.auth import default
from datetime import datetime, timedelta

# ✅ 日付文字列
today = datetime.now()
yesterday_15 = datetime(today.year, today.month, today.day, 15) - timedelta(days=1)
today_15 = datetime(today.year, today.month, today.day, 15)

today_str = today.strftime("%y%m%d")

# ✅ 認証
creds, _ = default()
gc = gspread.authorize(creds)

# ✅ スプレッドシート取得
SPREADSHEET_ID = "1IYUuwzvlR2OJC8r3FkaUvA44tc0XGqT2kxbAXiMgt2s"
sh = gc.open_by_key(SPREADSHEET_ID)

# ✅ 既存シート削除 → Baseコピー
try:
    sh.del_worksheet(sh.worksheet(today_str))
except:
    pass

base_ws = sh.worksheet("Base")
target_ws = sh.duplicate_sheet(base_ws.id, new_sheet_name=today_str)

# ✅ ニュースソース順に処理
SOURCE_ORDER = ["MSN", "Google", "Yahoo"]
insert_row = 2  # A2から開始

def parse_datetime(s):
    try:
        return datetime.strptime(s.strip(), "%Y/%m/%d %H:%M")
    except:
        return None

for source in SOURCE_ORDER:
    ws = sh.worksheet(source)
    all_data = ws.get_all_values()

    for row in all_data[1:]:  # ヘッダーを除く
        if len(row) < 3:
            continue
        dt = parse_datetime(row[2])
        if dt and yesterday_15 <= dt < today_15:
            # A〜E列のみ貼り付け
            target_ws.update(f"A{insert_row}:E{insert_row}", [row[:5]])
            insert_row += 1
