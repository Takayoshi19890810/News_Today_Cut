import os
import json
import re
from datetime import datetime, timedelta
import gspread

# スプレッドシート設定
SOURCE_SPREADSHEET_ID = "1RglATeTbLU1SqlfXnNToJqhXLdNoHCdePldioKDQgU8"
TARGET_SPREADSHEET_ID = "1IYUuwzvlR2OJC8r3FkaUvA44tc0XGqT2kxbAXiMgt2s"
SOURCE_SHEETS = ["Google", "Yahoo", "MSN"]
NEWS_SOURCES = {"Google": "Google", "Yahoo": "Yahoo", "MSN": "MSN"}
DATE_COLUMN_INDEX = 2  # C列「投稿日」列のインデックス（0始まり）

def extract_articles(gc):
    sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    now = datetime.now()
    today_15 = datetime(now.year, now.month, now.day, 15, 0)
    yesterday_15 = today_15 - timedelta(days=1)

    extracted = []
    for sheet in SOURCE_SHEETS:
        try:
            ws = sh.worksheet(sheet)
            values = ws.get_all_values()
            if not values:
                continue
            rows = values[1:]

            for row in rows:
                try:
                    date_str = row[DATE_COLUMN_INDEX].strip()

                    # Yahoo形式: 年無し + 時刻 → 年補完
                    if re.match(r"^\d{1,2}/\d{1,2} \d{1,2}:\d{2}$", date_str):
                        date_str = f"{now.year}/{date_str}"

                    # MSN形式1: 月/日（6/10）→ 年と00:00時刻補完
                    elif re.match(r"^\d{1,2}/\d{1,2}$", date_str):
                        date_str = f"{now.year}/{date_str} 00:00"

                    # MSN形式2: 年/月/日（2025/6/10）→ 時刻補完
                    elif re.match(r"^\d{4}/\d{1,2}/\d{1,2}$", date_str):
                        date_str = f"{date_str} 00:00"

                    dt = datetime.strptime(date_str, "%Y/%m/%d %H:%M")

                    if yesterday_15 <= dt < today_15:
                        # A:ニュース元 / B:タイトル / C:URL / D:投稿日 / E:引用元
                        extracted.append([
                            NEWS_SOURCES[sheet],  # A列: ニュース元
                            row[0],               # B列: タイトル
                            row[1],               # C列: URL
                            date_str,             # D列: 投稿日
                            row[3] if len(row) > 3 else ""  # E列: 引用元
                        ])
                except Exception as e:
                    print(f"⚠️ {sheet} スキップ: {row[DATE_COLUMN_INDEX]} → {e}")
                    continue
        except Exception as e:
            print(f"❌ {sheet} 読み込みエラー: {e}")
            continue

    headers = ["ニュース元", "タイトル", "URL", "投稿日", "引用元"]
    return headers, extracted

def overwrite_sheet(gc, sheet_name, headers, data):
    sh = gc.open_by_key(TARGET_SPREADSHEET_ID)

    try:
        ws_existing = sh.worksheet(sheet_name)
        sh.del_worksheet(ws_existing)
        print(f"🗑 シート「{sheet_name}」を削除しました。")
    except:
        pass

    ws = sh.add_worksheet(title=sheet_name, rows="100", cols=str(len(headers)))
    ws.append_row(headers)

    if data:
        ws.append_rows(data, value_input_option='USER_ENTERED')
        print(f"✅ {len(data)} 件をシート「{sheet_name}」に出力しました。")
    else:
        print("⚠️ 対象データがありません。")

def main():
    credentials = json.loads(os.environ["GCP_SERVICE_ACCOUNT_KEY"])
    gc = gspread.service_account_from_dict(credentials)
    headers, data = extract_articles(gc)
    sheet_name = datetime.now().strftime("%y%m%d")  # 例: 250610
    overwrite_sheet(gc, sheet_name, headers, data)

if __name__ == "__main__":
    main()
