import os
import json
import re
from datetime import datetime, timedelta
import gspread

# スプレッドシート設定
SOURCE_SPREADSHEET_ID = "1RglATeTbLU1SqlfXnNToJqhXLdNoHCdePldioKDQgU8"
TARGET_SPREADSHEET_ID = "1IYUuwzvlR2OJC8r3FkaUvA44tc0XGqT2kxbAXiMgt2s"
SOURCE_SHEETS = ["MSN", "Google", "Yahoo"]
NEWS_SOURCES = {"Google": "Google", "Yahoo": "Yahoo", "MSN": "MSN"}
DATE_COLUMN_INDEX = 2  # C列「投稿日」

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

                    # 各種日付形式の補正
                    if re.match(r"^\d{1,2}/\d{1,2} \d{1,2}:\d{2}$", date_str):  # MM/DD HH:MM
                        date_str = f"{now.year}/{date_str}"
                    elif re.match(r"^\d{1,2}/\d{1,2}$", date_str):  # MM/DD
                        date_str = f"{now.year}/{date_str} 00:00"
                    elif re.match(r"^\d{4}/\d{1,2}/\d{1,2}$", date_str):  # YYYY/MM/DD
                        date_str = f"{date_str} 00:00"
                    elif re.match(r"^\d{1,2}/\d{1,2}/\d{4}$", date_str):  # MM/DD/YYYY
                        date_str = f"{date_str} 00:00"
                        date_str = datetime.strptime(date_str, "%m/%d/%Y %H:%M").strftime("%Y/%m/%d %H:%M")

                    dt = datetime.strptime(date_str, "%Y/%m/%d %H:%M")

                    if yesterday_15 <= dt < today_15:
                        extracted.append([
                            NEWS_SOURCES[sheet],     # A:ニュースサイト
                            row[0],                  # B:タイトル
                            row[1],                  # C:URL
                            date_str,                # D:投稿日時
                            row[3] if len(row) > 3 else "",  # E:ソース
                            "", "", "",               # F〜H: コメント数, ポジ/ネガ, カテゴリー
                            f'=IFERROR(VLOOKUP(C{len(extracted)+2},ダブり!C:L,10,FALSE),"")',  # I:ダブりチェック
                            "",                      # J:タイトル抜粋
                            ""                       # K:番号（あとで入力）
                        ])
                except Exception as e:
                    print(f"⚠️ {sheet} スキップ: {row[DATE_COLUMN_INDEX]} → {e}")
        except Exception as e:
            print(f"❌ {sheet} 読み込みエラー: {e}")

    headers = [
        "ニュースサイト", "タイトル", "URL", "投稿日時", "ソース",
        "コメント数", "ポジ/ネガ", "カテゴリー", "ダブりチェック",
        "タイトル抜粋", "番号"
    ]
    return headers, extracted

def overwrite_sheet(gc, sheet_name, headers, data):
    sh = gc.open_by_key(TARGET_SPREADSHEET_ID)

    try:
        ws_existing = sh.worksheet(sheet_name)
        sh.del_worksheet(ws_existing)
        print(f"🗑 シート「{sheet_name}」を削除しました。")
    except:
        pass

    ws = sh.add_worksheet(title=sheet_name, rows="1", cols=str(len(headers)))
    ws.append_row(headers)

    if data:
        max_rows = len(data)

        # 行数を事前に拡張（append_rows前に反映されない可能性があるため、後で実行）
        ws.append_rows(data, value_input_option='USER_ENTERED')
        ws.resize(rows=max_rows + 10)

        # L列：番号付与（最大行数に安全配慮して1000行を上限）
        safe_limit = min(max_rows, 1000)
        cell_range = ws.range(f"L2:L{safe_limit+1}")
        for idx, cell in enumerate(cell_range, 1):
            cell.value = idx
        ws.update_cells(cell_range)

        print(f"✅ {max_rows} 件をシート「{sheet_name}」に出力しました。")
    else:
        print("⚠️ 対象データがありません。")

def main():
    credentials = json.loads(os.environ["GCP_SERVICE_ACCOUNT_KEY"])
    gc = gspread.service_account_from_dict(credentials)
    headers, data = extract_articles(gc)
    sheet_name = datetime.now().strftime("%y%m%d")  # 例: 250611
    overwrite_sheet(gc, sheet_name, headers, data)

if __name__ == "__main__":
    main()
