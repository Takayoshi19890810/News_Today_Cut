import os
import json
import gspread
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials

# ✅ Google認証
service_account_info = json.loads(os.environ["GCP_SERVICE_ACCOUNT_KEY"])
creds = Credentials.from_service_account_info(
    service_account_info,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
gc = gspread.authorize(creds)

# ✅ スプレッドシート設定
SOURCE_SPREADSHEET_ID = "1RglATeTbLU1SqlfXnNToJqhXLdNoHCdePldioKDQgU8"
DEST_SPREADSHEET_ID = "1IYUuwzvlR2OJC8r3FkaUvA44tc0XGqT2kxbAXiMgt2s"

# ✅ 日付範囲（前日15:00〜当日15:00）
today = datetime.now()
today_str = today.strftime("%y%m%d")
yesterday_15 = datetime(today.year, today.month, today.day, 15) - timedelta(days=1)
today_15 = datetime(today.year, today.month, today.day, 15)

# ✅ スプレッドシート接続
source_book = gc.open_by_key(SOURCE_SPREADSHEET_ID)
dest_book = gc.open_by_key(DEST_SPREADSHEET_ID)

# ✅ 出力先シート準備（Baseをコピーして今日の日付に）
try:
    dest_book.del_worksheet(dest_book.worksheet(today_str))
except:
    pass
base_ws = dest_book.worksheet("Base")
target_ws = dest_book.duplicate_sheet(base_ws.id, new_sheet_name=today_str)

# ✅ 処理対象のソース順
SOURCE_ORDER = ["MSN", "Google", "Yahoo"]
all_rows = []

# ✅ 日時パース（C列）
def parse_datetime(s):
    try:
        return datetime.strptime(s.strip(), "%Y/%m/%d %H:%M")
    except:
        pass
    try:
        dt = datetime.strptime(s.strip(), "%m/%d %H:%M")
        return dt.replace(year=datetime.now().year)
    except:
        return None

# ✅ 感情分類（G列：ルールベース）
def classify_sentiment(title):
    positives = ["新登場", "好評", "快挙", "注目", "期待", "成功", "改善", "上昇", "記録", "好転", "前向き"]
    negatives = ["事故", "批判", "炎上", "懸念", "問題", "失敗", "下落", "苦戦", "赤字", "不正", "後退", "不安"]
    title = title.lower()
    if any(word in title for word in negatives):
        return "ネガティブ"
    elif any(word in title for word in positives):
        return "ポジティブ"
    else:
        return "ニュートラル"

# ✅ カテゴリ分類（H列：ルールベース）
def classify_category(title):
    categories = {
        "エンタメ": ["映画", "ドラマ", "俳優", "女優", "アニメ", "アイドル", "芸能", "歌手"],
        "スポーツ": ["野球", "サッカー", "ゴルフ", "テニス", "選手", "試合", "W杯", "五輪"],
        "会社": ["企業", "会社", "決算", "社長", "業績", "買収", "上場", "IR", "株主"],
        "技術": ["AI", "半導体", "IoT", "量子", "テクノロジー", "開発", "エンジニア", "ロボット"],
        "社会": ["政治", "法律", "事件", "災害", "裁判", "教育", "警察", "人口"],
        "車": ["トヨタ", "ホンダ", "日産", "車", "EV", "SUV", "走行", "自動運転", "試乗"],
        "投資": ["株", "日経平均", "為替", "利上げ", "経済", "インフレ", "資産", "金利", "投資"]
    }
    title = title.lower()
    for category, keywords in categories.items():
        if any(k.lower() in title for k in keywords):
            return category
    return "社会"

# ✅ 各ニュースソース処理
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

    for row in all_data[1:]:
        if len(row) < 4:
            skipped += 1
            continue
        dt = parse_datetime(row[2])
        if dt and yesterday_15 <= dt < today_15:
            title = row[0]
            sentiment = classify_sentiment(title)
            category = classify_category(title)
            all_rows.append([source] + row[:4] + ["", sentiment, category])
            source_count += 1
        else:
            skipped += 1

    print(f"✅ {source}: 貼付 {source_count} 件 / スキップ {skipped} 件")

# ✅ 一括出力（A2〜）
if all_rows:
    target_ws.update(values=all_rows, range_name="A2")
    print(f"✅ 合計 {len(all_rows)} 件を貼り付けました。")
else:
    print("⚠️ 該当するニュースが見つかりませんでした。")
