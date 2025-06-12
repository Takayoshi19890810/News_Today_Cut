import os
import json
import gspread
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials

# ✅ 認証
service_account_info = json.loads(os.environ["GCP_SERVICE_ACCOUNT_KEY"])
creds = Credentials.from_service_account_info(
    service_account_info,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
gc = gspread.authorize(creds)

# ✅ スプレッドシート設定
SOURCE_SPREADSHEET_ID = "1RglATeTbLU1SqlfXnNToJqhXLdNoHCdePldioKDQgU8"  # データ元
DEST_SPREADSHEET_ID = "1IYUuwzvlR2OJC8r3FkaUvA44tc0XGqT2kxbAXiMgt2s"    # 出力先

# ✅ 日付範囲：前日15時〜当日15時
today = datetime.now()
today_str = today.strftime("%y%m%d")
yesterday_15 = datetime(today.year, today.month, today.day, 15) - timedelta(days=1)
today_15 = datetime(today.year, today.month, today.day, 15)

# ✅ スプレッドシート接続
source_book = gc.open_by_key(SOURCE_SPREADSHEET_ID)
dest_book = gc.open_by_key(DEST_SPREADSHEET_ID)

# ✅ 出力先シート作成（Baseコピー→日付名に）
try:
    dest_book.del_worksheet(dest_book.worksheet(today_str))
except:
    pass

base_ws = dest_book.worksheet("Base")
target_ws = dest_book.duplicate_sheet(base_ws.id, new_sheet_name=today_str)

# ✅ ソース順と結果格納リスト
SOURCE_ORDER = ["MSN", "Google", "Yahoo"]
all_rows = []

# ✅ 日時パース（年なし形式にも対応）
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

# ✅ 感情分類（簡易キーワード方式）
def classify_sentiment(title):
    positives = ["新登場", "好評", "成功", "快挙", "注目", "期待", "進化", "魅力", "強化"]
    negatives = ["事故", "批判", "炎上", "問題", "不安", "失敗", "懸念", "課題"]
    title = title.lower()
    if any(word in title for word in negatives):
        return "ネガティブ"
    elif any(word in title for word in positives):
        return "ポジティブ"
    else:
        return "ニュートラル"

# ✅ カテゴリ分類（キーワードベース）
def classify_category(title):
    categories = {
        "エンタメ": ["映画", "ドラマ", "俳優", "女優", "アイドル", "音楽"],
        "スポーツ": ["試合", "選手", "優勝", "リーグ", "五輪", "W杯", "ゴルフ", "野球"],
        "会社": ["企業", "社長", "業績", "決算", "上場", "買収"],
        "技術": ["AI", "半導体", "開発", "テクノロジー", "特許", "エンジニア"],
        "社会": ["政治", "教育", "事件", "災害", "法律", "政府"],
        "車": ["トヨタ", "日産", "車", "EV", "自動運転", "試乗", "SUV"],
        "投資": ["株", "投資", "為替", "金利", "資産", "経済", "円安"]
    }
    title = title.lower()
    for category, keywords in categories.items():
        if any(k.lower() in title for k in keywords):
            return category
    return "社会"

# ✅ 各ニュースソースを処理
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
            all_rows.append([source] + row[:4] + ["", sentiment, category])  # F列空欄
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
