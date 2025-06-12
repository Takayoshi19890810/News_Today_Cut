import os
import json
import gspread
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
from transformers import pipeline

# ✅ Hugging Face 感情分析モデル（日本語BERT）
sentiment_analyzer = pipeline(
    "sentiment-analysis",
    model="daigo/bert-base-japanese-sentiment",
    tokenizer="daigo/bert-base-japanese-sentiment"
)

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

# ✅ 日付範囲
today = datetime.now()
today_str = today.strftime("%y%m%d")
yesterday_15 = datetime(today.year, today.month, today.day, 15) - timedelta(days=1)
today_15 = datetime(today.year, today.month, today.day, 15)

# ✅ 接続
source_book = gc.open_by_key(SOURCE_SPREADSHEET_ID)
dest_book = gc.open_by_key(DEST_SPREADSHEET_ID)

# ✅ 出力先シート（Base → 今日の日付）
try:
    dest_book.del_worksheet(dest_book.worksheet(today_str))
except:
    pass
base_ws = dest_book.worksheet("Base")
target_ws = dest_book.duplicate_sheet(base_ws.id, new_sheet_name=today_str)

# ✅ データ処理対象シート
SOURCE_ORDER = ["MSN", "Google", "Yahoo"]
all_rows = []

# ✅ 日時パース関数
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

# ✅ 感情分析（G列）
def classify_sentiment_hf(text):
    try:
        result = sentiment_analyzer(text[:256])[0]
        label = result["label"]
        if "POSITIVE" in label:
            return "ポジティブ"
        elif "NEGATIVE" in label:
            return "ネガティブ"
        else:
            return "ニュートラル"
    except:
        return "不明"

# ✅ カテゴリ分類（H列）※ルールベース
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

# ✅ 処理ループ
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
            sentiment = classify_sentiment_hf(title)
            category = classify_category(title)
            all_rows.append([source] + row[:4] + ["", sentiment, category])  # F列は空欄
            source_count += 1
        else:
            skipped += 1

    print(f"✅ {source}: 貼付 {source_count} 件 / スキップ {skipped} 件")

# ✅ 一括貼り付け
if all_rows:
    target_ws.update(values=all_rows, range_name="A2")
    print(f"✅ 合計 {len(all_rows)} 件を貼り付けました。")
else:
    print("⚠️ 該当期間のニュースが見つかりませんでした。")
