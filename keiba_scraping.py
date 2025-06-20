#Pythonコード
import csv
import os
import requests
import time
import random
from bs4 import BeautifulSoup
from datetime import datetime
from collections import deque

URL_BASE = "https://db.netkeiba.com/race/"
CSV_DIR = "./data/"
OUTPUT_FILE = f"{CSV_DIR}v25y0005_data02.csv"

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1'
]

# リクエスト間隔を管理するためのdeque
request_timestamps = deque(maxlen=5) # 直近5回のタイムスタンプを保持（例）

def rate_limited_request():
    """リクエストレートを制限する（最低5秒間隔）。5回連続で短い間隔だった場合、長めに待機する可能性も考慮"""
    now = time.time()
    if len(request_timestamps) == request_timestamps.maxlen:
        time_since_oldest = now - request_timestamps[0]
        # 例えば、直近5回のリクエストが10秒以内に行われていたら少し長めに待つ
        if time_since_oldest < 10:
            wait_extra = random.uniform(5, 10)
            print(f"[INFO] Short interval detected. Waiting an extra {wait_extra:.1f} seconds...")
            time.sleep(wait_extra)

    if request_timestamps and now - request_timestamps[-1] < 5:
        wait_time = max(0, 2 - (now - request_timestamps[-1])) + random.uniform(0.5, 2) # 最低5秒 + α
        print(f"[INFO] Waiting {wait_time:.1f} seconds before next request...")
        time.sleep(wait_time)
    request_timestamps.append(time.time()) # 実際の時間はリクエスト送信直前に入れるのが理想だが、ここでは簡略化

def get_headers():
    """ランダムなUser-Agentを返す"""
    return {'User-Agent': random.choice(USER_AGENTS)}

def get_race_data(race_id):
    """指定されたrace_idのレースデータをnetkeiba.comから取得して構造化する"""
    url = URL_BASE + race_id
    try:
        rate_limited_request() # リクエスト前に待機チェック
        print(f"[INFO] Accessing: {url}")
        res = requests.get(url, headers=get_headers(), timeout=15) # タイムアウトを少し延長
        res.raise_for_status() # ステータスコードが200以外なら例外を発生させる
        res.encoding = res.apparent_encoding
        soup = BeautifulSoup(res.text, 'lxml')

        race_info_box = soup.find("div", class_="data_intro")
        if not race_info_box:
            print(f"[WARN] Race info box not found for race_id: {race_id}")
            return []

        # --- レース情報の抽出 ---
        race_name_tag = race_info_box.find("h1")
        race_name = race_name_tag.text.strip() if race_name_tag else "レース名不明"

        race_data_p = race_info_box.find("p")
        race_data_text = race_data_p.text.strip().replace('\xa0', ' ') if race_data_p else ""

        lines = race_data_text.split("\n")
        race_date_str = lines[0].split("：")[-1].strip() if len(lines) > 0 and "： " in lines[0] else ""
        try:
            race_date = datetime.strptime(race_date_str, "%Y年%m月%d日").strftime("%Y-%m-%d") if race_date_str else ""
        except ValueError:
            print(f"[WARN] Could not parse date: {race_date_str} in race {race_id}")
            race_date = ""

        info_line = lines[1].strip() if len(lines) > 1 else ""
        開催 = info_line.split(" ")[0] if info_line else ""
        クラス, 距離, 芝ダート, 回り, 馬場, 天気 = "", "", "", "", "", ""

        # "クラス"情報が含まれているかチェックして抽出 (例: '3歳未勝利', 'G1' など)
        # これはページ構造によって異なるため、より堅牢な方法が必要になる場合がある
        details_span = race_info_box.find("span")
        if details_span:
            details_text = details_span.text.strip().replace('\xa0', ' ')
            details_parts = details_text.split('/')
            # --- 詳細情報の解析 ---
            # この部分はサイトの構造変更に弱い可能性があるため注意
            for part in details_parts:
                part = part.strip()
                if "m" in part:
                    芝ダート = "芝" if "芝" in part else "ダ" if "ダ" in part else ""
                    距離 = ''.join(filter(str.isdigit, part)) # 数字のみ抽出
                    回り = "右" if "右" in part else "左" if "左" in part else ""
                elif "天候" in part:
                    天気 = part.split("：")[-1]
                elif "馬場" in part:
                    # 馬場状態が詳細情報に含まれる場合 (例: 芝：良)
                    if '芝' in part or 'ダ' in part:
                        馬場 = part.split("：")[-1]
                    # 馬場状態が独立している場合 (例: 良)
                    elif any(b in part for b in ["稍", "良", "不", "重"]):
                        馬場 = part
        # クラス情報は h1 タグの隣など、別の場所にある可能性もあるため、必要に応じて別途取得ロジックを追加

        place_id = race_id[4:6]
        place_name = 開催.split("回")[-1].split("日")[0] if 開催 else "" # 例: "1回東京1日" -> "東京"

        # --- レース結果テーブルの抽出 ---
        race_table = soup.find("table", class_="race_table_01 nk_tb_common")
        if race_table is None:
            print(f"[WARN] Race result table not found for race_id: {race_id}")
            return []

        rows = race_table.find_all("tr")
        if len(rows) < 2: # ヘッダー行 + データ行が最低1つないと処理できない
            print(f"[WARN] No data rows found in table for race_id: {race_id}")
            return []

        rows = rows[1:] # ヘッダー行を除外
        race_data = []

        for i, row in enumerate(rows):
            cols = row.find_all("td")
            # === 修正点: 列数チェックを強化 ===
            # 必須データ(着順～タイム、単勝、人気、馬体重)が存在するであろうインデックス14までチェック
            if len(cols) < 15:
                print(f"[WARN] Row {i+1} in race {race_id} has less than 15 columns ({len(cols)}), skipping.")
                continue
            try:
                # --- 各列データの抽出 ---
                着順 = cols[0].text.strip()
                枠番 = cols[1].text.strip()  # 枠番を追加
                馬番 = cols[2].text.strip()
                馬名 = cols[3].text.strip()
                性齢 = cols[4].text.strip()
                斤量 = cols[5].text.strip()
                騎手 = cols[6].text.strip()
                走破時間 = cols[7].text.strip()

                # === 修正点: 正しいインデックスを参照 ===
                通過順 = cols[10].text.strip() if len(cols) > 10 else ""
                上がり = cols[11].text.strip() if len(cols) > 11 else ""
                オッズ = cols[12].text.strip() if len(cols) > 12 else ""
                人気 = cols[13].text.strip() if len(cols) > 13 else ""
                馬体重_データ = cols[14].text.strip()

                # 性別と年齢を分割
                sex, age = "", ""
                if len(性齢) >= 2:
                    sex = 性齢[0]
                    age = 性齢[1:]

                # 体重と体重変化を分割
                weight, weight_diff = "", ""
                if 馬体重_データ and '(' in 馬体重_データ and ')' in 馬体重_データ:
                    parts = 馬体重_データ.replace(")", "").split("(")
                    if len(parts) == 2:
                        weight = parts[0]
                        weight_diff = parts[1]
                elif 馬体重_データ: # 体重のみの場合 (例: 計不)
                    weight = 馬体重_データ

                # 取得データを辞書に格納
                data = {
                    "race_id": race_id,
                    "着順": 着順,
                    "枠番": 枠番,  # 枠番を追加
                    "馬番": 馬番,
                    "馬": 馬名,
                    "性": sex,
                    "齢": age,
                    "斤量": 斤量,
                    "騎手": 騎手,
                    "走破時間": 走破時間,
                    "通過順": 通過順, # 修正済み
                    "上がり": 上がり, # 修正済み
                    "人気": 人気,
                    "オッズ": オッズ,
                    "体重": weight, # 修正済み (元データ参照先変更)
                    "体重変化": weight_diff, # 修正済み (元データ参照先変更)
                    # --- レース情報 ---
                    "レース名": race_name,
                    "日付": race_date,
                    "開催": 開催,
                    "クラス": クラス, # 注意: クラス情報の取得は改善が必要な場合あり
                    "芝・ダート": 芝ダート,
                    "距離": 距離,
                    "回り": 回り,
                    "馬場": 馬場,
                    "天気": 天気,
                    "場id": place_id,
                    "場名": place_name
                }
                race_data.append(data)
            except Exception as e:
                print(f"[WARN] Parsing error in row {i+1} for race {race_id}: {e}")
                # エラーが発生した行のcols内容を出力するとデバッグに役立つ
                # print(f"[DEBUG] Problematic row data: {[c.text.strip() for c in cols]}")
                continue

        return race_data

    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Request failed for race {race_id}: {e}")
        return []
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred while processing race {race_id}: {e}")
        return []

def clean_data(data):
    """辞書のリストを受け取り、文字列型の値に含まれるNBSPをスペースに置き換える + 通過順に'を追加 + 走破時間を秒単位に変換 + 芝・ダートと回りを数値に変換 + 性と天気を数値に変換"""
    cleaned = []
    for row in data:
        new_row = {}
        for k, v in row.items():
            if isinstance(v, str):
                v = v.replace('\xa0', ' ')
                # 通過順には先頭に ' を追加（ただし重複防止）
                if k == "通過順" and not v.startswith("'") and v:
                    v = f"'{v}"
                # 走破時間を秒単位に変換
                elif k == "走破時間" and v:
                    try:
                        if ":" in v:  # "分:秒" 形式の場合
                            minutes, seconds = v.split(":")
                            total_seconds = float(minutes) * 60 + float(seconds)
                            v = f"{total_seconds:.1f}"
                    except ValueError:
                        print(f"[WARN] Could not convert race time: {v}")
                # 芝・ダートを数値に変換（芝=1, ダート=0）
                elif k == "芝・ダート":
                    if v == "芝":
                        v = "1"
                    elif v == "ダ":
                        v = "0"
                # 回りを数値に変換（右=1, 左=0）
                elif k == "回り":
                    if v == "右":
                        v = "1"
                    elif v == "左":
                        v = "0"
                # 性を数値に変換（牡=1, 牝=0, セ=1）
                elif k == "性":
                    if v == "牡" or v == "セ":
                        v = "1"
                    elif v == "牝":
                        v = "0"
                # 天気を数値に変換（晴=1, 曇=0, 雨=-1）
                elif k == "天気":
                    if "晴" in v:
                        v = "1"
                    elif "曇" in v:
                        v = "0"
                    elif "雨" in v:
                        v = "-1"
                new_row[k] = v
            else:
                new_row[k] = v
        cleaned.append(new_row)
    return cleaned

def append_to_csv(data, filepath):
    if not data:
        return
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    file_exists = os.path.isfile(filepath)

    data_to_write = clean_data(data)

    fieldnames = [
        "race_id", "着順", "枠番", "馬番", "馬", "性", "齢", "斤量", "騎手", "走破時間",
        "通過順", "上がり", "人気", "オッズ", "体重", "体重変化",
        "レース名", "日付", "開催", "クラス", "芝・ダート", "距離",
        "回り", "馬場", "天気", "場id", "場名"
    ]

    try:
        with open(filepath, "a", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore', quoting=csv.QUOTE_ALL)
            if not file_exists:
                writer.writeheader()
            writer.writerows(data_to_write)

        # --- 追加: txtファイルへの追記 ---
        txt_filepath = filepath.replace(".csv", ".txt")
        with open(txt_filepath, "a", encoding="utf-8") as txtfile:
            for row in data_to_write:
                line = "\t".join([str(row.get(col, "")) for col in fieldnames])
                txtfile.write(line + "\n")

    except IOError as e:
        print(f"[ERROR] Failed to write to CSV file {filepath}: {e}")
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred during CSV writing: {e}")

def filter_race_by_conditions(race_data, distance_conditions=None, surface_conditions=None):
    """
    レースデータを距離と芝・ダートの条件でフィルタリングする
    
    Args:
        race_data: レースデータのリスト
        distance_conditions: 距離条件のリスト (例: ["1200", "1600", "2000"])
        surface_conditions: 芝・ダート条件のリスト (例: ["芝", "ダ"] または ["1", "0"])
    
    Returns:
        条件に合致するレースデータのリスト
    """
    if not race_data:
        return []
    
    # 条件が指定されていない場合は全データを返す
    if not distance_conditions and not surface_conditions:
        return race_data
    
    # 最初のレースデータから条件をチェック（同じレース内では距離と芝・ダートは同じ）
    first_race = race_data[0]
    race_distance = first_race.get("距離", "")
    race_surface = first_race.get("芝・ダート", "")
    
    # 距離条件のチェック
    if distance_conditions and race_distance not in distance_conditions:
        print(f"[INFO] Skipping race {first_race.get('race_id', '')} - distance {race_distance}m not in conditions {distance_conditions}")
        return []
    
    # 芝・ダート条件のチェック（文字列と数値の両方に対応）
    if surface_conditions:
        # 芝・ダートの値を数値に変換して比較
        surface_value = race_surface
        if race_surface == "芝":
            surface_value = "1"
        elif race_surface == "ダ":
            surface_value = "0"
        
        # 条件も数値に変換
        numeric_conditions = []
        for condition in surface_conditions:
            if condition == "芝":
                numeric_conditions.append("1")
            elif condition == "ダ":
                numeric_conditions.append("0")
            else:
                numeric_conditions.append(condition)  # 既に数値の場合
        
        if surface_value not in numeric_conditions:
            print(f"[INFO] Skipping race {first_race.get('race_id', '')} - surface {race_surface} (value: {surface_value}) not in conditions {surface_conditions}")
            return []
    
    print(f"[INFO] Including race {first_race.get('race_id', '')} - distance: {race_distance}m, surface: {race_surface}")
    return race_data

def main():
    """メイン処理: 指定された範囲のレースIDのデータを取得しCSVに保存する"""
    """
    === 使用方法 ===
    1. 年度指定:
       - 単年指定: target_year = "2024" または target_years = ["2024"]
       - 複数年度指定: target_years = ["2020", "2021", "2022", "2023", "2024"]
       - 年度範囲指定: target_years = list(range(2020, 2025))  # 2020-2024
    
    2. 開催回数・開催日目指定:
       - 単一指定: target_kaisai = "01", target_nichi = "01"
       - 複数指定: target_kaisai_list = ["01", "02", "03"], target_nichi_list = ["01", "02", "03"]
       - 範囲指定: target_kaisai_list = [f"{i:02d}" for i in range(1, 6)]  # 1回から5回
    
    3. 距離条件の指定:
       - distance_conditions = ["1200", "1600", "2000"]  # 特定の距離のみ
       - distance_conditions = None  # 全距離
    
    4. 芝・ダート条件の指定:
       - surface_conditions = ["芝"]  # 芝コースのみ
       - surface_conditions = ["ダ"]  # ダートコースのみ
       - surface_conditions = ["芝", "ダ"]  # 両方
       - surface_conditions = ["1"]  # 芝コースのみ（数値指定）
       - surface_conditions = ["0"]  # ダートコースのみ（数値指定）
       - surface_conditions = ["1", "0"]  # 両方（数値指定）
       - surface_conditions = None  # 全コース
    
    5. 条件の組み合わせ例:
       - 芝の短距離レースのみ: distance_conditions=["1200", "1400"], surface_conditions=["芝"]
       - ダートの中距離レースのみ: distance_conditions=["1600", "1800"], surface_conditions=["ダ"]
       - 特定開催回数のみ: target_kaisai_list = ["01", "05"]  # 1回と5回開催のみ
    """
    
    # === 年度指定（単年または複数年度） ===
    # 方法1: 単年指定（従来の方法）
    # target_year = "2024"
    
    # 方法2: 複数年度指定（新しい方法）
    # 例: 2020年から2024年まで
    target_years = ["2025"]
    
    # 方法3: 年度範囲指定（rangeを使用）
    # target_years = [str(year) for year in range(2020, 2025)]  # 2020-2024
    
    # 方法4: 特定の年度のみ
    # target_years = ["2022", "2024"]  # 2022年と2024年のみ
    
    # 方法5: 単年指定（リスト形式）
    # target_years = ["2024"]
    
    # === 年度設定例（コメントアウトを外して使用） ===
    # 直近3年間
    # target_years = ["2022", "2023", "2024"]
    
    # 特定の年度のみ
    # target_years = ["2020", "2022", "2024"]  # 偶数年のみ
    
    # 年度範囲指定（rangeを使用）
    # target_years = [str(year) for year in range(2018, 2025)]  # 2018-2024
    
    # 単年指定（従来の方法との互換性）
    # target_years = ["2024"]
    
    # 全年度（例：2015年から現在まで）
    # target_years = [str(year) for year in range(2015, 2025)]
    
    # 取得したいレースIDの範囲を指定
    # === 開催回数と開催日目の指定 ===
    # 方法1: 単一の開催回数・開催日目
    # target_kaisai = "01" # 開催回数（01=1回、02=2回、...）
    # target_nichi = "01" # 開催日目（01=1日目、02=2日目、...）
    
    # 方法2: 複数の開催回数・開催日目（リスト形式）
    # target_kaisai_list = ["01", "02", "03"]  # 1回、2回、3回開催
    # target_nichi_list = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]  # 1日目から12日目
    
    # 方法3: 開催回数・開催日目の範囲指定
    # target_kaisai_list = [f"{i:02d}" for i in range(1, 6)]  # 1回から5回開催
    # target_nichi_list = [f"{i:02d}" for i in range(1, 13)]  # 1日目から12日目
    
    # === 設定例（コメントアウトを外して使用） ===
    # 全開催回数・全開催日目
    # target_kaisai_list = [f"{i:02d}" for i in range(1, 11)]  # 1回から10回開催
    # target_nichi_list = [f"{i:02d}" for i in range(1, 13)]  # 1日目から12日目
    
    # 特定の開催回数のみ
    # target_kaisai_list = ["01", "05", "10"]  # 1回、5回、10回開催のみ
    
    # 特定の開催日目のみ
    # target_nichi_list = ["01", "02", "03"]  # 1日目、2日目、3日目のみ
    
    # 従来の単一指定との互換性のため、リスト形式に変換
    # "01", "02", "03", "04", "05", "06"
    target_kaisai_list = ["04", "05", "06"]
    target_nichi_list = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
    
    # 場所ID (01=札幌, 02=函館, 05=東京, 06=中山, 07=中京, 08=京都, 09=阪神)
    target_places = ["05"]
    
    # === 距離と芝・ダートの条件指定 ===
    # 距離条件（メートル単位の文字列リスト）
    # 例: ["1200", "1600", "2000"] で1200m、1600m、2000mのレースのみ収集
    # 空のリストまたはNoneで全距離を対象とする
    distance_conditions = ["1600"]  # 例：1200m、1600m、2000mのみ
    
    # 芝・ダート条件（"芝"または"ダ"のリスト）
    # 例: ["芝"] で芝コースのみ、["ダ"] でダートコースのみ、["芝", "ダ"] で両方
    # 空のリストまたはNoneで全コースを対象とする
    surface_conditions = ["芝"]  # 例：芝コースのみ
    
    # === 設定例（コメントアウトを外して使用） ===
    # 短距離レースのみ（1200m、1400m）
    # distance_conditions = ["1200", "1400"]
    # surface_conditions = ["芝", "ダ"]  # または ["1", "0"]
    
    # 中距離レースのみ（1600m、1800m、2000m）
    # distance_conditions = ["1600", "1800", "2000"]
    # surface_conditions = ["芝"]  # または ["1"]
    
    # 長距離レースのみ（2400m、3000m、3200m）
    # distance_conditions = ["2400", "3000", "3200"]
    # surface_conditions = ["芝", "ダ"]  # または ["1", "0"]
    
    # ダートコースのみ
    # distance_conditions = None  # 全距離
    # surface_conditions = ["ダ"]  # または ["0"]
    
    # 全レース（条件なし）
    # distance_conditions = None
    # surface_conditions = None

    # 条件の表示
    print(f"[INFO] Target years: {target_years}")
    print(f"[INFO] Target kaisai: {target_kaisai_list}")
    print(f"[INFO] Target nichi: {target_nichi_list}")
    print(f"[INFO] Distance conditions: {distance_conditions if distance_conditions else 'All distances'}")
    print(f"[INFO] Surface conditions: {surface_conditions if surface_conditions else 'All surfaces'}")

    all_race_data = [] # すべてのレースデータを一旦メモリに貯める場合 (非推奨：メモリ使用量大)

    # === 複数年度・複数開催回数・複数開催日目に対応したループ処理 ===
    total_years = len(target_years)
    total_kaisai = len(target_kaisai_list)
    total_nichi = len(target_nichi_list)
    
    for year_index, target_year in enumerate(target_years, 1):
        print(f"[INFO] Processing year: {target_year} ({year_index}/{total_years})")
        for kaisai_index, target_kaisai in enumerate(target_kaisai_list, 1):
            print(f"[INFO] Processing kaisai: {target_kaisai} ({kaisai_index}/{total_kaisai})")
            for nichi_index, target_nichi in enumerate(target_nichi_list, 1):
                print(f"[INFO] Processing nichi: {target_nichi} ({nichi_index}/{total_nichi})")
                for place_id_str in target_places:
                    for race_num in range(1, 13): # 1レースから12レースまで
                        race_id = f"{target_year}{place_id_str}{target_kaisai}{target_nichi}{race_num:02d}"
                        print(f"[INFO] Processing race_id: {race_id}")
                        data = get_race_data(race_id)
                        if data:
                            # === 条件フィルタリング ===
                            filtered_data = filter_race_by_conditions(data, distance_conditions, surface_conditions)
                            if filtered_data:
                                append_to_csv(filtered_data, OUTPUT_FILE) # 取得ごとにCSVに追記
                            # all_race_data.extend(data) # メモリに貯める場合
        print(f"[INFO] Completed year: {target_year}")

    # # メモリに貯めたデータを最後に一括で書き込む場合 (メモリ注意)
    # if all_race_data:
    #     append_to_csv(all_race_data, OUTPUT_FILE)

    print(f"[完了] データを {OUTPUT_FILE} に保存しました (または追記しました)。")

if __name__ == "__main__":
    main()