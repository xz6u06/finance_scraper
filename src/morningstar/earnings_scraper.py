"""
Morningstar 財報會議紀錄爬蟲 (Linux/WSL/Windows 通用版)
===========================
從 Morningstar 網站爬取公司的財報會議紀錄（Earnings Transcripts）
並支援寫入 PostgreSQL 資料庫
"""

import csv
import json
import time
import random
import re
import os
import sys
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# [新增] 引入 webdriver_manager 自動管理驅動
from webdriver_manager.chrome import ChromeDriverManager

# [新增] 引入 psycopg2 用於連接 PostgreSQL
import psycopg2


class EarningsScraper:
    """財報會議紀錄爬蟲類別"""

    def __init__(self):
        """
        初始化爬蟲
        """
        self.results = []
        self.driver = None
        self.db_conn = None

        # [新增] 初始化資料庫連接
        self.init_db()

    def init_db(self):
        """初始化 PostgreSQL 資料庫連接並建立資料表 (Schema: morningstar)"""
        try:
            # 從環境變數讀取連線資訊
            db_host = os.getenv("DB_POSTGRESDB_HOST", "postgres")
            db_name = os.getenv("DB_POSTGRESDB_DATABASE", "n8n")
            db_user = os.getenv("DB_POSTGRESDB_USER", "n8n")
            db_pass = os.getenv("DB_POSTGRESDB_PASSWORD", "n8n")
            db_port = os.getenv("DB_POSTGRESDB_PORT", "5432")

            print(f" 🐘 正在連接 PostgreSQL ({db_host}:{db_port})...")

            self.db_conn = psycopg2.connect(
                host=db_host,
                database=db_name,
                user=db_user,
                password=db_pass,
                port=db_port,
            )
            self.db_conn.autocommit = True

            with self.db_conn.cursor() as cursor:
                # [新增] 1. 建立 Schema (如果不存在)
                cursor.execute("CREATE SCHEMA IF NOT EXISTS morningstar;")

                # [修改] 2. 建立資料表 (加上 schema 前綴)
                create_table_query = """
                CREATE TABLE IF NOT EXISTS morningstar.earnings_transcripts (
                    id SERIAL PRIMARY KEY,
                    ticker VARCHAR(50) NOT NULL,
                    company_name VARCHAR(255),
                    quarter VARCHAR(50) NOT NULL,
                    transcript TEXT,
                    date DATE,
                    url TEXT,
                    scraped_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(ticker, quarter)
                );
                """
                cursor.execute(create_table_query)
                print(" ✅ 資料庫資料表檢查完成 (Schema: morningstar)")

        except Exception as e:
            print(f" ⚠️ 資料庫連接失敗 (將只儲存 JSON): {e}")
            self.db_conn = None

    @staticmethod
    def get_quarter_from_date(date_string):
        """
        根據日期字串判斷季度

        Args:
            date_string: 日期字串（任何格式）

        Returns:
            str: 季度標籤（格式：YYYY_Q#）或 "Unknown"
        """
        if not date_string or date_string == "Unknown":
            return "Unknown"

        try:
            # 先格式化日期
            formatted_date = EarningsScraper.format_date(date_string)

            if formatted_date == "Unknown" or "/" not in formatted_date:
                return "Unknown"

            # 解析 YYYY/MM/DD 格式
            parts = formatted_date.split("/")
            if len(parts) != 3:
                return "Unknown"

            year = parts[0]
            month = int(parts[1])

            # 根據月份判斷季度
            if 1 <= month <= 3:
                quarter = "Q1"
            elif 4 <= month <= 6:
                quarter = "Q2"
            elif 7 <= month <= 9:
                quarter = "Q3"
            elif 10 <= month <= 12:
                quarter = "Q4"
            else:
                return "Unknown"

            return f"{year}_{quarter}"

        except Exception as e:
            print(f"    ⚠️ 判斷季度時發生錯誤: {e}")
            return "Unknown"

    @staticmethod
    def format_date(date_string):
        """
        將日期字串格式化為 YYYY/MM/DD 格式

        Args:
            date_string: 原始日期字串

        Returns:
            str: 格式化後的日期字串 (YYYY/MM/DD) 或原始字串
        """
        if not date_string or date_string == "Unknown":
            return "Unknown"

        # 移除多餘的空白
        date_string = date_string.strip()

        # 月份名稱對應
        month_map = {
            "jan": "01",
            "january": "01",
            "feb": "02",
            "february": "02",
            "mar": "03",
            "march": "03",
            "apr": "04",
            "april": "04",
            "may": "05",
            "jun": "06",
            "june": "06",
            "jul": "07",
            "july": "07",
            "aug": "08",
            "august": "08",
            "sep": "09",
            "september": "09",
            "oct": "10",
            "october": "10",
            "nov": "11",
            "november": "11",
            "dec": "12",
            "december": "12",
        }

        # 嘗試直接用正則替換 (YYYY-MM-DD 或 YYYY/MM/DD)
        iso_match = re.match(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})", date_string)
        if iso_match:
            year, month, day = iso_match.groups()
            return f"{year}/{month.zfill(2)}/{day.zfill(2)}"

        # 嘗試解析美式格式 (Jan 31, 2024)
        us_match = re.search(
            r"(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+(\d{1,2}),?\s+(\d{4})",
            date_string,
            re.IGNORECASE,
        )
        if us_match:
            month_name, day, year = us_match.groups()
            month_num = month_map.get(month_name.lower(), "00")
            return f"{year}/{month_num}/{day.zfill(2)}"

        # 嘗試解析美式簡短格式 (MM/DD/YYYY)
        us_short_match = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", date_string)
        if us_short_match:
            month, day, year = us_short_match.groups()
            return f"{year}/{month.zfill(2)}/{day.zfill(2)}"

        # 嘗試解析歐式格式 (31 Jan 2024)
        eu_match = re.search(
            r"(\d{1,2})[-\s](Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)[-\s](\d{4})",
            date_string,
            re.IGNORECASE,
        )
        if eu_match:
            day, month_name, year = eu_match.groups()
            month_num = month_map.get(month_name.lower(), "00")
            return f"{year}/{month_num}/{day.zfill(2)}"

        # 如果都無法解析，嘗試使用 datetime.strptime
        try:
            # 嘗試多種格式
            for fmt in [
                "%Y-%m-%d",
                "%Y/%m/%d",
                "%d-%m-%Y",
                "%d/%m/%Y",
                "%m-%d-%Y",
                "%m/%d/%Y",
                "%B %d, %Y",
                "%b %d, %Y",
                "%d %B %Y",
                "%d %b %Y",
            ]:
                try:
                    dt = datetime.strptime(date_string, fmt)
                    return dt.strftime("%Y/%m/%d")
                except ValueError:
                    continue
        except:
            pass

        # 如果所有方法都失敗，返回原始字串
        return date_string

    def setup_driver(self):
        """設定 Chrome WebDriver (自動適配 Windows/Linux/WSL)"""
        options = webdriver.ChromeOptions()

        # 設定 User-Agent 模擬真實瀏覽器
        options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

        # [重要] Linux/WSL 環境設定
        # 在伺服器或 WSL 環境下，通常必須使用 headless 模式
        if sys.platform.startswith("linux"):
            print(" 🐧 偵測到 Linux 環境，啟用 Headless 模式與 Sandbox 修補")
            options.add_argument("--headless=new")  # 新版無頭模式
            options.add_argument("--no-sandbox")  # 解決權限問題
            options.add_argument("--disable-dev-shm-usage")  # 解決記憶體共享問題
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1920,1080")  # 設定視窗大小以確保元素渲染
        else:
            # Windows 開發階段：可選擇是否開啟 headless
            # options.add_argument('--headless')
            pass

        options.add_argument("--disable-blink-features=AutomationControlled")

        try:
            # [修改] 使用 ChromeDriverManager 自動安裝與載入驅動
            driver_path = ChromeDriverManager().install()
            print(f" 🔧 WebDriver 路徑: {driver_path}")

            service = Service(driver_path)
            self.driver = webdriver.Chrome(service=service, options=options)

            if not sys.platform.startswith("linux"):
                self.driver.maximize_window()

        except Exception as e:
            print(f"❌ WebDriver 初始化失敗: {e}")
            print("💡 提示: 請確認系統已安裝 Google Chrome 瀏覽器")
            if sys.platform.startswith("linux"):
                print(
                    "   Linux 安裝指令: wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && sudo dpkg -i google-chrome-stable_current_amd64.deb && sudo apt-get install -f -y"
                )
            raise e

    def read_urls_from_csv(self, csv_path):
        """
        從 CSV 檔案讀取網址列表
        """
        urls = []
        if not os.path.exists(csv_path):
            print(f"❌ 找不到 CSV 檔案: {csv_path}")
            return []

        try:
            with open(csv_path, "r", encoding="utf-8") as file:
                reader = csv.reader(file)
                for row in reader:
                    if row:  # 確保不是空行
                        urls.append(row[0].strip())
            print(f"✅ 成功讀取 {len(urls)} 個網址")
            return urls
        except Exception as e:
            print(f"❌ 讀取 CSV 檔案失敗: {e}")
            return []

    def debug_page_buttons(self):
        """調試方法：顯示頁面上所有的按鈕和可點擊元素"""
        try:
            print("\n  🔍 === 調試：頁面元素分析 ===")

            # 尋找所有按鈕
            buttons = self.driver.find_elements(By.TAG_NAME, "button")
            print(f"  📌 找到 {len(buttons)} 個 button 元素:")
            for i, btn in enumerate(buttons[:5], 1):  # 只顯示前5個避免洗版
                text = btn.text.strip() or "(無文字)"
                classes = btn.get_attribute("class") or "(無class)"
                print(f"     {i}. 文字: '{text}' | class: '{classes}'")

            # 尋找所有日期選項 labels
            date_labels = self.driver.find_elements(By.CSS_SELECTOR, "label.mds-radio-button__sal")
            print(f"  📌 找到 {len(date_labels)} 個日期選項 labels")

            print("  🔍 === 調試結束 ===\n")
        except Exception as e:
            print(f"  ⚠️ 調試時發生錯誤: {e}")

    def collect_all_transcripts_by_clicking_dates(self, company_name, ticker, url):
        """逐一點擊所有日期選項並直接提取逐字稿內容"""
        all_transcripts = []

        try:
            print("  📅 尋找並打開日期選擇選單...")

            # 嘗試點擊"發表日期"按鈕
            date_button_selectors = [
                "//button[contains(., 'Published Date')]",
                "//span[contains(text(), 'Published Date')]/..",
                "button[class*='event']",
                "#eventPopver-transcript",
            ]

            for selector in date_button_selectors:
                try:
                    if selector.startswith("//"):
                        btn = self.driver.find_element(By.XPATH, selector)
                    else:
                        btn = self.driver.find_element(By.CSS_SELECTOR, selector)

                    self.driver.execute_script("arguments[0].click();", btn)
                    time.sleep(2)
                    break
                except:
                    continue

            # 尋找所有日期選項
            label_selectors = [
                "label.mds-radio-button__sal",
                'label[class*="mds-radio-button"]',
            ]

            date_labels = []
            for selector in label_selectors:
                try:
                    date_labels = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if date_labels:
                        print(f"  ✅ 找到 {len(date_labels)} 個日期選項")
                        break
                except:
                    continue

            if not date_labels:
                print("  ⚠️ 未找到多個日期選項，抓取當前頁面")
                transcript_data = self.extract_transcript_data(url, company_name, ticker, "TBD")
                if transcript_data:
                    all_transcripts.append(transcript_data)
                return all_transcripts

            print(f"  🔄 將逐一點擊 {len(date_labels)} 個日期選項...")

            # 逐一點擊每個 label
            for idx, label in enumerate(date_labels, 1):
                try:
                    # 為了避免元素過期 (StaleElement)，每次重新抓取列表
                    current_labels = self.driver.find_elements(
                        By.CSS_SELECTOR, "label.mds-radio-button__sal"
                    )
                    if idx - 1 < len(current_labels):
                        current_label = current_labels[idx - 1]

                        # 使用 JS 點擊最穩定
                        self.driver.execute_script("arguments[0].click();", current_label)
                        print(f"    [{idx}] 點擊日期選項...")

                        time.sleep(3)  # 等待渲染

                        transcript_data = self.extract_transcript_data(
                            url, company_name, ticker, "TBD"
                        )
                        if transcript_data:
                            all_transcripts.append(transcript_data)
                            print(
                                f"    ✅ 提取成功 (長度: {len(transcript_data.get('transcript', ''))})"
                            )
                except Exception as e:
                    print(f"    ⚠️ 處理第 {idx} 個日期時錯誤: {e}")
                    continue

            return all_transcripts

        except Exception as e:
            print(f"  ❌ 收集逐字稿時發生錯誤: {e}")
            transcript_data = self.extract_transcript_data(url, company_name, ticker, "TBD")
            if transcript_data:
                all_transcripts.append(transcript_data)
            return all_transcripts

    def scrape_transcript_page(self, url, output_dir):
        """爬取單一財報會議紀錄頁面"""
        page_results = []

        try:
            print(f"\n🌐 正在訪問: {url}")
            self.driver.get(url)

            wait = WebDriverWait(self.driver, 20)
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.company-name")))
            time.sleep(3)

            # 提取基本資訊
            try:
                company_name = self.driver.find_element(
                    By.CSS_SELECTOR, "div.company-name"
                ).text.strip()
            except:
                company_name = "Unknown"

            try:
                ticker = self.driver.find_element(By.CSS_SELECTOR, "span.ticker").text.strip()
            except:
                ticker = "Unknown"

            print(f"📊 公司: {company_name} ({ticker})")

            if "/earnings-transcripts" in url or "/transcript" in url:
                print("\n  💡 檢測到逐字稿頁面，開始處理...")
                page_results = self.collect_all_transcripts_by_clicking_dates(
                    company_name, ticker, url
                )
            else:
                print("  ⚠️ 非逐字稿頁面")
                return page_results

            # 分類並儲存 (改為同步寫入 DB)
            print(f"\n📅 正在分類並儲存資料...")
            self.classify_and_save_by_quarter(page_results, ticker, company_name, output_dir)

            print(f"✅ 完成 {url}")

        except TimeoutException:
            print(f"❌ 頁面載入逾時: {url}")
        except Exception as e:
            print(f"❌ 爬取頁面時發生錯誤: {e}")

        return page_results

    def extract_transcript_data(
        self, transcript_url, company_name, ticker, quarter_label="Unknown"
    ):
        """提取逐字稿資料"""
        try:
            # 確保元素存在
            try:
                transcript_element = self.driver.find_element(By.CSS_SELECTOR, "div.transcript")
                transcript_text = transcript_element.text.strip()
            except NoSuchElementException:
                return None

            # 提取日期
            date_text = "Unknown"
            date_selectors = [
                "#eventPopver-transcript > span:nth-child(2)",
                "span.date",
                "time",
            ]
            for selector in date_selectors:
                try:
                    date_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    date_text = date_element.text.strip()
                    if date_text:
                        break
                except:
                    continue

            formatted_date = self.format_date(date_text)

            data = {
                "company_name": company_name,
                "ticker": ticker,
                "quarter": quarter_label,
                "transcript": transcript_text,
                "date": formatted_date,
                "url": transcript_url,
                "scraped_at": datetime.now().strftime("%Y/%m/%d %H:%M:%S"),
            }
            return data

        except Exception as e:
            print(f"    ❌ 提取失敗: {e}")
            return None

    def scrape_all(self, csv_path, output_dir="output"):
        """執行完整爬蟲流程"""
        print("=" * 60)
        print("🚀 開始執行 Morningstar 財報會議紀錄爬蟲 (DB Ready)")
        print("=" * 60)

        urls = self.read_urls_from_csv(csv_path)
        if not urls:
            print("❌ 沒有網址，程式結束")
            return

        print("\n🔧 正在初始化 WebDriver...")
        self.setup_driver()

        try:
            for idx, url in enumerate(urls, 1):
                print(f"\n📍 進度: [{idx}/{len(urls)}]")
                page_results = self.scrape_transcript_page(url, output_dir)
                self.results.extend(page_results)

                if idx < len(urls):
                    delay = random.uniform(3, 5)
                    print(f"\n⏳ 休眠 {delay:.1f} 秒...")
                    time.sleep(delay)

            self.show_final_stats()

        except KeyboardInterrupt:
            print("\n\n⚠️ 使用者中斷程式")
            self.show_final_stats()
        except Exception as e:
            print(f"\n❌ 發生錯誤: {e}")
        finally:
            if self.driver:
                print("\n🔧 關閉瀏覽器...")
                self.driver.quit()
            if self.db_conn:
                print("🐘 關閉資料庫連線...")
                self.db_conn.close()

    def classify_and_save_by_quarter(self, all_data, ticker, company_name, output_dir):
        """分類並儲存 (JSON + DB)"""
        if not all_data:
            return

        quarters_data = {}
        for record in all_data:
            date_str = record.get("date", "Unknown")
            quarter_label = self.get_quarter_from_date(date_str)
            record["quarter"] = quarter_label

            if quarter_label not in quarters_data:
                quarters_data[quarter_label] = []
            quarters_data[quarter_label].append(record)

        for quarter_label, quarter_records in quarters_data.items():
            self.save_quarter_results(
                quarter_records, ticker, company_name, quarter_label, output_dir
            )

    def save_quarter_results(self, quarter_data, ticker, company_name, quarter_label, output_dir):
        """儲存 JSON 並寫入資料庫"""
        if not quarter_data:
            return

        # 1. 儲存為 JSON (保留原功能作為備份)
        try:
            os.makedirs(output_dir, exist_ok=True)
            safe_ticker = re.sub(r"[^\w\-]", "_", ticker)
            safe_quarter = re.sub(r"[^\w\-]", "_", quarter_label)
            filename = f"{safe_ticker}_{safe_quarter}.json"
            output_path = os.path.join(output_dir, filename)

            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(quarter_data, f, ensure_ascii=False, indent=2)

            print(f"  💾 JSON 已儲存: {filename}")
        except Exception as e:
            print(f"  ❌ JSON 存檔失敗: {e}")

        # 2. 寫入 Postgres 資料庫
        if self.db_conn:
            try:
                with self.db_conn.cursor() as cursor:
                    for record in quarter_data:
                        # 處理日期格式以符合 SQL DATE (YYYY-MM-DD)
                        sql_date = (
                            record["date"].replace("/", "-")
                            if record["date"] != "Unknown"
                            else None
                        )

                        insert_query = """
                        INSERT INTO morningstar.earnings_transcripts 
                        (ticker, company_name, quarter, transcript, date, url, scraped_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (ticker, quarter) 
                        DO UPDATE SET 
                            transcript = EXCLUDED.transcript,
                            scraped_at = EXCLUDED.scraped_at,
                            url = EXCLUDED.url;
                        """
                        cursor.execute(
                            insert_query,
                            (
                                record["ticker"],
                                record["company_name"],
                                record["quarter"],
                                record["transcript"],
                                sql_date,
                                record["url"],
                                datetime.now(),
                            ),
                        )
                print(f"  🐘 DB 已寫入/更新: {len(quarter_data)} 筆資料")
            except Exception as e:
                print(f"  ❌ DB 寫入失敗: {e}")

    def show_final_stats(self):
        """顯示統計"""
        if not self.results:
            print("\n📊 無資料")
            return
        print(f"\n📊 總計爬取: {len(self.results)} 筆資料")


def main():
    """主程式"""
    # 設定路徑
    BASE_DIR = os.getcwd()

    # [修改] 1. 改讀取 input 資料夾，並更新檔名
    # 原本: os.path.join(BASE_DIR, "source", "weburl.csv")
    CSV_PATH = os.path.join(BASE_DIR, "input", "morningstar_ET_urls.csv")

    # [修改] 2. 更新輸出路徑到子資料夾 morningstar_ET
    # 原本: os.path.join(BASE_DIR, "output")
    OUTPUT_DIR = os.path.join(BASE_DIR, "output", "morningstar_ET")

    # 建立爬蟲實例
    scraper = EarningsScraper()
    scraper.scrape_all(CSV_PATH, OUTPUT_DIR)


if __name__ == "__main__":
    main()
