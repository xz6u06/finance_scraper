import json
import time
import os
import logging
from datetime import datetime, timedelta
from typing import List, Tuple
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager

class InvestingCalendarScraper:
    def __init__(self, headless=True):
        """
        初始化 InvestingCalendarScraper。
        包含 Log 設定與 Selenium WebDriver 設定。
        """
        # 1. 設定 Log (這會建立 logs 資料夾並設定雙向輸出)
        self.logger = self._setup_logger()
        
        self.base_url = "https://hk.investing.com/economic-calendar/"
        options = Options()
        if headless:
            options.add_argument("--headless")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-extensions")
        options.add_argument("--page-load-strategy=eager")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36")
        
        self.logger.info(f"啟動瀏覽器 (Headless: {headless})...")
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        self.driver.set_page_load_timeout(30)
        self.wait = WebDriverWait(self.driver, 10)

    def _setup_logger(self):
        """
        設定 Logging 系統
        - 輸出到 Console
        - 輸出到 logs/scraper_YYYYMMDD_HHMMSS.log
        """
        # 取得專案根目錄
        current_script_path = os.path.abspath(__file__)
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_script_path)))
        log_dir = os.path.join(project_root, "logs")

        # 建立 logs 資料夾
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        # 建立 Logger
        logger = logging.getLogger("InvestingScraper")
        logger.setLevel(logging.INFO)
        
        # 清除舊的 handlers (避免重複 print)
        if logger.hasHandlers():
            logger.handlers.clear()

        # Formatter
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        # 1. File Handler (寫入檔案)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(log_dir, f"scraper_{timestamp}.log")
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # 2. Stream Handler (輸出到螢幕)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
        
        # 雖然此時還沒 return，但這行會被寫入檔案
        file_handler.stream.write(f"{datetime.now()} - INFO - Log 系統初始化完成，Log 檔路徑: {log_file}\n")
        
        print(f"Log 將儲存於: {log_file}") # 這是唯一保留的 print，確保使用者第一時間看到路徑
        return logger

    def close(self):
        """關閉 WebDriver"""
        if self.driver:
            self.logger.info("關閉瀏覽器...")
            self.driver.quit()

    def _generate_date_chunks(self, start_date: str, end_date: str, interval_days: int) -> List[Tuple[str, str]]:
        """輔助方法：將大日期範圍切分為多個小區段"""
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        chunks = []
        
        current = start
        while current <= end:
            chunk_end = current + timedelta(days=interval_days - 1)
            if chunk_end > end:
                chunk_end = end
            
            chunks.append((current.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")))
            current = chunk_end + timedelta(days=1)
            
        return chunks

    def run(self, start_date=None, end_date=None, target_countries=None, interval_days=14):
        """主要執行方法 (Master Loop)"""
        if start_date is None:
            start_date = datetime.now().strftime('%Y-%m-%d')
        if end_date is None:
            end_date = (datetime.now() + timedelta(days=14)).strftime('%Y-%m-%d')
        if target_countries is None:
            target_countries = ['歐盟區', '英國', '德國', '美國', '中國', '日本', '南韓', '新加坡', '台灣']

        self.logger.info(f"=== 啟動總任務: {start_date} 至 {end_date} (每 {interval_days} 天切分) ===")
        
        generated_files = []
        date_chunks = self._generate_date_chunks(start_date, end_date, interval_days)

        try:
            try:
                self.driver.get(self.base_url)
            except TimeoutException:
                self.logger.warning("頁面載入超時，嘗試繼續執行...")
                self.driver.execute_script("window.stop();")

            self._handle_popup()
            self._apply_country_filters(target_countries)

            for idx, (s_date, e_date) in enumerate(date_chunks):
                self.logger.info(f"--- 執行第 {idx+1}/{len(date_chunks)} 批次: {s_date} ~ {e_date} ---")
                
                filename = self._scrape_single_range(s_date, e_date)
                if filename:
                    generated_files.append(filename)
                
                time.sleep(2)

            msg = f"總任務完成，共產生 {len(generated_files)} 個檔案。"
            self.logger.info(msg)
            return generated_files, msg

        except Exception as e:
            self.logger.error(f"總任務執行失敗: {e}", exc_info=True)
            raise e
        finally:
            self.close()

    def _scrape_single_range(self, start_date, end_date):
        """執行單一日期區段的爬取"""
        try:
            self._apply_date_filters(start_date, end_date)
            self._scroll_to_load(end_date)
            data = self._parse_data(start_date, end_date)
            
            s_date_str = start_date.replace('/', '-')
            e_date_str = end_date.replace('/', '-')
            
            current_script_path = os.path.abspath(__file__)
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_script_path)))
            output_dir = os.path.join(project_root, "output", "invest_EC")

            if not os.path.exists(output_dir):
                os.makedirs(output_dir)

            filename = os.path.join(output_dir, f"{s_date_str}_{e_date_str}_Ecalendar.json")

            final_output = {
                "meta": {
                    "crawled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "filter_start": start_date,
                    "filter_end": end_date,
                    "count": len(data)
                },
                "data": data
            }
            
            self._save_data(final_output, filename)
            self.logger.info(f"批次存檔成功: {filename} (資料筆數: {len(data)})")
            return filename

        except Exception as e:
            self.logger.error(f"批次 {start_date}~{end_date} 失敗: {e}")
            return None

    def _handle_popup(self):
        """處理彈窗"""
        try:
            # 這裡改成 logger.debug 避免洗版，或是 info 也可以
            # self.logger.info("檢查是否有彈窗...")
            popup = WebDriverWait(self.driver, 3).until(
                EC.visibility_of_element_located((By.ID, "PromoteSignUpPopUp"))
            )
            close_btn = popup.find_element(By.CSS_SELECTOR, ".popupCloseIcon.largeBannerCloser")
            close_btn.click()
            self.logger.info("偵測到彈窗並已關閉")
        except:
            pass 

    def _apply_country_filters(self, target_countries):
        """設定國家篩選"""
        self.logger.info(f"設定國家篩選: {target_countries}")
        try:
            filter_btn = self.wait.until(EC.presence_of_element_located((By.ID, "filterStateAnchor")))
            self.driver.execute_script("arguments[0].click();", filter_btn)
            time.sleep(1)

            try:
                restore_btn = self.driver.find_element(By.ID, "filterRestoreDefaults")
                if restore_btn.is_displayed():
                    restore_btn.click()
                    time.sleep(1)
            except:
                pass
            
            all_checked = self.driver.find_elements(By.CSS_SELECTOR, "ul.countryOption input:checked")
            for inp in all_checked:
                try:
                    self.driver.execute_script("arguments[0].click();", inp)
                except:
                    pass
            
            country_labels = self.driver.find_elements(By.CSS_SELECTOR, "ul.countryOption li label")
            for label in country_labels:
                country_name = label.text.strip()
                if any(target in country_name for target in target_countries):
                    input_id = label.get_attribute("for")
                    if input_id:
                        input_box = self.driver.find_element(By.ID, input_id)
                        if not input_box.is_selected():
                            self.driver.execute_script("arguments[0].click();", input_box)
            
            submit_btn = self.driver.find_element(By.ID, "ecSubmitButton")
            submit_btn.click()
            time.sleep(2)
            
        except Exception as e:
            self.logger.warning(f"國家篩選設定警告: {e}")

    def _apply_date_filters(self, start_date, end_date):
        """設定日期篩選"""
        try:
            self.logger.info(f"套用日期: {start_date} - {end_date}")
            self.driver.execute_script("calendarFilters.datePickerFilter(arguments[0], arguments[1]);", start_date, end_date)
            time.sleep(3)
            self._handle_popup()
            
        except Exception as e:
            self.logger.error(f"日期篩選失敗: {e}")
            raise

    def _scroll_to_load(self, end_date_str):
        """捲動載入"""
        self.logger.info("捲動載入資料中...")
        try:
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            
            max_scroll_attempts = 30
            attempts = 0

            while attempts < max_scroll_attempts:
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                
                try:
                    date_rows = self.driver.find_elements(By.CSS_SELECTOR, "#economicCalendarData tr.theDay")
                    if date_rows:
                        last_date_text = date_rows[-1].text
                        clean_date_text = last_date_text.split(' ')[0]
                        try:
                            # 支援中文日期格式 "YYYY年MM月DD日"
                            current_loaded_date = datetime.strptime(clean_date_text, '%Y年%m月%d日')
                            if current_loaded_date >= end_date:
                                self.logger.info(f"已載入至 {clean_date_text}，停止捲動。")
                                break
                        except ValueError:
                            pass
                except:
                    pass

                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
                attempts += 1
        except Exception as e:
            self.logger.warning(f"捲動時發生非致命錯誤: {e}")

    def _parse_data(self, start_date_str, end_date_str):
        """解析 HTML"""
        self.logger.info("解析 HTML 資料...")
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        table = soup.find('table', id='economicCalendarData')
        if not table:
            self.logger.warning("未找到資料表格 #economicCalendarData")
            return []
            
        results = []
        rows = table.find('tbody').find_all('tr')
        current_date = None
        start_date_obj = datetime.strptime(start_date_str, '%Y-%m-%d')
        end_date_obj = datetime.strptime(end_date_str, '%Y-%m-%d')

        for row in rows:
            date_cell = row.find('td', class_='theDay')
            if date_cell:
                try:
                    parts = date_cell.text.strip().split()
                    if parts:
                        current_date = datetime.strptime(parts[0], '%Y年%m月%d日')
                except:
                    current_date = None
                continue
            
            if 'js-event-item' in row.get('class', []):
                if current_date and start_date_obj <= current_date <= end_date_obj:
                    try:
                        time_str = row.find('td', class_='time').text.strip()
                        currency = row.find('td', class_='flagCur').text.strip()
                        
                        country_elem = row.find('td', class_='flagCur').find('span')
                        country = country_elem.get('title') if country_elem else ""
                        
                        sentiment_td = row.find('td', class_='sentiment')
                        importance = len(sentiment_td.find_all('i', class_='grayFullBullishIcon')) if sentiment_td else 0

                        event = row.find('td', class_='event').text.strip()
                        actual = row.find('td', class_='act').text.strip()
                        forecast = row.find('td', class_='fore').text.strip()
                        previous = row.find('td', class_='prev').text.strip()
                        
                        timestamp = f"{current_date.strftime('%Y-%m-%d')} {time_str}"

                        item = {
                            "date": current_date.strftime('%Y-%m-%d'),
                            "time": time_str,
                            "timestamp": timestamp,
                            "country": country,
                            "currency": currency,
                            "importance": importance,
                            "event": event,
                            "actual": actual,
                            "forecast": forecast,
                            "previous": previous
                        }
                        results.append(item)
                    except Exception:
                        continue
        return results

    def _save_data(self, data, filename):
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

if __name__ == "__main__":
    # 使用 headless=False 來進行有畫面的測試
    # 如果要完全背景執行，請改為 True
    scraper = InvestingCalendarScraper(headless=False)
    
    try:
        # 測試：爬取 2025 全年資料
        scraper.run(
            start_date="2000-01-01", 
            end_date="2014-12-31", 
            interval_days=14,
            # target_countries=['美國', '台灣', '中國', '歐盟區', '日本']
        )
    except Exception as e:
        # 這裡的 print 是最後一道防線，如果 logger 初始化失敗至少看得到錯誤
        print(f"測試執行失敗: {e}")