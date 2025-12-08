
import json
import time
import os
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException
from webdriver_manager.chrome import ChromeDriverManager

class InvestingCalendarScraper:
    def __init__(self, headless=True):
        """
        初始化 InvestingCalendarScraper。
        :param headless: 是否使用無頭模式。
        """
        self.base_url = "https://hk.investing.com/economic-calendar/"
        options = Options()
        if headless:
            options.add_argument("--headless")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage") # 解決資源限制問題
        options.add_argument("--disable-extensions")
        options.add_argument("--page-load-strategy=eager") # 不等待所有資源載入，加速
        # 設定 User-Agent 以避免被輕易偵測
        options.add_argument("user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36")
        
        # 使用 webdriver_manager 安裝並啟動 ChromeDriver
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        self.driver.set_page_load_timeout(30) # 設定頁面載入超時
        self.wait = WebDriverWait(self.driver, 10)

    def close(self):
        """
        關閉 WebDriver。
        """
        if self.driver:
            self.driver.quit()

    def run(self, start_date=None, end_date=None, target_countries=None):
        """
        主要執行方法。
        :param start_date: 字串 'YYYY/MM/DD' (預設值: 今天)
        :param end_date: 字串 'YYYY/MM/DD' (預設值: 今天 + 14 天)
        :param target_countries: 字串列表
        :return: 儲存檔案名稱
        """
        if start_date is None:
            start_date = datetime.now().strftime('%Y-%m-%d')
        if end_date is None:
            end_date = (datetime.now() + timedelta(days=14)).strftime('%Y-%m-%d')
        if target_countries is None:
            target_countries = ['歐盟區', '英國', '德國', '美國', '中國', '日本', '南韓', '新加坡', '台灣']

        print(f"開始爬取任務: {start_date} 至 {end_date}")
        
        try:
            try:
                self.driver.get(self.base_url)
            except TimeoutException:
                print("頁面載入超時，嘗試繼續執行...")
                self.driver.execute_script("window.stop();")

            self._handle_popup()
            self._apply_filters(start_date, end_date, target_countries)
            self._scroll_to_load(end_date)
            data = self._parse_data(start_date, end_date)
            
            # 轉換檔名格式 YYYY-MM-DD
            s_date_str = start_date.replace('/', '-')
            e_date_str = end_date.replace('/', '-')
            
            output_dir = "output"
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
                
            filename = os.path.join(output_dir, f"{s_date_str}-{e_date_str}_Ecalendar.json")
            
            # 建構包含 metadata 的最終輸出
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
            print(f"任務完成，資料已儲存至 {filename}")
            return filename, final_output
            
        except Exception as e:
            print(f"執行過程中發生錯誤: {e}")
            raise
        finally:
            self.close()

    def _handle_popup(self):
        """
        處理可能出現的蓋版彈窗 (PromoteSignUpPopUp)。
        """
        try:
            # 等待彈窗出現，最多 5 秒，因為不一定會出現
            print("檢查是否有彈窗...")
            popup = WebDriverWait(self.driver, 5).until(
                EC.visibility_of_element_located((By.ID, "PromoteSignUpPopUp"))
            )
            print("偵測到彈窗，嘗試關閉...")
            close_btn = popup.find_element(By.CSS_SELECTOR, ".popupCloseIcon.largeBannerCloser")
            close_btn.click()
            # 等待彈窗消失
            WebDriverWait(self.driver, 5).until(
                EC.invisibility_of_element_located((By.ID, "PromoteSignUpPopUp"))
            )
            print("彈窗已關閉")
        except TimeoutException:
            print("未偵測到彈窗或彈窗未在時限內出現")
        except Exception as e:
            print(f"處理彈窗時發生非預期錯誤: {e}")
            # 不讓彈窗錯誤中斷主流程

    def _apply_filters(self, start_date, end_date, target_countries):
        """
        應用國家與日期篩選。
        """
        print("開始設定篩選條件...")
        
        # 1. 國家篩選
        try:
            filter_btn = self.wait.until(EC.presence_of_element_located((By.ID, "filterStateAnchor")))
            self.driver.execute_script("arguments[0].click();", filter_btn)
            time.sleep(1) # 等待動畫

            # 重置預設值 (取消所有勾選或恢復預設再取消)
            # 這裡策略是先點擊 "恢復默認" 然後手動處理，或者直接遍歷取消。
            # 為了保險，先嘗試點擊恢復默認 (如果有此按鈕且可見)
            try:
                restore_btn = self.driver.find_element(By.ID, "filterRestoreDefaults")
                if restore_btn.is_displayed():
                    restore_btn.click()
                    time.sleep(1)
            except:
                pass
            
            # 使用 target_countries 去勾選需要的，其他的取消勾選
            all_checked = self.driver.find_elements(By.CSS_SELECTOR, "ul.countryOption input:checked")
            for inp in all_checked:
                try:
                    self.driver.execute_script("arguments[0].click();", inp)
                except:
                    pass
            
            # 勾選目標國家
            country_labels = self.driver.find_elements(By.CSS_SELECTOR, "ul.countryOption li label")
            for label in country_labels:
                country_name = label.text.strip()
                if any(target in country_name for target in target_countries):
                    input_id = label.get_attribute("for")
                    if input_id:
                        input_box = self.driver.find_element(By.ID, input_id)
                        if not input_box.is_selected():
                            self.driver.execute_script("arguments[0].click();", input_box)
            
            # 提交國家篩選
            submit_btn = self.driver.find_element(By.ID, "ecSubmitButton")
            submit_btn.click()
            
            # 等待頁面刷新或遮罩消失
            time.sleep(2) 
            
        except Exception as e:
            print(f"國家篩選設定失敗: {e}")
            raise

        # 2. 日期篩選
        try:
            print(f"使用 JS 直接套用日期: {start_date} - {end_date}")
            self.driver.execute_script("calendarFilters.datePickerFilter(arguments[0], arguments[1]);", start_date, end_date)
            # 等待加載
            print("等待日期篩選套用(JS)...")
            time.sleep(5)
            
        except Exception as e:
            print(f"日期篩選設定失敗: {e}")
            raise
            
        except Exception as e:
            print(f"日期篩選設定失敗: {e}")
            raise

    def _scroll_to_load(self, end_date_str):
        """
        無限捲動直到加載到結束日期的資料。
        """
        print("開始捲動載入資料...")
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        
        while True:
            # 捲動到底部
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2) # 等待資源加載
            
            # 檢查最新的日期
            # 這裡我們解析最後一個顯示的日期行
            try:
                date_rows = self.driver.find_elements(By.CSS_SELECTOR, "#economicCalendarData tr.theDay")
                if date_rows:
                    last_date_text = date_rows[-1].text
                    # 格式可能是 "2025年12月21日 星期日"
                    # 嘗試簡化解析
                    # 移除 " 星期X"
                    clean_date_text = last_date_text.split(' ')[0]
                    # 解析中文日期
                    # 假設格式: YYYY年MM月DD日
                    current_loaded_date = datetime.strptime(clean_date_text, '%Y年%m月%d日')
                    
                    if current_loaded_date >= end_date:
                        print(f"已載入至 {clean_date_text}，滿足結束日期需求。")
                        break
            except Exception as e:
                # 解析日期失敗可能是因為格式不同或尚未載入，繼續捲動嘗試
                print(f"檢查日期時發生警告 (可能無礙): {e}")

            # 檢查是否無法再捲動
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                print("頁面已到底，停止捲動。")
                break
            last_height = new_height

    def _parse_data(self, start_date_str, end_date_str):
        """
        解析 HTML 表格資料。
        """
        print("開始解析 HTML...")
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        table = soup.find('table', id='economicCalendarData')
        
        if not table:
            print("未找到資料表格 #economicCalendarData")
            return []

        results = []
        rows = table.find('tbody').find_all('tr')
        
        current_date = None
        current_year = datetime.now().year # 預設年份，以防萬一
        
        # 轉換輸入日期字串為 datetime 物件以便比較
        start_date_obj = datetime.strptime(start_date_str, '%Y-%m-%d')
        end_date_obj = datetime.strptime(end_date_str, '%Y-%m-%d')

        for row in rows:
            # 檢查是否為日期列
            # 注意: 實際 HTML 中，class="theDay" 是在 td 上，而不是 tr 上
            date_cell = row.find('td', class_='theDay')
            if date_cell:
                date_text = date_cell.text.strip()
                try:
                    # 格式: "2025年12月7日 星期日"
                    # 使用 split 並過濾空字串
                    parts = date_text.split()
                    if parts:
                        clean_date_text = parts[0]
                        current_date = datetime.strptime(clean_date_text, '%Y年%m月%d日')
                except ValueError:
                    print(f"日期解析失敗: {date_text}")
                    current_date = None
                continue
            
            # 檢查是否為事件列
            if 'js-event-item' in row.get('class', []):
                if current_date is None:
                    continue
                
                # 過濾日期範圍 (因為網頁可能會載入稍微超出範圍的資料)
                if not (start_date_obj <= current_date <= end_date_obj):
                    continue

                try:
                    time_str = row.find('td', class_='time').text.strip()
                    currency = row.find('td', class_='flagCur').text.strip()
                    
                    country_elem = row.find('td', class_='flagCur').find('span')
                    country = country_elem.get('title') if country_elem else ""
                    
                    sentiment_td = row.find('td', class_='sentiment')
                    importance = len(sentiment_td.find_all('i', class_='grayFullBullishIcon')) if sentiment_td else 0
                    
                    event_elem = row.find('td', class_='event').find('a')
                    event = event_elem.text.strip() if event_elem else row.find('td', class_='event').text.strip()
                    
                    actual = row.find('td', class_='act').text.strip()
                    forecast = row.find('td', class_='fore').text.strip()
                    previous = row.find('td', class_='prev').text.strip()
                    
                    # 組合完整時間
                    # 注意: time_str 可能是 "全天" 或具體時間 "15:30"
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
                    
                except AttributeError as e:
                    # 某些欄位可能缺失，略過該行或記錄錯誤
                    continue

        print(f"解析完成，共提取 {len(results)} 筆資料。")
        return results

    def _save_data(self, data, filename):
        """
        儲存資料為 JSON。
        """
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

if __name__ == "__main__":
    # 測試執行
    scraper = InvestingCalendarScraper(headless=True) # 使用 True 進行無頭測試
    
    # 設定測試日期，例如未來一週
    # 注意: 為了測試，這裡使用預設值
    try:
        scraper.run()
    except Exception as e:
        print(f"測試執行失敗: {e}")
