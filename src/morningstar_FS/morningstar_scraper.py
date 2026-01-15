#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Morningstar 財務報表自動化爬蟲程式
從 Morningstar 網站抓取 Income Statement、Balance Sheet 及 Cash Flow 報表

Usage:
    python morningstar_scraper.py              # 處理所有 URL
    python morningstar_scraper.py --test-mode  # 測試模式，僅處理前 3 個 URL
"""

import os
import sys
import time
import glob
import shutil
import argparse
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, 
    NoSuchElementException, 
    ElementClickInterceptedException,
    StaleElementReferenceException
)
from webdriver_manager.chrome import ChromeDriverManager


class MorningstarScraper:
    """Morningstar 財務報表爬蟲類別"""
    
    # 報表類型與對應的 Tab ID
    REPORT_TYPES = {
        'Income Statement': 'incomeStatement',
        'Balance Sheet': 'balanceSheet',
        'Cash Flow': 'cashFlow'
    }
    
    # 輸出資料夾名稱
    OUTPUT_FOLDERS = {
        'Income Statement': 'Income_Statement',
        'Balance Sheet': 'Balance_Sheet',
        'Cash Flow': 'Cash_Flow'
    }
    
    def __init__(self, base_dir: str, headless: bool = True):
        """
        初始化爬蟲
        
        Args:
            base_dir: 專案根目錄路徑
            headless: 是否使用無頭模式
        """
        self.base_dir = Path(base_dir)
        self.input_dir = self.base_dir / '../input'
        self.output_dir = self.base_dir / '../output/morningstar_FS'
        self.headless = headless
        self.driver = None
        self.download_dir = None
        
        # 設定 logging
        self._setup_logging()
        
        # 建立輸出資料夾
        self._create_output_folders()
        
    def _setup_logging(self):
        """設定日誌記錄"""
        log_file = self.base_dir / '../error_log.txt'
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def _create_output_folders(self):
        """建立輸出資料夾結構"""
        for folder in self.OUTPUT_FOLDERS.values():
            folder_path = self.output_dir / folder
            folder_path.mkdir(parents=True, exist_ok=True)
            self.logger.info(f"資料夾已建立/確認: {folder_path}")
            
    def setup_driver(self):
        """設定 Chrome WebDriver"""
        # 建立暫時下載目錄
        self.download_dir = self.base_dir / 'temp_downloads'
        self.download_dir.mkdir(parents=True, exist_ok=True)
        
        chrome_options = Options()
        
        if self.headless:
            chrome_options.add_argument('--headless')
            
        # 基本設定
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # 下載設定
        prefs = {
            'download.default_directory': str(self.download_dir.absolute()),
            'download.prompt_for_download': False,
            'download.directory_upgrade': True,
            'safebrowsing.enabled': True,
            'plugins.always_open_pdf_externally': True
        }
        chrome_options.add_experimental_option('prefs', prefs)
        
        # 初始化 WebDriver
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        self.driver.implicitly_wait(10)
        
        self.logger.info("Chrome WebDriver 初始化完成")
        
    def read_urls(self) -> list:
        """
        讀取 CSV 檔案中的 URL 列表
        
        Returns:
            list: 有效的 URL 列表
        """
        csv_path = self.input_dir / 'morningstar_FS_urls.csv'
        
        if not csv_path.exists():
            raise FileNotFoundError(f"找不到 CSV 檔案: {csv_path}")
            
        # 讀取 CSV (無標題行)
        df = pd.read_csv(csv_path, header=None, names=['url'])
        
        # 過濾無效 URL
        valid_urls = []
        for url in df['url']:
            url = str(url).strip()
            if url.startswith('https://www.morningstar.com/'):
                valid_urls.append(url)
            else:
                self.logger.warning(f"跳過無效 URL: {url}")
                
        self.logger.info(f"讀取到 {len(valid_urls)} 個有效 URL")
        return valid_urls
        
    def extract_ticker(self, url: str) -> str:
        """
        從 URL 中提取股票代碼
        
        Args:
            url: Morningstar URL
            
        Returns:
            str: 股票代碼 (大寫)
        """
        # URL 格式: https://www.morningstar.com/stocks/xnys/ibm/financials
        parts = url.rstrip('/').split('/')
        if len(parts) >= 2:
            ticker = parts[-2].upper()
            return ticker
        return 'UNKNOWN'
        
    def wait_for_page_load(self, seconds: int = 5):
        """等待頁面載入"""
        time.sleep(seconds)
        
    def click_element_safely(self, element, retries: int = 3):
        """安全地點擊元素，處理可能的遮蓋問題"""
        for attempt in range(retries):
            try:
                # 滾動到元素位置
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
                time.sleep(0.5)
                
                # 嘗試點擊
                element.click()
                return True
            except ElementClickInterceptedException:
                # 如果被遮蓋，嘗試關閉可能的彈窗
                self._close_overlays()
                time.sleep(1)
            except StaleElementReferenceException:
                self.logger.warning(f"元素已失效，重試中... ({attempt + 1}/{retries})")
                time.sleep(1)
        return False
        
    def _close_overlays(self):
        """嘗試關閉可能的遮蓋層/彈窗"""
        overlay_selectors = [
            "button[aria-label='Close']",
            ".modal-close",
            ".overlay-close",
            "[data-dismiss='modal']"
        ]
        for selector in overlay_selectors:
            try:
                close_btn = self.driver.find_element(By.CSS_SELECTOR, selector)
                close_btn.click()
                time.sleep(0.5)
            except NoSuchElementException:
                pass
                
    def switch_to_original_reported(self):
        """切換至 'As Originally Reported' 視圖"""
        try:
            # 尋找包含 "As Originally Reported" 或 "Restated" 的按鈕並點擊
            dropdown_btn_xpath = "//button[contains(@class, 'mds-button') and (contains(.//span, 'As Originally Reported') or contains(.//span, 'Restated'))]"
            dropdown_btn = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, dropdown_btn_xpath))
            )
            self.click_element_safely(dropdown_btn)
            time.sleep(1)
            
            # 選擇 "As Originally Reported" 選項
            option_xpath = "//span[contains(@class, 'mds-list-group-item__text') and contains(text(), 'As Originally Reported')]"
            option = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, option_xpath))
            )
            self.click_element_safely(option)
            
            self.logger.info("已切換至 'As Originally Reported' 視圖")
            return True
            
        except TimeoutException:
            self.logger.warning("找不到 'As Originally Reported' 選項，可能已經是該視圖")
            return True
        except Exception as e:
            self.logger.error(f"切換視圖失敗: {e}")
            return False
            
    def switch_to_quarterly(self):
        """切換至季度 (Quarterly) 視圖"""
        try:
            # 尋找年度/季度切換按鈕
            period_btn_xpath = "//button[contains(@class, 'mds-button') and (contains(.//span, 'Annual') or contains(.//span, 'Quarterly'))]"
            period_btn = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, period_btn_xpath))
            )
            
            # 檢查當前是否已經是 Quarterly
            btn_text = period_btn.text.strip()
            if 'Quarterly' in btn_text:
                self.logger.info("已經是 Quarterly 視圖")
                return True
                
            self.click_element_safely(period_btn)
            time.sleep(1)
            
            # 選擇 "Quarterly" 選項
            option_xpath = "//span[contains(@class, 'mds-list-group-item__text') and contains(text(), 'Quarterly')]"
            option = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, option_xpath))
            )
            self.click_element_safely(option)
            
            self.logger.info("已切換至 Quarterly 視圖")
            return True
            
        except TimeoutException:
            self.logger.warning("找不到 Quarterly 選項")
            return False
        except Exception as e:
            self.logger.error(f"切換季度視圖失敗: {e}")
            return False
            
    def switch_to_report_tab(self, report_type: str) -> bool:
        """
        切換至指定的報表分頁
        
        Args:
            report_type: 報表類型 (Income Statement, Balance Sheet, Cash Flow)
            
        Returns:
            bool: 是否成功切換
        """
        tab_id = self.REPORT_TYPES.get(report_type)
        if not tab_id:
            self.logger.error(f"未知的報表類型: {report_type}")
            return False
            
        try:
            tab_btn = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.ID, tab_id))
            )
            self.click_element_safely(tab_btn)
            time.sleep(2)  # 等待分頁載入
            self.logger.info(f"已切換至 {report_type} 分頁")
            return True
            
        except TimeoutException:
            self.logger.error(f"找不到 {report_type} 分頁按鈕")
            return False
            
    def download_report(self) -> bool:
        """
        點擊下載 (Export) 按鈕
        
        Returns:
            bool: 是否成功觸發下載
        """
        try:
            # 使用多種方式嘗試找到 Export 按鈕
            export_selectors = [
                (By.CSS_SELECTOR, "button[aria-label='Export']"),
                (By.ID, "salEqsvFinancialsPopoverExport"),
                (By.XPATH, "//button[contains(@aria-label, 'Export')]"),
                (By.XPATH, "//button[.//span[contains(@data-mds-icon-name, 'share')]]")
            ]
            
            export_btn = None
            for by, selector in export_selectors:
                try:
                    export_btn = WebDriverWait(self.driver, 5).until(
                        EC.element_to_be_clickable((by, selector))
                    )
                    break
                except TimeoutException:
                    continue
                    
            if export_btn is None:
                self.logger.error("找不到 Export 按鈕")
                return False
                
            self.click_element_safely(export_btn)
            self.logger.info("已觸發下載")
            return True
            
        except Exception as e:
            self.logger.error(f"下載失敗: {e}")
            return False
            
    def wait_for_download(self, timeout: int = 30) -> str:
        """
        等待檔案下載完成
        
        Args:
            timeout: 超時時間 (秒)
            
        Returns:
            str: 下載完成的檔案路徑，若失敗則返回 None
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            # 取得下載目錄中的所有檔案
            files = list(self.download_dir.glob('*'))
            
            # 檢查是否有正在下載的檔案
            downloading = [f for f in files if f.suffix in ['.crdownload', '.tmp']]
            
            if downloading:
                time.sleep(1)
                continue
                
            # 找到最新的 xls/xlsx 檔案
            xls_files = [f for f in files if f.suffix in ['.xls', '.xlsx']]
            
            if xls_files:
                # 返回最新的檔案
                newest = max(xls_files, key=lambda f: f.stat().st_mtime)
                return str(newest)
                
            time.sleep(1)
            
        self.logger.error("下載超時")
        return None
        
    def rename_and_move_file(self, file_path: str, ticker: str, report_type: str) -> bool:
        """
        重命名並移動檔案至對應資料夾
        
        Args:
            file_path: 原始檔案路徑
            ticker: 股票代碼
            report_type: 報表類型
            
        Returns:
            bool: 是否成功
        """
        try:
            file_path = Path(file_path)
            extension = file_path.suffix
            
            # 產生時間戳記
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # 新檔名格式: {TICKER}_{報表類型}_Quarterly_As Originally Reported_{時間戳}.xls
            new_filename = f"{ticker}_{report_type}_Quarterly_As Originally Reported_{timestamp}{extension}"
            
            # 目標資料夾
            target_folder = self.output_dir / self.OUTPUT_FOLDERS[report_type]
            target_path = target_folder / new_filename
            
            # 移動檔案
            shutil.move(str(file_path), str(target_path))
            
            self.logger.info(f"檔案已儲存: {target_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"移動檔案失敗: {e}")
            return False
            
    def process_url(self, url: str) -> bool:
        """
        處理單一 URL
        
        Args:
            url: Morningstar 財務頁面 URL
            
        Returns:
            bool: 是否成功處理
        """
        ticker = self.extract_ticker(url)
        self.logger.info(f"開始處理: {ticker} ({url})")
        
        try:
            # 1. 進入網頁
            self.driver.get(url)
            self.wait_for_page_load(5)
            
            # 2. 切換至 "As Originally Reported" 視圖
            self.switch_to_original_reported()
            self.wait_for_page_load(3)
            
            # 3. 切換至季度視圖
            self.switch_to_quarterly()
            self.wait_for_page_load(3)
            
            # 4. 下載各報表
            for report_type in self.REPORT_TYPES.keys():
                self.logger.info(f"處理 {report_type}...")
                
                # 切換分頁
                if not self.switch_to_report_tab(report_type):
                    continue
                    
                # 下載報表
                if not self.download_report():
                    continue
                    
                # 等待下載完成
                downloaded_file = self.wait_for_download()
                if downloaded_file:
                    self.rename_and_move_file(downloaded_file, ticker, report_type)
                else:
                    self.logger.error(f"下載 {report_type} 失敗")
                    
                time.sleep(2)  # 短暫等待
                
            self.logger.info(f"完成處理: {ticker}")
            return True
            
        except Exception as e:
            self.logger.error(f"處理 {ticker} 時發生錯誤: {e}")
            return False
            
    def run(self, test_mode: bool = False):
        """
        執行爬蟲主流程
        
        Args:
            test_mode: 測試模式，僅處理前 3 個 URL
        """
        try:
            # 初始化 WebDriver
            self.setup_driver()
            
            # 讀取 URL 列表
            urls = self.read_urls()
            
            if test_mode:
                urls = urls[:3]
                self.logger.info(f"測試模式: 僅處理前 3 個 URL")
                
            total = len(urls)
            success_count = 0
            fail_count = 0
            
            for idx, url in enumerate(urls, 1):
                self.logger.info(f"進度: {idx}/{total}")
                
                try:
                    if self.process_url(url):
                        success_count += 1
                    else:
                        fail_count += 1
                except Exception as e:
                    self.logger.error(f"處理失敗 ({url}): {e}")
                    fail_count += 1
                    
                # 每處理完一個 URL 稍作休息，避免被封鎖
                time.sleep(2)
                
            # 輸出統計資訊
            self.logger.info("=" * 50)
            self.logger.info(f"處理完成!")
            self.logger.info(f"成功: {success_count}")
            self.logger.info(f"失敗: {fail_count}")
            self.logger.info(f"總計: {total}")
            self.logger.info("=" * 50)
            
        finally:
            # 清理
            if self.driver:
                self.driver.quit()
                self.logger.info("瀏覽器已關閉")
                
            # 清理暫時下載目錄
            if self.download_dir and self.download_dir.exists():
                try:
                    shutil.rmtree(self.download_dir)
                except Exception:
                    pass


def main():
    """主程式入口"""
    parser = argparse.ArgumentParser(description='Morningstar 財務報表爬蟲')
    parser.add_argument('--test-mode', action='store_true', help='測試模式，僅處理前 3 個 URL')
    parser.add_argument('--headless', action='store_true', help='使用無頭模式 (不顯示瀏覽器)')
    args = parser.parse_args()
    
    # 取得專案根目錄 (此腳本位於 src/ 目錄下)
    script_dir = Path(__file__).parent
    base_dir = script_dir.parent
    
    # 建立並執行爬蟲
    scraper = MorningstarScraper(base_dir=base_dir, headless=args.headless)
    scraper.run(test_mode=args.test_mode)


if __name__ == '__main__':
    main()
