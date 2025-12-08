import os
from datetime import datetime
from typing import Optional, List, Any
from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel

# ==========================================
# 1. 引入您的爬蟲模組
# ==========================================
# 舊爬蟲 (Morningstar)
from src.morningstar.earnings_scraper import EarningsScraper
# 新爬蟲 (Investing.com) - 假設您已將檔案移至 src/investing/scraper.py
from src.investing.scraper import InvestingCalendarScraper

app = FastAPI(
    title="Financial Data Scraper Service",
    description="整合 Morningstar 與 Investing.com 的爬蟲微服務",
    version="1.0.0"
)

# ==========================================
# 2. 定義資料結構 (Pydantic Models)
#    這是為了讓 API Input/Output 結構化
# ==========================================

# [通用] 標準回應格式 (Standard Response)
class StandardResponse(BaseModel):
    status: str             # "success" or "error"
    message: str            # 描述訊息
    timestamp: str          # 執行時間
    task_id: Optional[str] = None
    data: Optional[Any] = None

# [Input] Morningstar 請求參數
class MorningstarRequest(BaseModel):
    ticker: Optional[str] = None  # 選填，若不填則爬 CSV 全表

# [Input] Investing.com 請求參數
class InvestingRequest(BaseModel):
    start_date: Optional[str] = None  # 格式: YYYY-MM-DD
    end_date: Optional[str] = None    # 格式: YYYY-MM-DD
    countries: Optional[List[str]] = None  # 例如 ["United States", "China"]

# ==========================================
# 3. 定義背景任務邏輯 (Background Tasks)
#    這裡負責實際執行爬蟲，不讓 API 卡住
# ==========================================

def run_morningstar_task(ticker: str = None):
    """執行 Morningstar 財報爬蟲"""
    print(f"[{datetime.now()}] 🚀 啟動 Morningstar 任務 (Ticker: {ticker or 'ALL'})...")
    try:
        scraper = EarningsScraper()
        # 這裡根據您的邏輯，如果要支援單一 Ticker，您可能要修改 EarningsScraper
        # 目前假設它會去讀 CSV
        
        # 為了相容，這裡我們示範基本的執行
        # 注意：您的 earnings_scraper.py 需要確認路徑是否正確
        base_dir = os.getcwd()
        csv_path = os.path.join(base_dir, "input", "morningstar_ET_urls.csv")
        output_dir = os.path.join(base_dir, "output", "morningstar_ET")
        
        # 執行 (這裡只是示範，實際參數看您的 scraper 實作)
        scraper.scrape_all(csv_path, output_dir)
        print(f"[{datetime.now()}] ✅ Morningstar 任務完成")
        
    except Exception as e:
        print(f"[{datetime.now()}] ❌ Morningstar 任務失敗: {e}")

def run_investing_task(start_date: str, end_date: str, countries: List[str]):
    """執行 Investing.com 財經日曆爬蟲"""
    print(f"[{datetime.now()}] 🚀 啟動 Investing 任務 ({start_date} ~ {end_date})...")
    try:
        # 初始化爬蟲 (無頭模式)
        scraper = InvestingCalendarScraper(headless=True)
        
        # 執行爬取
        filename, result = scraper.run(
            start_date=start_date,
            end_date=end_date,
            target_countries=countries
        )
        print(f"[{datetime.now()}] ✅ Investing 任務完成，檔案: {filename}")
        
    except Exception as e:
        print(f"[{datetime.now()}] ❌ Investing 任務失敗: {e}")

# ==========================================
# 4. API 路由 (Endpoints)
# ==========================================

@app.get("/health", tags=["System"])
def health_check():
    """系統健康檢查"""
    return {"status": "ok", "timestamp": datetime.now().isoformat()}

@app.post("/scrape/morningstar", response_model=StandardResponse, tags=["Scrapers"])
async def trigger_morningstar(request: MorningstarRequest, background_tasks: BackgroundTasks):
    """
    觸發 Morningstar 財報爬蟲
    """
    # 將任務加入背景排程 (非阻塞)
    background_tasks.add_task(run_morningstar_task, request.ticker)
    
    return {
        "status": "success",
        "message": "Morningstar 爬蟲任務已接受並在背景執行",
        "timestamp": datetime.now().isoformat(),
        "data": {
            "target": request.ticker or "ALL_FROM_CSV"
        }
    }

@app.post("/scrape/investing", response_model=StandardResponse, tags=["Scrapers"])
async def trigger_investing(request: InvestingRequest, background_tasks: BackgroundTasks):
    """
    觸發 Investing.com 財經日曆爬蟲
    """
    # 將任務加入背景排程 (非阻塞)
    background_tasks.add_task(
        run_investing_task, 
        request.start_date, 
        request.end_date, 
        request.countries
    )
    
    return {
        "status": "success",
        "message": "Investing.com 爬蟲任務已接受並在背景執行",
        "timestamp": datetime.now().isoformat(),
        "data": {
            "start_date": request.start_date,
            "end_date": request.end_date,
            "countries": request.countries
        }
    }