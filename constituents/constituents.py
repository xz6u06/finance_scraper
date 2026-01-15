#!/usr/bin/env python3
"""
Append-only updater for S&P 500 constituents.

目的：
- 從 Wikipedia 抓取最新的 S&P 500 成分股
- 與本地 CSV 比對
- 只把「本地缺少的 Symbol」append 寫入 CSV
- 不刪除、不覆蓋、不重寫既有資料

資料來源：
https://en.wikipedia.org/wiki/List_of_S%26P_500_companies
"""

from __future__ import annotations

# ===== 標準函式庫 =====
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Set
import time
import requests
# ===== 第三方套件 =====
import pandas as pd


# ============================================================
# 全域設定（程式的「規格說明區」）
# ============================================================

# Wikipedia S&P 500 成分股頁面
WIKI_URL: str = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

# 表格在 Wikipedia HTML 中的 id
TABLE_ID: str = "constituents"

# 用來判斷是否為同一家公司（主鍵）
KEY_COL: str = "Symbol"

# 每次抓取的時間欄位（每一列都會帶）
FETCHED_COL: str = "fetched_at_utc"

# 指定輸出的三個欄位
COLS: List[str] = ["Symbol", "GICS Sector", "GICS Sub-Industry", FETCHED_COL]


# ============================================================
# 資料結構：用來回傳「這次更新做了什麼」
# ============================================================

@dataclass(frozen=True)
class AppendResult:
    """
    描述一次更新結果的資料結構
    frozen=True 表示建立後不可修改，避免誤改結果
    """
    added: List[str]            # 本次新增的 Symbol 清單
    added_rows: int             # 本次實際新增的列數
    total_rows_after: int       # 更新後 CSV 總列數
    csv_path: str               # CSV 檔案路徑
    fetched_at_utc: str         # 抓取 Wikipedia 的時間（UTC）


# ============================================================
# 負責「抓資料」：只做一件事 → 從 Wikipedia 取得最新表格
# ============================================================

class WikiSP500Fetcher:
    """
    負責「抓資料」：只做一件事 → 從 Wikipedia 取得最新表格

    為什麼要用 requests？
    - 直接 pd.read_html(WIKI_URL) 會讓 pandas 內部用 urllib 去抓網頁
    - 有些網站（包含 Wikipedia 在某些環境/IP）會對「不像瀏覽器」的請求回 403
    - 用 requests 自己補齊 headers（尤其 User-Agent）可大幅降低被擋機率
    """

    def _get_html(self) -> str:
        """
        用 requests 抓 Wikipedia HTML 原始碼（字串）
        - 補上常見瀏覽器 headers（最重要是 User-Agent）
        - 加入簡單重試，避免偶發 403 / 429 / 網路抖動
        """
        headers = {
            # 403 最常見解法：補上 User-Agent 讓請求看起來像瀏覽器
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
            ),
            # 這些不是必須，但可以更像正常瀏覽器請求
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,zh-TW;q=0.8",
            "Connection": "keep-alive",
        }

        last_err: Exception | None = None
        for attempt in range(3):
            try:
                resp = requests.get(WIKI_URL, headers=headers, timeout=20)
                resp.raise_for_status()
                return resp.text
            except Exception as e:
                last_err = e
                # 1s, 2s, 3s 漸進等待，降低被暫時封鎖的機率
                time.sleep(1 + attempt)

        raise RuntimeError(f"Failed to fetch Wikipedia HTML after retries: {last_err}")

    def _parse_table(self, html: str) -> pd.DataFrame:
        """
        把 HTML 字串丟給 pandas.read_html 解析成 DataFrame
        - attrs={"id": TABLE_ID} 仍然可以精準定位 constituents 表格
        """
        tables = pd.read_html(html, attrs={"id": TABLE_ID})
        if not tables:
            raise RuntimeError("Wikipedia constituents table not found (page structure may have changed)")
        return tables[0]

    def fetch(self) -> pd.DataFrame:
        """
        從 Wikipedia 抓取 S&P 500 constituents 表格，
        並整理成只包含 Symbol / Sector / Sub-Industry 的 DataFrame
        """
        # 這次抓取的時間（UTC，寫進每一列）
        fetched_at_utc = datetime.now(timezone.utc).isoformat()

        # 1) 先用 requests 把 HTML 抓下來（避免 pandas/urllib 直連被 403）
        html = self._get_html()

        # 2) 再用 pandas 從 HTML 字串解析表格
        raw = self._parse_table(html)

        # 3) 只取我們需要的三個欄位
        df = raw[["Symbol", "GICS Sector", "GICS Sub-Industry"]].copy()

        # ---------- 資料清理 ----------
        # Symbol 一定要是乾淨字串（避免空白、NaN、型別問題）
        df[KEY_COL] = df[KEY_COL].astype(str).str.strip()

        # 其他欄位也順手清理空白
        for c in ["GICS Sector", "GICS Sub-Industry"]:
            df[c] = df[c].astype(str).str.strip()

        # 保險起見：若同一 Symbol 出現多次，只留最後一筆
        df = df.drop_duplicates(subset=[KEY_COL], keep="last")

        # 每列帶上本次抓取時間（方便後續 DB 增量寫入）
        df[FETCHED_COL] = fetched_at_utc
        
        # 回傳一個 index 乾淨的 DataFrame
        return df.reset_index(drop=True)

# ============================================================
# 負責「存資料」：讀取與寫入本地 CSV
# ============================================================

class CSVStore:
    def __init__(self, path: Path) -> None:
        # CSV 檔案路徑
        self.path = path

    def load(self) -> pd.DataFrame:
        """
        讀取本地 CSV
        - 若檔案不存在，回傳空表（讓流程不中斷）
        """

        if not self.path.exists():
            # 回傳只有欄位、沒有資料的 DataFrame
            return pd.DataFrame(columns=COLS)

        # 強制用字串讀取，避免 Symbol 被轉成數字
        df = pd.read_csv(self.path, dtype=str)

        # 如果舊檔沒有 fetched_at_utc，就補一欄空值
        if FETCHED_COL not in df.columns:
            df[FETCHED_COL] = ""

        # 只保留我們規格內欄位（順序也一致）
        df = df[COLS]

        # Symbol 去除空白，避免比對失準
        df[KEY_COL] = df[KEY_COL].astype(str).str.strip()

        # 去重，確保一個 Symbol 只存在一次
        return (
            df.drop_duplicates(subset=[KEY_COL], keep="last")
              .reset_index(drop=True)
        )

    def append(self, base: pd.DataFrame, to_add: pd.DataFrame) -> pd.DataFrame:
        """
        把新資料 append 到既有資料後，再寫回 CSV
        """

        # 如果這次沒有任何新資料，直接回傳原表
        if to_add.empty:
            return base

        # 合併舊資料與新資料
        merged = (
            pd.concat([base, to_add], ignore_index=True)
            # 以 Symbol 為唯一鍵去重
            .drop_duplicates(subset=[KEY_COL], keep="last")
            # 排序只是為了可讀性（非必要）
            .sort_values(KEY_COL)
            .reset_index(drop=True)
        )

        # 寫回 CSV（不使用 mode="a"，避免 CSV 被破壞）
        merged.to_csv(self.path, index=False, encoding="utf-8")

        return merged


# ============================================================
# 核心流程控制器：把 fetch + compare + append 串起來
# ============================================================

class SP500AppendOnlyUpdater:
    def __init__(self, csv_path: Path) -> None:
        self.fetcher = WikiSP500Fetcher()
        self.store = CSVStore(csv_path)

    @staticmethod
    def _symbols(df: pd.DataFrame) -> Set[str]:
        """
        從 DataFrame 取出 Symbol 欄位，轉成 set
        用來做集合差集運算
        """
        return set(df[KEY_COL].dropna().astype(str))

    def run(self) -> AppendResult:
        """
        執行一次完整的更新流程
        """

        # 1. 抓最新 Wikipedia 資料
        latest = self.fetcher.fetch()

        # 從資料本身拿這次抓取時間（每列都一樣）
        fetched_at = str(latest[FETCHED_COL].iloc[0]) if not latest.empty else datetime.now(timezone.utc).isoformat()

        # 2. 讀取本地 CSV
        existing = self.store.load()

        # 3. 各自取出 Symbol 集合
        latest_syms = self._symbols(latest)
        existing_syms = self._symbols(existing)

        # 4. 核心邏輯：
        #    Wikipedia 有，但本地沒有 → 缺少的 Symbol
        missing_syms = sorted(latest_syms - existing_syms)

        # 5. 從最新資料中，挑出缺少的那些完整列
        to_add = latest[latest[KEY_COL].isin(missing_syms)]

        # 6. Append 寫入 CSV
        updated = self.store.append(existing, to_add)

        # 7. 回傳本次更新摘要
        return AppendResult(
            added=missing_syms,
            added_rows=len(to_add),
            total_rows_after=len(updated),
            csv_path=str(self.store.path),
            fetched_at_utc=fetched_at,
        )


# ============================================================
# 程式進入點
# ============================================================

def main() -> None:
    # CSV 輸出路徑（與程式同資料夾）
    csv_path = Path("sp500_constituents.csv").resolve()

    # 建立 updater 並執行
    updater = SP500AppendOnlyUpdater(csv_path)
    result = updater.run()

    # ----------- 輸出結果給人看 -----------
    print(f"[OK] CSV path          : {result.csv_path}")
    print(f"[OK] fetched_at_utc   : {result.fetched_at_utc}")
    print(f"[OK] added_rows       : {result.added_rows}")
    print(f"[OK] total_rows_after : {result.total_rows_after}")

    if result.added:
        print("[Added Symbols]")
        print(", ".join(result.added))
    else:
        print("[Added Symbols] none")


if __name__ == "__main__":
    main()