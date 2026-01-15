#!/usr/bin/env python3
from __future__ import annotations

"""
將 sp500_constituents.csv 寫入 PostgreSQL 的 finance.public.stocks 資料表。

設計目標：
1. 可指定 fetched_at_utc 的時間區間（from / to）
2. 只寫入符合時間條件的資料
3. ticker 相同時覆蓋既有資料（UPSERT）
4. updated_at 一律使用資料庫當下時間（NOW()）
"""

# ===== 標準函式庫 =====
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

# ===== 第三方套件 =====
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


# ============================================================
# 常數設定
# ============================================================

# CSV 中用來判斷「這筆資料是哪次抓取」的時間欄位
FETCHED_COL: str = "fetched_at_utc"


# ============================================================
# 設定物件：把所有可變參數集中管理
# ============================================================

@dataclass(frozen=True)
class LoadConfig:
    """
    描述一次「CSV → DB 載入任務」的設定

    frozen=True：
    - 建立後不可修改
    - 避免流程中被誤改（資料工程常見安全寫法）
    """
    csv_path: Path                     # CSV 檔案路徑
    db_url: str                        # SQLAlchemy DB 連線字串
    fetched_from: Optional[datetime]   # fetched_at_utc 起始（含）
    fetched_to: Optional[datetime]     # fetched_at_utc 結束（不含）
    chunk_size: int = 1000             # DB 批次寫入大小


# ============================================================
# 核心類別：負責「讀 CSV → 篩選 → 寫入 DB」
# ============================================================

class StockCSVLoader:
    """
    將 S&P 500 constituents CSV 依 fetched_at_utc 篩選後，
    UPSERT 寫入 finance.public.stocks
    """

    def __init__(self, engine: Engine) -> None:
        # SQLAlchemy engine（連線池 + transaction 管理）
        self.engine = engine

    # --------------------------------------------------------
    # 工具函式：解析 fetched_at_utc 字串成 datetime
    # --------------------------------------------------------
    @staticmethod
    def _parse_dt(s: str) -> datetime:
        """
        fetched_at_utc 是 ISO8601 字串（例如 2026-01-07T12:34:56+00:00）
        datetime.fromisoformat 可以直接解析
        """
        return datetime.fromisoformat(s)

    # --------------------------------------------------------
    # Step 1：讀 CSV + 依 fetched_at_utc 篩選
    # --------------------------------------------------------
    def load_csv_filtered(self, cfg: LoadConfig) -> pd.DataFrame:
        """
        讀取 CSV，並依 fetched_at_utc 時間區間做篩選
        """

        # 一律用字串讀，避免 ticker 被轉成數字
        df = pd.read_csv(cfg.csv_path, dtype=str)

        # ---------- 必要欄位檢查 ----------
        required_cols = {
            "Symbol",
            "GICS Sector",
            "GICS Sub-Industry",
            FETCHED_COL,
        }
        missing = required_cols - set(df.columns)
        if missing:
            raise ValueError(f"CSV 缺少必要欄位: {sorted(missing)}")

        # ---------- 基本清理 ----------
        df["Symbol"] = df["Symbol"].astype(str).str.strip()
        df["GICS Sector"] = df["GICS Sector"].astype(str).str.strip()
        df["GICS Sub-Industry"] = df["GICS Sub-Industry"].astype(str).str.strip()
        df[FETCHED_COL] = df[FETCHED_COL].astype(str).str.strip()

        # ---------- fetched_at_utc → datetime ----------
        # 新增一個暫時用的 datetime 欄位，方便做區間比較
        df["_fetched_dt"] = df[FETCHED_COL].apply(self._parse_dt)

        # ---------- 時間區間篩選 ----------
        # fetched_from：含
        if cfg.fetched_from is not None:
            df = df[df["_fetched_dt"] >= cfg.fetched_from]

        # fetched_to：不含（方便做 daily / batch partition）
        if cfg.fetched_to is not None:
            df = df[df["_fetched_dt"] < cfg.fetched_to]

        # ---------- 對應成 DB 欄位結構 ----------
        out = pd.DataFrame({
            "ticker": df["Symbol"],
            "sector": df["GICS Sector"],
            "industry": df["GICS Sub-Industry"],
        })

        # 移除空 ticker（DB key 不可為空）
        out = out[
            out["ticker"].notna()
            & (out["ticker"].astype(str).str.len() > 0)
        ]

        # 同一批資料中 ticker 重複 → 留最後一筆
        #（避免同一個 batch 內 UPSERT 重複）
        out = (
            out.drop_duplicates(subset=["ticker"], keep="last")
               .reset_index(drop=True)
        )

        return out

    # --------------------------------------------------------
    # Step 2：UPSERT 寫入 PostgreSQL
    # --------------------------------------------------------
    def upsert(self, rows: pd.DataFrame, chunk_size: int = 1000) -> int:
        """
        使用 PostgreSQL ON CONFLICT (ticker) DO UPDATE
        實現「同 ticker 覆蓋，不同 ticker 新增」
        """

        # 沒資料就直接結束
        if rows.empty:
            return 0

        # SQL 使用 named parameters（安全 + 可批次）
        upsert_sql = text("""
            INSERT INTO finance.public.stocks
                (ticker, sector, industry, updated_at)
            VALUES
                (:ticker, :sector, :industry, NOW())
            ON CONFLICT (ticker) DO UPDATE
            SET
                sector = EXCLUDED.sector,
                industry = EXCLUDED.industry,
                updated_at = NOW();
        """)

        total = 0
        records = rows.to_dict(orient="records")

        # engine.begin() 會自動開 transaction
        with self.engine.begin() as conn:
            # 分批寫入，避免一次塞太多筆
            for i in range(0, len(records), chunk_size):
                batch = records[i:i + chunk_size]
                conn.execute(upsert_sql, batch)
                total += len(batch)

        return total


# ============================================================
# DB Engine 建立
# ============================================================

def build_engine(db_url: str) -> Engine:
    """
    建立 SQLAlchemy Engine
    pool_pre_ping=True 可避免連線閒置失效
    """
    return create_engine(
        db_url,
        future=True,
        pool_pre_ping=True,
    )


# ============================================================
# CLI 參數處理
# ============================================================

def parse_cli_dt(value: Optional[str]) -> Optional[datetime]:
    """
    CLI 傳入的時間字串轉成 datetime
    - 空值或未給 → None
    - 支援直接貼 fetched_at_utc 原字串
    """
    if value is None or value.strip() == "":
        return None
    return datetime.fromisoformat(value)


# ============================================================
# 程式進入點
# ============================================================

def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Load sp500_constituents.csv into finance.public.stocks with UPSERT"
    )

    parser.add_argument(
        "--csv",
        required=True,
        help="Path to sp500_constituents.csv",
    )
    parser.add_argument(
        "--db-url",
        required=True,
        help="SQLAlchemy DB URL, e.g. postgresql+psycopg2://user:pass@host:5432/finance",
    )
    parser.add_argument(
        "--from",
        dest="fetched_from",
        default=None,
        help="Inclusive fetched_at_utc start (ISO8601)",
    )
    parser.add_argument(
        "--to",
        dest="fetched_to",
        default=None,
        help="Exclusive fetched_at_utc end (ISO8601)",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1000,
        help="DB insert batch size",
    )

    args = parser.parse_args()

    # 封裝成 LoadConfig（流程用的唯一設定來源）
    cfg = LoadConfig(
        csv_path=Path(args.csv).expanduser().resolve(),
        db_url=args.db_url,
        fetched_from=parse_cli_dt(args.fetched_from),
        fetched_to=parse_cli_dt(args.fetched_to),
        chunk_size=args.chunk_size,
    )

    engine = build_engine(cfg.db_url)
    loader = StockCSVLoader(engine)

    # Step A：CSV → DataFrame（含時間篩選）
    rows = loader.load_csv_filtered(cfg)

    # Step B：UPSERT 到 DB
    written = loader.upsert(rows, chunk_size=cfg.chunk_size)

    # ---------- 結果輸出 ----------
    print(f"[OK] filtered_rows = {len(rows)}")
    print(f"[OK] upserted_rows = {written}")
    if cfg.fetched_from:
        print(f"[OK] fetched_from = {cfg.fetched_from.isoformat()}")
    if cfg.fetched_to:
        print(f"[OK] fetched_to   = {cfg.fetched_to.isoformat()}")


if __name__ == "__main__":
    main()