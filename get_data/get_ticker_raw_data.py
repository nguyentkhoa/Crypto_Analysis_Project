import logging
import pandas as pd
import os
import time
from ws_client import BitgetWS
from rest_api import get_all_spot_pairs
from load_to_sql import load_to_db

# --- Cấu hình logging ---
log_dir = r'scripts_get_data\log'
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, 'ticker_loader.log'), encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# --- Hàm lấy dữ liệu ticker ---
def get_ticker_data() -> pd.DataFrame:
    try:
        logging.info("📡 Đang lấy dữ liệu ticker từ WebSocket...")
        df = BitgetWS(
            inst_type="SPOT",
            channel="ticker",
            inst_ids=get_all_spot_pairs(),
            dur_sec=1
        ).run()
        return df
    except Exception as e:
        logging.error(f"❌ Lỗi khi lấy dữ liệu ticker: {e}")
        return pd.DataFrame()

# --- Main ---
def main():
    try:
        df_ticker = get_ticker_data()

        if df_ticker.empty:
            logging.warning("⚠️ Không có dữ liệu ticker được trả về.")
            return
        logging.info(f"✅ Lấy thành công {len(df_ticker)} dòng. Đang load vào DB...")

        load_to_db('raw','ticker_data',df_ticker)
        logging.info("🎉 Thành công load dữ liệu ticker vào DB.")

    except Exception as e:
        logging.critical(f"🔥 Lỗi nghiêm trọng trong quá trình xử lý: {e}")

# --- Entry point ---
if __name__ == "__main__":
    start = time.time()
    main()
    end = time.time()
    print(f"Thời gian chạy: {end - start:.2f} giây")
