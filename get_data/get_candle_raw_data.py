import logging
import pandas as pd
import os
import time
from ws_client import BitgetWS
from rest_api import target_list_coin  # Đảm bảo biến này được định nghĩa đúng
from load_to_sql import load_to_db

# --- Logging setup ---
log_dir = r'scripts_get_data\log'
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, 'candle_loader.log'), encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# --- Function to get 5m candle data for a single coin ---
def get_candle_data() -> pd.DataFrame:
    try:
        logging.info(f"Đang lấy dữ liệu candle 1m cho")
        df = BitgetWS(
            inst_type="SPOT",
            channel="candle1m",
            inst_ids=target_list_coin,
            dur_sec=1.4
        ).run()
        return df
    except Exception as e:
        logging.error(f"❌ Lỗi khi lấy dữ liệu: {e}")
        return pd.DataFrame()

# --- Main process ---
def main():
    try:
        df=get_candle_data()
        df_final=df.groupby('instId').head(5)
        if df_final.empty:
            logging.warning("Không có dữ liệu nào được lấy.")
            return pd.DataFrame()
        logging.info(f'Thu thập thành công {len(df_final)} dòng')
        # Load vào DB
        try:
            load_to_db('raw', 'candle_data', df_final)
            logging.info("✅ Thành công load vào DB.")
        except Exception as e:
            logging.error(f"❌ Lỗi khi load vào DB: {e}")

    except Exception as e:
        logging.critical(f"❌ Lỗi không mong đợi trong chương trình chính: {e}")

# --- Entry point ---
if __name__ == "__main__":
    start = time.time()
    main()
    end = time.time()
    print(f"Thời gian chạy: {end - start:.2f} giây")
