import logging
import pandas as pd
import os
import time
from ws_client import BitgetWS
from rest_api import target_list_coin  # Đảm bảo biến này được định nghĩa đúng
from load_to_sql import load_to_db

# --- Cấu hình logging ---
log_dir = r'scripts_get_data\log'
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, 'trade_loader.log'), encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# --- Hàm lấy dữ liệu trade cho từng coin ---
def get_trading_data() -> pd.DataFrame:
    try:
        logging.info(f"📡 Đang lấy dữ liệu trade")
        df = BitgetWS(
            inst_type="SPOT",
            channel="trade",
            inst_ids=target_list_coin,
            dur_sec=1.4
        ).run()
        return df
    except Exception as e:
        logging.error(f"❌ Lỗi khi lấy dữ liệu trade {e}")
        return pd.DataFrame()

# --- Main ---
def main():
    try:
        df=get_trading_data()
        df_final_trading = df.groupby('instId').head(5)
        if df_final_trading.empty:
            logging.warning("⚠️ Không có dữ liệu nào được lấy từ bất kỳ coin nào.")
            return pd.DataFrame()
        logging.info(f"✅ Đã lấy thành công dữ liệu tổng cộng {len(df_final_trading)} dòng.")

        # Load vào DB
        try:
            load_to_db('raw', 'trade_data', df_final_trading)
            logging.info("🎉 Thành công load dữ liệu trade vào DB.")
        except Exception as e:
            logging.error(f"❌ Lỗi khi load dữ liệu vào DB: {e}")

    except Exception as e:
        logging.critical(f"🔥 Lỗi nghiêm trọng trong quá trình xử lý chính: {e}")

# --- Entry point ---
if __name__ == "__main__":
    start = time.time()
    main()
    end = time.time()
    print(f"Thời gian chạy: {end - start:.2f} giây")
