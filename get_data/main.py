import subprocess
import time
files = ['E:\\FPT\\crypto_analysis_project\\scripts_get_data\\get_candle_raw_data.py', 
        'E:\\FPT\\crypto_analysis_project\\scripts_get_data\\get_ticker_raw_data.py', 
        'E:\\FPT\\crypto_analysis_project\\scripts_get_data\\get_trade_raw_data.py']
# Số lần lặp lại
processes = []
# Khởi động tất cả tiến trình trong lần lặp này
for file in files:
    print(f"Đang chạy {file}...")
    p = subprocess.Popen(["python", file])
    processes.append(p)
# Chờ tất cả tiến trình trong lần lặp này hoàn tất
for p in processes:
    p.wait()
