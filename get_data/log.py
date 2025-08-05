import logging
import os
log_dir='scripts_get_data'
log_file_path = os.path.join(log_dir, 'log.txt')
# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path, encoding='utf-8'),
        logging.StreamHandler()  # In ra console luôn nếu cần
    ]
)

# Ví dụ ghi log
logging.info("Chương trình bắt đầu chạy.")
logging.warning("Cảnh báo: Đây là một cảnh báo.")
logging.error("Lỗi: Có lỗi xảy ra trong quá trình xử lý.")
