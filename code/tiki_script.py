import asyncio
import datetime
import glob
import json
import os
import re
import time
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup

# =====================================================================
# 1. CẤU HÌNH HỆ THỐNG
# =====================================================================
CSV_FOLDER = "csv_data_folder"         # Thư mục chứa các file CSV đầu vào
OUTPUT_DIR = "tiki_products_json"      # Thư mục lưu các file JSON kết quả
ERROR_FILE = "failed_products.json"    # File lưu danh sách sản phẩm bị lỗi hệ thống
SUCCESS_LOG_FILE = "success_ids.txt"   # File lưu vết các ID đã tải xong thành công
DELETED_404_FILE = "deleted_ids_404.txt" # File lưu riêng các ID đã bị Tiki xóa vĩnh viễn

CHUNK_SIZE = 1000             # Số lượng sản phẩm trên mỗi file JSON
CONCURRENT_REQUESTS = 20      # Số lượng request đồng thời tối đa (Giảm xuống nếu chạy mạng nhà bị block)
DELAY_BETWEEN_REQUESTS = 0.1  # Độ trễ nhỏ giữa các request (giây)

TIMEOUT = 15                  # Thời gian chờ tối đa cho mỗi request (giây)
MAX_RETRIES = 3               # Số lần tự động thử lại tối đa khi gặp lỗi mạng/timeout

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "vi-VN,vi;q=0.9,fr-FR;q=0.8",
}

# Danh sách toàn cục chứa lỗi phát sinh trong PHIÊN CHẠY HIỆN TẠI
failed_products_list = []

# =====================================================================
# 2. CÁC HÀM BỔ TRỢ (HELPER FUNCTIONS)
# =====================================================================
def clean_description(html_content):
    if not html_content:
        return ""
    soup = BeautifulSoup(html_content, "html.parser")
    text = soup.get_text(separator=" ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def log_failed_product(product_id, reason):
    """Ghi nhận lỗi vào danh sách tạm thời và đẩy thẳng vào file JSON"""
    error_item = {
        "id": int(product_id),
        "reason": reason,
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    failed_products_list.append(error_item)
    print(f"       [THẤT BẠI HOÀN TOÀN] ID {product_id}: {reason}")


def log_success_id(product_id):
    with open(SUCCESS_LOG_FILE, "a") as f:
        f.write(f"{product_id}\n")


def log_deleted_404(product_id):
    with open(DELETED_404_FILE, "a") as f:
        f.write(f"{product_id}\n")

# =====================================================================
# 3. HÀM TẢI CHI TIẾT SẢN PHẨM (WORKER)
# =====================================================================
async def fetch_product(session, product_id, semaphore):
    url = f"https://api.tiki.vn/product-detail/api/v1/products/{product_id}"

    async with semaphore:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                # Gửi request trực tiếp không qua proxy
                async with session.get(url, headers=HEADERS, timeout=TIMEOUT) as response:
                    
                    if response.status == 200:
                        data = await response.json()
                        images = data.get("images", [])
                        image_urls = [img.get("base_url") for img in images if img.get("base_url")]

                        product_info = {
                            "id": data.get("id"),
                            "name": data.get("name"),
                            "url_key": data.get("url_key"),
                            "price": data.get("price"),
                            "description": clean_description(data.get("description")),
                            "images_url": image_urls
                        }
                        log_success_id(product_id)
                        return product_info

                    elif response.status == 404:
                        print(f"       [404 Not Found] ID {product_id} đã bị gỡ bỏ.")
                        log_deleted_404(product_id)
                        return None

                    elif response.status in [403, 429]:
                        print(f"       [Thử lại {attempt}/{MAX_RETRIES}] ID {product_id} bị Tiki chặn IP (Status {response.status}). Đang đợi thử lại...")
                    else:
                        print(f"       [Thử lại {attempt}/{MAX_RETRIES}] ID {product_id} dính lỗi HTTP {response.status}.")

            except asyncio.TimeoutError:
                print(f"       [Thử lại {attempt}/{MAX_RETRIES}] ID {product_id} bị Quá thời gian phản hồi (>{TIMEOUT}s).")
            except Exception as e:
                print(f"       [Thử lại {attempt}/{MAX_RETRIES}] ID {product_id} dính lỗi kết nối: {type(e).__name__}")
            
            # Nếu chưa hết số lần thử lại, đợi một chút tăng dần thời gian để giảm tải
            if attempt < MAX_RETRIES:
                await asyncio.sleep(attempt * 0.5)
        
        # Nếu đã thử trực tiếp 3 lần mà vẫn không được (ví dụ IP mạng nhà đã bị Tiki block cứng)
        log_failed_product(product_id, f"Lỗi kết nối hoặc bị chặn sau {MAX_RETRIES} lần thử lại.")
        return None

# =====================================================================
# 4. HÀM ĐIỀU PHỐI CHÍNH (MAIN PROCESS)
# =====================================================================
async def main():
    global failed_products_list
    if not os.path.exists(CSV_FOLDER):
        print(f"Lỗi: Không tìm thấy thư mục CSV '{CSV_FOLDER}'!")
        return

    csv_files = glob.glob(os.path.join(CSV_FOLDER, "*.csv"))
    if not csv_files:
        print(f"Không tìm thấy file .csv nào trong thư mục '{CSV_FOLDER}'!")
        return

    print(f"Tìm thấy {len(csv_files)} file CSV cần xử lý.")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

    # --- BƯỚC NÂNG CẤP CHỐNG QUÉT LẠI (PRE-LOAD LOGS) ---
    ignore_ids = set()

    # 1. Nạp ID thành công
    if os.path.exists(SUCCESS_LOG_FILE):
        with open(SUCCESS_LOG_FILE, "r") as f:
            ignore_ids.update(int(line.strip()) for line in f if line.strip())

    # 2. Nạp ID lỗi 404 vĩnh viễn
    if os.path.exists(DELETED_404_FILE):
        with open(DELETED_404_FILE, "r") as f:
            ignore_ids.update(int(line.strip()) for line in f if line.strip())

    # 3. Nạp ID lỗi hệ thống đã có trong file JSON để ignore
    if os.path.exists(ERROR_FILE):
        try:
            with open(ERROR_FILE, "r", encoding="utf-8") as f:
                existing_errors = json.load(f)
                failed_products_list = existing_errors
                ignore_ids.update(int(item["id"]) for item in existing_errors if "id" in item)
        except Exception:
            print("⚠️ File lỗi JSON bị hỏng hoặc trống, sẽ khởi tạo mới.")

    async with aiohttp.ClientSession() as session:
        for file_idx, csv_file_path in enumerate(csv_files, 1):
            file_name_only = os.path.basename(csv_file_path)
            print(f"\n==================================================================")
            print(f"📂 [{file_idx}/{len(csv_files)}] ĐANG ĐỌC FILE: {file_name_only}")
            print(f"==================================================================")

            try:
                df = pd.read_csv(csv_file_path)
                if "id" not in df.columns:
                    print(f"⚠️ Bỏ qua file {file_name_only} vì không có cột 'id'!")
                    continue
                
                all_product_ids = df["id"].dropna().unique().tolist()
                
                # Sàng lọc thông minh: Loại bỏ mọi ID nằm trong blacklist (Success + 404 + Fail JSON)
                product_ids = [pid for pid in all_product_ids if int(pid) not in ignore_ids]
                total_products = len(product_ids)
                
                print(f" -> Tổng số ID gốc của file: {len(all_product_ids):,}")
                print(f" -> Số ID được bỏ qua (Đã xử lý/Lỗi trước đó): {len(all_product_ids) - total_products:,}")
                print(f" -> Số ID thực tế sẽ cào lượt này: {total_products:,}")

                if total_products == 0:
                    print(f"✨ File {file_name_only} không còn ID nào cần tải. Tiếp tục chuyển file...")
                    continue

                for i in range(0, total_products, CHUNK_SIZE):
                    chunk_ids = product_ids[i : i + CHUNK_SIZE]
                    
                    current_timestamp = int(time.time())
                    clean_csv_name = re.sub(r'[^\w\-_]', '_', file_name_only.replace('.csv', ''))
                    json_file_name = f"data_{clean_csv_name}_{current_timestamp}_{i // CHUNK_SIZE + 1}.json"
                    json_file_path = os.path.join(OUTPUT_DIR, json_file_name)

                    print(f"\n🚀 Đang tải cụm sản phẩm (Từ STT {i:,} đến {i+len(chunk_ids):,}) của file {file_name_only}...")

                    tasks = [fetch_product(session, pid, semaphore) for pid in chunk_ids]
                    results = await asyncio.gather(*tasks)
                    valid_results = [res for res in results if res is not None]

                    if valid_results:
                        with open(json_file_path, "w", encoding="utf-8") as f:
                            json.dump(valid_results, f, ensure_ascii=False, indent=4)
                        print(f"   => Đã lưu {len(valid_results)} sản phẩm vào: {json_file_path}")

                    # Cập nhật ghi đè file lỗi JSON một cách an toàn (Bao gồm cả dữ liệu cũ lịch sử)
                    if failed_products_list:
                        with open(ERROR_FILE, "w", encoding="utf-8") as ef:
                            json.dump(failed_products_list, ef, ensure_ascii=False, indent=4)

                    await asyncio.sleep(0.5)

            except Exception as file_error:
                print(f"❌ Có lỗi xảy ra khi xử lý file {file_name_only}: {str(file_error)}")
                continue

    print("\n==================================================================")
    print("🏁 TOÀN BỘ TIẾN TRÌNH ĐÃ HOÀN THÀNH!")
    print(f" -> Tổng số lỗi tích lũy trong hệ thống: {len(failed_products_list):,}")
    print("==================================================================")


if __name__ == "__main__":
    asyncio.run(main())
