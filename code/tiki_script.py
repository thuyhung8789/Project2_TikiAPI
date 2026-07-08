import asyncio
import datetime
import glob
import json
import os
import random
import re
import time
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup

# =====================================================================
# 1. CẤU HÌNH HỆ THỐNG (Có thể điều chỉnh tùy nhu cầu)
# =====================================================================
CSV_FOLDER = "source_csv"     # Tên file CSV chứa danh sách 200k ID ban đầu
OUTPUT_DIR = "result_products_json"      # Thư mục lưu các file JSON kết quả
ERROR_FILE ="failed_products.json"    # File lưu danh sách các sản phẩm bị lỗi
SUCCESS_LOG_FILE ="success_ids2.txt"   # File lưu vết các ID đã tải thành công (Chase-back)

CHUNK_SIZE = 1000             # Số lượng sản phẩm lưu trên mỗi file JSON
CONCURRENT_REQUESTS = 20      # Số lượng request đồng thời tối đa (Tăng nếu nhiều proxy ngon)
DELAY_BETWEEN_REQUESTS = 0.2  # Độ trễ nhỏ giữa các request của cùng một luồng (giây)

# Giả lập trình duyệt (User-Agent) để tránh bị hệ thống Tiki chặn ngay từ đầu
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "vi-VN,vi;q=0.9,fr-FR;q=0.8,en-US;q=0.6,en;q=0.5",
}

# --- DANH SÁCH PROXY XOAY VÒNG ---
# Hãy điền danh sách proxy HTTP/HTTPS của bạn vào đây. 
# Định dạng có mật khẩu: "http://username:password@ip:port"
# Định dạng không mật khẩu: "http://ip:port"
#PROXIES = [
    # "http://123.45.67.89:8080",
    # "http://98.76.54.321:3128",
#]

# Biến toàn cục chứa danh sách lỗi tích lũy trong phiên chạy hiện tại
failed_products_list = []

# =====================================================================
# 2. CÁC HÀM BỔ TRỢ (HELPER FUNCTIONS)
# =====================================================================
#def get_random_proxy():
#   Lấy ngẫu nhiên một proxy từ danh sách để thực hiện request.
 #   if not PROXIES:
  #      return None
   # return random.choice(PROXIES)
os.makedirs(OUTPUT_DIR, exist_ok=True)

def clean_description(html_content):
    """Chuẩn hóa nội dung description: Xóa toàn bộ thẻ HTML, làm sạch khoảng trắng."""
    if not html_content:
        return ""
    # Loại bỏ toàn bộ tag HTML
    soup = BeautifulSoup(html_content, "html.parser")
    text = soup.get_text(separator=" ")

    # Loại bỏ nhiều dấu cách, dấu tab, dấu xuống dòng liên tiếp thành một khoảng trắng duy nhất
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def log_failed_product(product_id, reason):
    """Ghi nhận thông tin sản phẩm bị lỗi vào bộ nhớ tạm."""
    error_item = {
        "id": int(product_id),
        "reason": reason,
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    failed_products_list.append(error_item)
    print(f"       [LỖI] ID {product_id}: {reason}")


def log_success_id(product_id):
    """Ghi nhanh ID đã tải thành công vào file txt dưới dạng append (ghi nối tiếp)."""
    with open(SUCCESS_LOG_FILE, "a") as f:
        f.write(f"{product_id}\n")

# =====================================================================
# 3. HÀM TẢI CHI TIẾT SẢN PHẨM (ASYNC WORKER)
# =====================================================================
async def fetch_product(session, product_id, semaphore):
    """Thực hiện gọi API lấy thông tin sản phẩm, bóc tách và làm sạch dữ liệu."""
    url = f"https://api.tiki.vn/product-detail/api/v1/products/{product_id}"
  #  proxy_url = get_random_proxy()

    async with semaphore:
        try:
            # Gửi request với cấu hình headers, proxy ngẫu nhiên và thời gian timeout là 12 giây
            async with session.get(url, headers=HEADERS, timeout=12) as response:
                if response.status == 200:
                    data = await response.json()

                    # Bóc tách mảng danh sách ảnh thành danh sách URL dạng text đơn giản
                    images = data.get("images", [])
                    image_urls = [img.get("base_url") for img in images if img.get("base_url")]

                    # Tổ chức lại dữ liệu sạch theo yêu cầu
                    product_info = {
                        "id": data.get("id"),
                        "name": data.get("name"),
                        "url_key": data.get("url_key"),
                        "price": data.get("price"),
                        "description": clean_description(data.get("description")),
                        "images_url": image_urls
                    }

                    # Ghi nhận ID thành công vào file log txt để chase-back
                    log_success_id(product_id)
                    return product_info

                elif response.status == 404:
                    log_failed_product(product_id, "Product không tồn tại trên hệ thống (404)")
                    return None
                elif response.status in [403, 429]:
                    log_failed_product(product_id, f"Bị Tiki chặn IP/Giới hạn tần suất (Status {response.status})")
                    return None
                else:
                    log_failed_product(product_id, f"Lỗi phản hồi từ Tiki (Status {response.status})")
                    return None

        except aiohttp.ClientProxyConnectionError:
            log_failed_product(product_id, f"Không thể kết nối thông qua Proxy: {proxy_url}")
            return None
        except asyncio.TimeoutError:
            log_failed_product(product_id, "Request bị quá thời gian phản hồi (Timeout)")
            return None
        except Exception as e:
            log_failed_product(product_id, f"Lỗi không xác định: {str(e)}")
            return None
        finally:
            # Nghỉ một khoảng thời gian cực ngắn sau mỗi request để tránh spam dồn dập
            await asyncio.sleep(DELAY_BETWEEN_REQUESTS)

# =====================================================================
# 4. HÀM ĐIỀU PHỐI CHÍNH (MAIN PROCESS)
# =====================================================================
async def main():
    # Bước 1: Kiểm tra file CSV đầu vào
# Lấy danh sách tất cả các file .csv trong thư mục
    csv_files = glob.glob(os.path.join(CSV_FOLDER, "*.csv"))
    if not csv_files:
        print(f"Không tìm thấy file .csv nào trong thư mục '{CSV_FOLDER}'!")
        return

    print(f"Tìm thấy {len(csv_files)} file CSV cần xử lý.")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

    # Khởi tạo một phiên kết nối duy nhất dùng chung cho toàn bộ chương trình
    async with aiohttp.ClientSession() as session:
        
        # VÒNG LẶP DUYỆT TỪNG FILE CSV TRONG THƯ MỤC
        for file_idx, csv_file_path in enumerate(csv_files, 1):
            file_name_only = os.path.basename(csv_file_path)
            print(f"\n==================================================================")
            print(f"📂 [{file_idx}/{len(csv_files)}] ĐANG XỬ LÝ FILE: {file_name_only}")
            print(f"==================================================================")

            try:
                # Đọc danh sách ID từ file CSV hiện tại
                df = pd.read_csv(csv_file_path)
                if "id" not in df.columns:
                    print(f"⚠️ Bỏ qua file {file_name_only} vì không có cột 'id'!")
                    continue
                
                all_product_ids = df["id"].dropna().unique().tolist()
                
                # Đọc lịch sử các ID đã chạy thành công trước đó (bất kể thuộc file CSV nào)
                success_ids = set()
                if os.path.exists(SUCCESS_LOG_FILE):
                    with open(SUCCESS_LOG_FILE, "r") as f:
                        success_ids = set(int(line.strip()) for line in f if line.strip())
                        
                # Lọc sạch: Chỉ lấy những ID chưa chạy
                product_ids = [pid for pid in all_product_ids if pid not in success_ids]
                total_products = len(product_ids)
                
                print(f" -> Tổng số ID trong file: {len(all_product_ids):,}")
                print(f" -> ID đã hoàn thành trước đây: {len(all_product_ids) - total_products:,}")
                print(f" -> ID còn lại cần tải của file này: {total_products:,}")

                if total_products == 0:
                    print(f"✨ File {file_name_only} đã được hoàn thành trước đó. Chuyển file tiếp theo.")
                    continue

                # Bắt đầu chia cụm (chunk) cho file CSV hiện tại
                for i in range(0, total_products, CHUNK_SIZE):
                    chunk_ids = product_ids[i : i + CHUNK_SIZE]
                    
                    # Tạo tên file JSON có chứa tên file CSV gốc để bạn dễ phân loại dữ liệu sau này
                    current_timestamp = int(time.time())
                    clean_csv_name = re.sub(r'[^\w\-_]', '_', file_name_only.replace('.csv', ''))
                    json_file_name = f"data_{clean_csv_name}_{current_timestamp}_{i // CHUNK_SIZE + 1}.json"
                    json_file_path = os.path.join(OUTPUT_DIR, json_file_name)

                    print(f"\n🚀 Tải cụm sản phẩm (Từ STT {i:,} đến {i+len(chunk_ids):,}) của file {file_name_only}...")

                    tasks = [fetch_product(session, pid, semaphore) for pid in chunk_ids]
                    results = await asyncio.gather(*tasks)
                    valid_results = [res for res in results if res is not None]

                    if valid_results:
                        with open(json_file_path, "w", encoding="utf-8") as f:
                            json.dump(valid_results, f, ensure_ascii=False, indent=4)
                        print(f"   => Đã lưu {len(valid_results)} sản phẩm vào: {json_file_path}")

                    # Cập nhật danh sách lỗi định kỳ
                    if failed_products_list:
                        with open(ERROR_FILE, "w", encoding="utf-8") as ef:
                            json.dump(failed_products_list, ef, ensure_ascii=False, indent=4)

            except Exception as file_error:
                print(f"❌ Có lỗi xảy ra khi xử lý file {file_name_only}: {str(file_error)}")
                continue

    # --- BÁO CÁO TỔNG KẾT ---
    print("\n==================================================================")
    print("🏁 TOÀN BỘ THƯ MỤC CSV ĐÃ ĐƯỢC XỬ LÝ XONG!")
    print(f" -> Tổng số lỗi phát sinh: {len(failed_products_list):,}")
    print("==================================================================")


if __name__ == "__main__":
    asyncio.run(main())
