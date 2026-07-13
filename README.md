# Project2_TikiAPI
Data learning

**Request:**

Sử dụng code Python, tải về thông tin của 200k sản phẩm (list product id bên dưới) của Tiki và lưu thành các file .json. 
Mỗi file có thông tin của khoảng 1000 sản phẩm. Các thông in cần lấy bao gồm: id, name, url_key, price, description, images url. 
Yêu cầu chuẩn hoá nội dung trong "description" và tìm phương án rút ngắn thời gian lấy dữ liệu.
- List 600 product_id: (https://1drv.ms/f/c/5961f7334e952fe9/IgDpL5VOM_dhIIBZAIoAAAAAATpYFYLJToIgLXCflY3DaIU?e=ghBrEH)
- API get product detail: https://api.tiki.vn/product-detail/api/v1/products/138083218
  
**Solution:**
- Project code by Python and call execute direct on Linux
- Crawl product detail information and save in Json file
- Using asyncio and aiohttp to fetch data
- Support write success and failed log to ignore if rerun
  
**Input data**:
the lists ID keep in folder with 3 csv files.

/code/source_csv

**Output data**:

Locate in: /code/result_products_json

Batch will break to 1000 IDs each time and save result to json file. Below fields will we save:
id
name
url
key
price
description
images_url
discount
discount_rate
rating_average
review_count 

**How to run**:

call from command:

sudo python3 tiki_script.py


  
