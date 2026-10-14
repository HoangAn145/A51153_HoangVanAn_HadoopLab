
---

# BÁO CÁO DỰ ÁN PHÂN TÍCH DOANH THU ĐƠN HÀNG THƯƠNG MẠI ĐIỆN TỬ BẰNG HADOOP MAPREDUCE

## 1. Giới Thiệu & Mục Tiêu

### 1.1. Giới thiệu

Trong ngành thương mại điện tử (E-Commerce), dữ liệu giao dịch phát sinh hàng ngày với khối lượng rất lớn từ nhiều kênh (Web, App, Partner) và nhiều khu vực địa lý. Việc tổng hợp doanh thu định kỳ giúp bộ phận vận hành nhanh chóng xác định được nhóm sản phẩm chủ lực để tối ưu hóa tồn kho, chiến dịch khuyến mại và nguồn lực giao hàng.

### 1.2. Mục tiêu dự án

* Về kỹ thuật:
* Triển khai cụm Hadoop Single-Node (Pseudo-Distributed) thành công trên môi trường Google Colab.
* Thực hành quản trị hệ thống tệp phân tán HDFS (tạo thư mục, upload, đọc và kiểm tra block dữ liệu).
* Khởi chạy Job Hadoop Streaming (Python) kết hợp giữa mapper.py và reducer.py để tổng hợp doanh thu theo lô (Batch Processing).


* Về nghiệp vụ:
* Lọc các giao dịch thành công (status = SUCCESS) trong tháng 09/2026.
* Phân nhóm và tính tổng doanh thu theo từng danh mục sản phẩm (category).
* Lập bảng Top 3 nhóm sản phẩm có doanh thu cao nhất, trực quan hóa dữ liệu và đưa ra nhận xét chiến lược kinh doanh.



---

## 2. Cấu Trúc Thư Mục Repository

```text
.
├── data/
│   └── orders_2026_09.csv         # Tập dữ liệu đơn hàng thương mại điện tử tháng 09/2026
├── scripts/
│   ├── mapper.py                  # Mã nguồn Mapper (Python)
│   └── reducer.py                 # Mã nguồn Reducer (Python)
├── output/
│   └── part-00000                 # Kết quả xuất ra từ Hadoop MapReduce
├── notebook/
│   └── Hadoop_Ecommerce_Lab.ipynb # Notebook Colab chứa toàn bộ lệnh thực thi & báo cáo
├── images/
│   └── revenue_by_category.png    # Biểu đồ trực quan hóa doanh thu theo danh mục
└── README.md                      # Báo cáo chi tiết dự án

```

---


## 3. Mô Tả Tập Dữ Liệu
```markdown
- Tệp dữ liệu: orders_2026_09.csv (gồm 1.500 bản ghi giao dịch đơn hàng).
- Các trường thông tin trong tệp:

| Tên trường (Column) | Kiểu dữ liệu | Mô tả |
| :--- | :--- | :--- |
| transaction_id | String | Mã định danh duy nhất của giao dịch (VD: TX000001) |
| order_date | Date | Ngày đặt hàng (Tháng 09/2026) |
| order_time | Time | Thời gian đặt hàng |
| province | String | Tỉnh/Thành phố giao hàng (Ha Noi, Da Nang, HCM, ...) |
| category | String | Danh mục sản phẩm (Electronics, Home, Fashion, ...) |
| channel | String | Kênh bán hàng (web, mobile, partner) |
| quantity | Integer | Số lượng sản phẩm mua |
| unit_price | Integer | Đơn giá sản phẩm (VNĐ) |
| amount | Integer | Tổng tiền giao dịch = quantity * unit_price (VNĐ) |
| status | String | Trạng thái đơn hàng (SUCCESS, FAILED, RETURNED) |
| shipping_days | Integer | Số ngày vận chuyển |
```
---

## 4. Kiến Trúc & Quy Trình Xử Lý

### 4.1. Sơ đồ luồng dữ liệu (Data Pipeline)
```text
[ orders_2026_09.csv ]
          │
          ▼  (Upload via hdfs dfs -put)
[ HDFS Input ]  (/user/student/ecommerce/input/)
          │
          ▼  (Đọc từng dòng từ stdin, lọc status == SUCCESS)
[ Mapper.py ]   (Key: Category  ->  Value: Amount)
          │
          ▼
[ Shuffle & Sort ] (Hadoop tự động nhóm các Amount cùng Category)
          │
          ▼
[ Reducer.py ]   (Cộng dồn Amount cho từng Category)
          │
          ▼
[ HDFS Output ]  (/user/student/ecommerce/output/revenue_by_category/part-00000)

```

### 4.2. Nguyên lý Mapper & Reducer

* Mapper (mapper_revenue.py): Đọc từng dòng dữ liệu từ standard input (sys.stdin), sử dụng csv.DictReader để bóc tách cột. Chỉ lọc lấy bản ghi có status == 'SUCCESS' và phát sinh cặp key-value: category \t amount.
* Shuffle & Sort: Framework Hadoop nhận đầu ra của Mapper, gom các bản ghi có chung khóa category về cùng một Reducer.
* Reducer (reducer_revenue.py): Tiếp nhận luồng key-value đã sắp xếp, cộng dồn tổng amount cho từng category và ghi kết quả ra file part-00000.

---

## 5. Nội Dung Chi Tiết Trong Notebook

### 5.1 Cài đặt & Cấu hình Hadoop (Single-Node)

1. Cài đặt OpenJDK 8/17 đảm bảo môi trường Java Virtual Machine (JVM).
2. Tải và giải nén Apache Hadoop 3.3.6 / 3.5.0 vào thư mục làm việc.
3. Cấu hình biến môi trường JAVA_HOME, HADOOP_HOME và bổ sung vào PATH.
4. Cấu hình Passwordless SSH (ssh-keygen, authorized_keys) cho phép kết nối daemon nội bộ.
5. Cấu hình các file XML:
* core-site.xml: Thiết lập mặc định fs.defaultFS = hdfs://localhost:9000.
* hdfs-site.xml: Thiết lập hệ số bản sao dfs.replication = 1.
* hadoop-env.sh: Thiết lập cố định JAVA_HOME và người dùng chạy dịch vụ.



### 5.2 Quản trị Lưu trữ HDFS

1. Định dạng NameNode lần đầu:
```bash
!hdfs namenode -format -force

```


2. Khởi động các dịch vụ HDFS (NameNode, DataNode) và kiểm tra tiến trình bằng jps.
3. Thao tác dữ liệu trên HDFS:
* Thoát Safemode nếu cần: !hdfs dfsadmin -safemode leave
* Tạo thư mục input/output: !hdfs dfs -mkdir -p /user/student/ecommerce/input
* Đẩy tệp dữ liệu lên HDFS: !hdfs dfs -put -f /content/orders_2026_09.csv /user/student/ecommerce/input/
* Kiểm tra thông tin tệp: !hdfs dfs -ls /user/student/ecommerce/input/



### 5.3 Phát triển Thuật toán MapReduce

* Mã nguồn mapper.py:
```python
import sys, csv
reader = csv.DictReader(sys.stdin)
for row in reader:
    if row.get("status", "").strip() == "SUCCESS":
        category = row.get("category", "").strip()
        amount = row.get("amount", "").strip()
        if category and amount:
            print(f"{category}\t{amount}")

```


* Mã nguồn reducer.py:
```python
import sys
current_category, current_sum = None, 0
for line in sys.stdin:
    line = line.strip()
    if not line: continue
    category, amount = line.split("\t")
    amount = int(amount)
    if current_category == category:
        current_sum += amount
    else:
        if current_category:
            print(f"{current_category}\t{current_sum}")
        current_category, current_sum = category, amount
if current_category:
    print(f"{current_category}\t{current_sum}")

```


* Lệnh chạy Hadoop Streaming Job:
```bash
!hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -files mapper.py,reducer.py \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -input /user/student/ecommerce/input/orders_2026_09.csv \
  -output /user/student/ecommerce/output/revenue_by_category

```



### 5.4 Trực quan hóa & Báo cáo Doanh thu

#### 1. Nội dung file kết quả part-00000 trên HDFS:

```text
Beauty        111032003
Book          39306022
Electronics   3703769311
Fashion       218597763
Grocery       80983147
Home          410315065
Sport         166194354
Toy           83881779

```

#### 2. Bảng báo cáo Top 3 Nhóm sản phẩm doanh thu cao nhất:

| Hạng | Category | Doanh thu (VNĐ) | Tỷ trọng | Nhận xét kinh doanh |
| --- | --- | --- | --- | --- |
| 1 | Electronics | 3.703.769.311 | 77,09% | Nhóm sản phẩm tạo ra doanh thu lớn nhất toàn sàn. |
| 2 | Home | 410.315.065 | 8,54% | Nhóm có doanh thu đứng thứ hai. |
| 3 | Fashion | 218.597.763 | 4,55% | Nhóm có doanh thu đứng thứ ba. |

#### 3. Nhận xét chiến lược:

> Trong tháng 09/2026, ngành hàng Electronics đóng vai trò chủ lực vượt trội khi đóng góp tới 3,7 tỷ VNĐ (chiếm hơn 77% tổng doanh thu của các đơn hàng thành công). Hai ngành hàng Home và Fashion lần lượt xếp tiếp theo với doanh thu đạt 410 triệu VNĐ và 218 triệu VNĐ. Kết quả phân tích chỉ ra rằng doanh nghiệp cần ưu tiên nguồn lực cho ngành hàng Electronics về mặt tối ưu tồn kho, chương trình khuyến mại và dịch vụ giao hàng. Bên cạnh đó, việc kết hợp bán kèm (cross-selling) giữa Electronics với Home và Fashion sẽ giúp nâng cao giá trị trung bình trên mỗi đơn hàng.

### 5.5 Báo cáo Lý thuyết Hệ sinh thái Hadoop

* HDFS (Hadoop Distributed File System): Cung cấp khả năng lưu trữ phân tán, tin cậy, chi phí thấp cho khối lượng dữ liệu lớn. Cơ chế chia tệp thành các Block và lưu trữ trên các DataNode giúp hệ thống chịu lỗi cao.
* MapReduce: Mô hình lập trình xử lý dữ liệu phân tán theo lô (batch processing) dựa trên cặp khóa-giá trị (key-value), chia bài toán thành 2 giai đoạn chính là Map (phân rã/lọc) và Reduce (tổng hợp/gom nhóm).
* YARN (Yet Another Resource Negotiator): Lớp quản lý tài nguyên và lập lịch nhiệm vụ cho cụm Hadoop, cho phép nhiều framework xử lý khác nhau (như MapReduce, Spark) cùng chạy trên một hạ tầng.

---

## 6. Hướng Dẫn Chạy Trên Google Colab

### Bước 1: Mở Notebook và kết nối Runtime

1. Truy cập Google Colab (https://colab.research.google.com).
2. Tải tệp Hadoop_Ecommerce_Lab.ipynb từ thư mục notebook/ lên Colab.
3. Vào Runtime -> Change runtime type -> chọn CPU -> Bấm Connect.

### Bước 2: Tải dữ liệu mẫu

Tải file orders_2026_09.csv lên thư mục gốc /content/ của Colab (hoặc chạy ô tạo dữ liệu mẫu tự động trong notebook).

### Bước 3: Chạy lần lượt các Cell trong Notebook

* Chạy Phần 1 để tự động tải Hadoop, cài Java và cấu hình môi trường.
* Chạy Phần 2 để định dạng NameNode, khởi động HDFS daemon và push file dữ liệu vào HDFS.
* Chạy Phần 3 để tạo file mapper.py, reducer.py và thực thi Hadoop Streaming Job.
* Chạy Phần 4 để hiển thị kết quả part-00000, lập bảng báo cáo và vẽ biểu đồ seaborn.

```

```
