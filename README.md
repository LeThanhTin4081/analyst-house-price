# 📊 Phân tích Thị trường Bất động sản TP.HCM 2021 – 2025

![Housing Price Analysis](https://drive.google.com/uc?export=view&id=1V6hgv6wq18K5YmiblM5mJKjhJZ8Oad4w)

## 🔎 1. Tổng quan dự án

Dự án thực hiện quy trình phân tích dữ liệu toàn diện (End-to-End) về thị trường bất động sản tại TP. Hồ Chí Minh trong giai đoạn 2021 – 2025. Quy trình được triển khai qua các giai đoạn chính:

* **Thu thập dữ liệu (Web Crawling):** Thu thập dữ liệu từ 4 nền tảng bất động sản hàng đầu tại Việt Nam: `Alonhatdat`, `Batdongsan`, `Meeyland` và  `Nhadat24h`.
* **Tiền xử lý dữ liệu (Data Preprocessing):** Làm sạch dữ liệu thô (raw data), loại bỏ bản ghi trùng lặp (duplicates) từ nhiều nguồn crawl, xử lý các giá trị thiếu (missing values), loại bỏ các điểm dữ liệu bất thường (outliers), chuẩn hóa đơn vị giá và định dạng dữ liệu để chuẩn bị cho việc phân tích.
* **Phân tích khám phá (EDA):** Sử dụng các kỹ thuật thống kê mô tả và biểu đồ phân phối để nhận diện các đặc điểm nổi bật, phát hiện dữ liệu bất thường (outliers) và khám phá mối tương quan giữa các biến trong thị trường Bất động sản.
* **Trực quan hóa chuyên sâu (Visualization):** Xây dựng Dashboard tương tác trên Power BI, giúp theo dõi biến động giá nhà và số lượng tin đăng theo từng quận/huyện và loại hình Bất động sản.

| Giai đoạn | 1. Thu thập dữ liệu | 2. Tiền xử lý | 3. Phân tích EDA | 4. Trực quan hóa |
|---|---|---|---|---|
| **Công cụ** | Python, Selenium | Python, Pandas | Pandas, Seaborn | Power BI, DAX |


Bộ dữ liệu gồm 163,571 tin đăng bán được thu thập từ 4 sàn giao dịch Bất Động Sản trực tuyến. Sau tiền xử lý, bộ dữ liệu phân tích gồm 116,875 bản ghi, bao phủ 4 phân khúc chính: Nhà · Đất · Căn hộ · Biệt thự trên toàn bộ 21 quận/huyện của TP.HCM.

---

## 🔆 2. Kỹ năng phân tích dữ liệu được thể hiện

| Kỹ năng | Công cụ / Kỹ thuật |
|---|---|
| **Thu thập dữ liệu** | Python, Selenium, Web Crawling đa nguồn |
| **Tiền xử lý dữ liệu** | Python, Pandas — Làm sạch, chuẩn hóa, xử lý missing values & outliers |
| **Phân tích khám phá (EDA)** | Python, Pandas, NumPy, Matplotlib, Seaborn — Thống kê mô tả, phân phối, tương quan |
| **Trực quan hóa dữ liệu** | Power BI — DAX Measures, Data Modeling, Interactive Dashboard |

---

## 📌 3. Quy trình thực hiện

### 3.1. Thu thập dữ liệu (Web Crawling)

Xây dựng hệ thống crawler tự động thu thập dữ liệu từ 4 sàn giao dịch BĐS:

| Nguồn | Số tin thu thập |
|---|---|
| batdongsan.com.vn | 76,748 |
| nhadat24h.net | 59,913 |
| alonhadat.com.vn | 17,537 |
| meeyland.com | 9,340 |
| **Tổng cộng** | **163,538** |

Kiến trúc crawler được thiết kế theo pipeline chức năng với cơ chế checkpoint, retry và anti-detection, đảm bảo khả năng thu thập ổn định trên quy mô lớn.

> 📁 **Xem chi tiết Source Code và Tài liệu kỹ thuật tại:** [`crawler/`](./crawler/)


### 3.2. Tiền xử lý dữ liệu (Preprocessing)

Dữ liệu thô từ 4 nguồn được hợp nhất và làm sạch qua các bước chính:

- Loại bỏ bản ghi trùng lặp từ các nguồn crawl chồng chéo.
- Chuẩn hóa tên cột, đơn vị giá (quy đổi "tỷ", "triệu" về VNĐ) và định dạng ngày tháng.
- Tách và chuẩn hóa địa chỉ — trích xuất Quận/Huyện từ chuỗi địa chỉ gốc.
- Xử lý giá trị thiếu và loại bỏ các bản ghi không hợp lệ.

**Kết quả:** 163,571 dòng raw → **116,875 dòng clean** (~28.5% bản ghi bị loại).

> 📁 **Xem chi tiết Source Code và Tài liệu quy trình tiền xử lý tại:** [`notebooks/preprocessing/`](./notebooks/preprocessing/)


### 3.3. Phân tích khám phá dữ liệu (EDA)

Thực hiện phân tích khám phá toàn diện trên bộ dữ liệu đã làm sạch, bao gồm: thống kê mô tả, phân tích phân phối (histogram), phân bố biến kiến trúc, phân tích biến phân loại, chuỗi thời gian, ma trận tương quan và boxplot theo khu vực.

**Một số phát hiện quan trọng từ EDA:**

- **Phân phối lệch phải (Right-Skewed):** Giá trung bình (~17 tỷ) cao hơn nhiều so với trung vị (~7.5 tỷ), cho thấy một số ít BĐS siêu sang kéo lệch toàn bộ chỉ số trung bình.
- **Vị trí là yếu tố quyết định:** Quận 1 dẫn đầu (~33 tỷ/căn), giảm dần qua các quận trung tâm, và thấp nhất ở các huyện ngoại thành (~3.5 tỷ/căn).
- **Thiên lệch thời gian (Time Bias):** Biến `NgayDang` phản ánh thời điểm crawl hơn là vòng đời thực của thị trường — tin cũ đã bị gỡ bỏ trên website.

> 📁 **Xem chi tiết Source Code và Tài liệu phân tích EDA tại:** [`notebooks/eda/`](./notebooks/eda/)


### 3.4. Trực quan hóa trên Power BI

Hệ thống dashboard gồm **1 trang báo cáo**, được thiết kế theo hướng đi từ **tổng quan → chi tiết từng phân khúc**:

| Trang | Nội dung |
|---|---|
| **Tổng quan** | Bức tranh toàn cảnh thị trường Bất động sản TP.HCM |
| **Biệt thự** | Phân tích chuyên sâu phân khúc hạng sang |
| **Căn hộ** | Phân tích phân khúc căn hộ chung cư |
| **Đất** | Phân tích phân khúc đất nền, đất thổ cư |
| **Nhà** | Phân tích phân khúc nhà phố — chiếm tỷ trọng lớn nhất |

<!-- ![Dashboard Tổng quan](./powerbi/screenshots/tong_quan.png) -->

Ứng dụng **DAX nâng cao** để xây dựng hệ thống Measures tính toán giá trung bình, trung vị, giá cao nhất và đơn giá/m². Kết hợp Cross-filtering và Interactive Slicers cho phép người dùng tự khám phá dữ liệu theo nhu cầu.

> 🔗 **Trải nghiệm trực tiếp:** [Truy cập Báo cáo Power BI (Bản công khai) tại đây](https://app.powerbi.com/view?r=eyJrIjoiYTE3OWVkZWMtYzMzZi00N2IwLWE4MDMtOTdhNTQzNzM4YWQ4IiwidCI6ImVkOGYxNjczLTM4OTAtNGRiNC1hM2YwLTk3YWQ5NDI3Yzc0ZiIsImMiOjEwfQ%3D%3D)



---

## 📈 4. Các kết quả phân tích chính (Key Insights)

### 4.1. Insight Thống kê: Bẫy "Giá trung bình" (Mean vs Median)
Phát hiện cốt lõi của dự án đến từ việc kết hợp **biểu đồ cột** (Mean) và **biểu đồ hộp** (Median) trên cùng một trang, phơi bày mức độ bóp méo của các BĐS "siêu sang" lên chỉ số trung bình toàn thị trường:

| Phân khúc | Mean/m² | Median/m² | Tỷ lệ chênh lệch |
|---|---|---|---|
| **Đất** | 70 triệu | 5 triệu | **×14** |
| **Nhà** | 152 triệu | 25 triệu | **×6** |
| **Căn hộ** | 73 triệu | 28 triệu | **×2.6** |

**Kết luận:** Việc chỉ sử dụng "Giá trung bình" (Mean) để đánh giá thị trường là cực kỳ sai lệch. Box Plot và Median là công cụ bắt buộc để hiểu đúng giá trị "điển hình" và sức mua thực tế của đại đa số người dân.

### 4.2. Insight Thị trường: Nguồn cung và Định giá
Bức tranh thị trường BĐS TP.HCM được định hình rõ rệt qua các yếu tố:

- **Vị trí định hình phân khúc:** Quận 1 và Quận 3 luôn dẫn đầu về đơn giá trong mọi loại hình. Ngược lại, nguồn cung "vừa túi tiền" dạt hẳn về các huyện ngoại thành (Bình Chánh, Hóc Môn, Củ Chi).
- **Đặc điểm nguồn cung:** **"Nhà phố"** chiếm tỷ trọng áp đảo nhất, tập trung dày đặc ở các quận Tân Bình, Gò Vấp, Bình Tân. Trong khi đó, **"Căn hộ"** có dấu ấn rõ nét tại Quận 7 và TP. Thủ Đức, phản ánh đúng xu hướng quy hoạch đô thị mới.
- **Tiêu chuẩn kiến trúc đô thị:** Phần lớn các BĐS tập trung ở cấu trúc 2-4 tầng và 2-4 phòng ngủ, phản ánh chuẩn mực thiết kế phổ biến nhất của hộ gia đình tại TP.HCM.

### 4.3. Đánh giá Chất lượng Dữ liệu (Data Quality)
Quá trình xử lý dữ liệu thô từ 4 nguồn crawl đã bộc lộ nhiều điểm mù của thị trường thông tin BĐS trực tuyến:

- **Missing Values cục bộ:** Các biến kiến trúc chi tiết (`SoPhongTam`, `ChoDeXe`) có tỷ lệ rỗng rất cao (>50%), phản ánh thói quen đăng tin thiếu minh bạch hoặc sơ sài của môi giới.
- **Outliers "Ẩn danh":** Nhóm tin đăng có `Loại BĐS = Không xác định` chính là nguồn gốc của các mức giá phi lý (ví dụ: hàng nghìn tỷ). Việc loại trừ nhóm này là bắt buộc trước khi đưa vào mô hình phân tích sâu hơn.
- **Thiên lệch thời gian (Time Bias):** Biến `NgayDang` hoàn toàn bị bóp méo do các website BĐS thường xuyên gỡ tin cũ. Do đó, dữ liệu crawl ngang (Cross-sectional) này không phù hợp để chạy các mô hình dự báo chuỗi thời gian (Time Series).


---

## 🔗 5. Cấu trúc dự án & Liên kết chính
Dự án được tổ chức gọn gàng theo từng module chức năng:
```text
bds_hcm_project_data/
├── crawler/               # Module thu thập dữ liệu (Web Crawling)
│   ├── web_alonhatdat/
│   ├── web_bds/
│   ├── web_meeyland/
│   ├── web_nhadat24h/
│   └── README.md
├── data/                  # Dữ liệu dự án
│   ├── processed/
│   │   └── fulldata_clean_4web.csv
│   ├── raw/
│   │   ├── fulldata_raw_4web.csv
│   │   ├── raw_data_alonhatdat_full.csv
│   │   ├── raw_data_bds_full.csv
│   │   ├── raw_data_meeyland_full.csv
│   │   └── raw_data_nhadat24h_full.csv
│   └── README.md
├── notebooks/             # Phân tích dữ liệu
│   ├── eda/
│   │   ├── eda_bds_hcm.ipynb
│   │   └── README.md
│   └── preprocessing/
│       ├── preprocessing_bds_hcm.ipynb
│       └── README.md
├── powerbi/               # Trực quan hóa Power BI
│   ├── Housing_Price_Analysis_2021_2025.pbix
│   └── README.md
├── .gitignore
└── README.md                   
```

| Phân khu | Mô tả | Liên kết |
|---|---|---|
| **Dữ liệu** | Cấu trúc dữ liệu raw/processed, data dictionary | [`data/`](./data/) |
| **Web Crawling** | Kiến trúc crawler, kỹ thuật thu thập | [`crawler/`](./crawler/) |
| **Tiền xử lý** | Pipeline làm sạch và chuẩn hóa dữ liệu | [`notebooks/preprocessing/`](./notebooks/preprocessing/) |
| **EDA** | Phân tích khám phá, thống kê mô tả, tương quan | [`notebooks/eda/`](./notebooks/eda/) |
| **Power BI** | Dashboard trực quan hóa, phân tích phân khúc | [`Link xem trực tiếp báo cáo`](https://app.powerbi.com/view?r=eyJrIjoiYTE3OWVkZWMtYzMzZi00N2IwLWE4MDMtOTdhNTQzNzM4YWQ4IiwidCI6ImVkOGYxNjczLTM4OTAtNGRiNC1hM2YwLTk3YWQ5NDI3Yzc0ZiIsImMiOjEwfQ%3D%3D) |
