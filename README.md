# 📊 Phân tích Thị trường Bất động sản TP.HCM 2021 – 2025

<!-- ![Dashboard Preview](./powerbi/screenshots/overview.png) -->

## 🔎 1. Tổng quan dự án

Dự án thực hiện phân tích toàn diện thị trường bất động sản tại TP. Hồ Chí Minh trong giai đoạn 2021 – 2025, từ khâu **thu thập dữ liệu tự động** (web crawling) đến **phân tích khám phá** (EDA) và **trực quan hóa chuyên sâu** bằng Power BI.

Thay vì sử dụng một bộ dữ liệu có sẵn, toàn bộ pipeline được xây dựng từ đầu:

```
Web Crawling → Tiền xử lý dữ liệu → Phân tích khám phá (EDA) → Trực quan hóa Power BI
```

Bộ dữ liệu bao gồm **163,571 tin đăng bán** thu thập từ 4 sàn giao dịch BĐS trực tuyến, sau khi làm sạch còn **116,875 bản ghi** phục vụ phân tích, bao phủ 4 phân khúc chính: **Nhà** · **Đất** · **Căn hộ** · **Biệt thự** trên toàn bộ 21 quận/huyện của TP.HCM.

---

## 🔆 2. Kỹ năng phân tích dữ liệu được thể hiện

| Kỹ năng | Công cụ / Kỹ thuật |
|---|---|
| **Thu thập dữ liệu** | Python, Selenium, Web Crawling đa nguồn |
| **Tiền xử lý dữ liệu** | Python, Pandas — Làm sạch, chuẩn hóa, xử lý missing values & outliers |
| **Phân tích khám phá (EDA)** | Python, Pandas, Matplotlib, Seaborn — Thống kê mô tả, phân phối, tương quan |
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

> 📁 Chi tiết kỹ thuật crawling: [`crawler/`](./crawler/)

### 3.2. Tiền xử lý dữ liệu (Preprocessing)

Dữ liệu thô từ 4 nguồn được hợp nhất và làm sạch qua các bước chính:

- Loại bỏ bản ghi trùng lặp từ các nguồn crawl chồng chéo.
- Chuẩn hóa tên cột, đơn vị giá (quy đổi "tỷ", "triệu" về VNĐ) và định dạng ngày tháng.
- Tách và chuẩn hóa địa chỉ — trích xuất Quận/Huyện từ chuỗi địa chỉ gốc.
- Xử lý giá trị thiếu và loại bỏ các bản ghi không hợp lệ.

**Kết quả:** 163,571 dòng raw → **116,875 dòng clean** (~28.5% bản ghi bị loại).

> 📁 Chi tiết preprocessing: [`notebooks/preprocessing/`](./notebooks/preprocessing/)

### 3.3. Phân tích khám phá dữ liệu (EDA)

Thực hiện phân tích khám phá toàn diện trên bộ dữ liệu đã làm sạch, bao gồm: thống kê mô tả, phân tích phân phối (histogram), phân bố biến kiến trúc, phân tích biến phân loại, chuỗi thời gian, ma trận tương quan và boxplot theo khu vực.

**Một số phát hiện quan trọng từ EDA:**

- **Phân phối lệch phải (Right-Skewed):** Giá trung bình (~17 tỷ) cao hơn nhiều so với trung vị (~7.5 tỷ), cho thấy một số ít BĐS siêu sang kéo lệch toàn bộ chỉ số trung bình.
- **Vị trí là yếu tố quyết định:** Quận 1 dẫn đầu (~33 tỷ/căn), giảm dần qua các quận trung tâm, và thấp nhất ở các huyện ngoại thành (~3.5 tỷ/căn).
- **Thiên lệch thời gian (Time Bias):** Biến `NgayDang` phản ánh thời điểm crawl hơn là vòng đời thực của thị trường — tin cũ đã bị gỡ bỏ trên website.

> 📁 Chi tiết phân tích EDA: [`notebooks/eda/`](./notebooks/eda/)

### 3.4. Trực quan hóa trên Power BI

Hệ thống dashboard gồm **5 trang báo cáo**, được thiết kế theo hướng đi từ **tổng quan → chi tiết từng phân khúc**:

| Trang | Nội dung |
|---|---|
| **Tổng quan** | Bức tranh toàn cảnh thị trường BĐS TP.HCM |
| **Biệt thự** | Phân tích chuyên sâu phân khúc hạng sang |
| **Căn hộ** | Phân tích phân khúc căn hộ chung cư |
| **Đất** | Phân tích phân khúc đất nền, đất thổ cư |
| **Nhà** | Phân tích phân khúc nhà phố — chiếm tỷ trọng lớn nhất |

<!-- ![Dashboard Tổng quan](./powerbi/screenshots/tong_quan.png) -->

Ứng dụng **DAX nâng cao** để xây dựng hệ thống Measures tính toán giá trung bình, trung vị, giá cao nhất và đơn giá/m². Kết hợp Cross-filtering và Interactive Slicers cho phép người dùng tự khám phá dữ liệu theo nhu cầu.

> 📁 Chi tiết phân tích Dashboard: [`powerbi/`](./powerbi/)

---

## 📈 4. Các kết quả phân tích chính (Key Insights)

### Phát hiện quan trọng nhất: Mean vs Median

Phát hiện cốt lõi của dự án đến từ việc kết hợp **biểu đồ cột** (Mean) và **biểu đồ hộp** (Median) trên cùng một trang, phơi bày mức độ bóp méo của outlier lên chỉ số trung bình:

| Phân khúc | Mean/m² | Median/m² | Tỷ lệ chênh lệch |
|---|---|---|---|
| **Đất** | 70 triệu | 5 triệu | **×14** |
| **Nhà** | 152 triệu | 25 triệu | **×6** |
| **Căn hộ** | 73 triệu | 28 triệu | **×2.6** |

**Kết luận:** Việc chỉ sử dụng "Giá trung bình" (Mean) là cực kỳ sai lệch do ảnh hưởng của các giá trị ngoại lai. Box Plot là công cụ bắt buộc để hiểu đúng giá trị "điển hình" của một BĐS.

### Các yếu tố ảnh hưởng giá được xác nhận

- **Vị trí (Quận/Huyện):** Yếu tố hàng đầu quyết định giá trị, với Quận 1 và Quận 3 luôn dẫn đầu trong mọi phân khúc.
- **Loại hình BĐS:** Mỗi loại hình có phân bố giá hoàn toàn khác nhau — Biệt thự (~180 triệu/m²) và Nhà (~152 triệu/m²) dẫn đầu, Căn hộ (~73 triệu/m²) và Đất (~70 triệu/m²) tương đương nhau.
- **Diện tích:** Biến có tương quan cao nhất với giá bán (xác nhận qua ma trận tương quan trong EDA).

### Vấn đề chất lượng dữ liệu

- Nhiều biến kiến trúc (`SoPhongTam`, `ChoDeXe`) có tỷ lệ missing cao do đặc thù tin đăng thiếu thông tin.
- Nhóm `Loại BĐS = Không xác định` là nguồn chính của các giá trị phi lý còn sót lại.
- Biến `NgayDang` không phù hợp cho phân tích Time Series do thiên lệch thời điểm crawl.

---

## 🔗 5. Cấu trúc dự án

```
bds_hcm_project_data/
├── crawler/               # Module thu thập dữ liệu (Web Crawling)
│   ├── web_bds/           # Crawler cho batdongsan.com.vn
│   ├── web_nhadat24h/     # Crawler cho nhadat24h.net
│   ├── web_alonhatdat/    # Crawler cho alonhadat.com.vn
│   ├── web_meeyland/      # Crawler cho meeyland.com
│   └── README.md
├── data/                  # Dữ liệu dự án
│   ├── raw/               # Dữ liệu thô (163,571 dòng)
│   ├── processed/         # Dữ liệu đã làm sạch (116,875 dòng)
│   └── README.md
├── notebooks/             # Phân tích dữ liệu
│   ├── preprocessing/     # Tiền xử lý dữ liệu (Python/Pandas)
│   └── eda/               # Phân tích khám phá (EDA)
├── powerbi/               # Trực quan hóa Power BI
│   ├── Housing_Price_Analysis_2021_2025.pbix
│   └── README.md
└── README.md              # ← File hiện tại
```

| Phân khu | Mô tả | Liên kết |
|---|---|---|
| **Dữ liệu** | Cấu trúc dữ liệu raw/processed, data dictionary | [`data/`](./data/) |
| **Web Crawling** | Kiến trúc crawler, kỹ thuật thu thập | [`crawler/`](./crawler/) |
| **Tiền xử lý** | Pipeline làm sạch và chuẩn hóa dữ liệu | [`notebooks/preprocessing/`](./notebooks/preprocessing/) |
| **EDA** | Phân tích khám phá, thống kê mô tả, tương quan | [`notebooks/eda/`](./notebooks/eda/) |
| **Power BI** | Dashboard trực quan hóa, phân tích phân khúc | [`powerbi/`](./powerbi/) |
