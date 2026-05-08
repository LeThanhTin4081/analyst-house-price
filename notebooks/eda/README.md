# Exploratory Data Analysis — Thị trường BĐS TP.HCM

Notebook chính: [eda_bds_hcm.ipynb](./eda_bds_hcm.ipynb)

## Mục tiêu

Notebook này thực hiện phân tích khám phá (EDA) trên tập dữ liệu BĐS TP.HCM đã qua tiền xử lý, nhằm:

- Hiểu phân phối và đặc trưng thống kê của các biến chính (Giá, Diện tích, Giá/m²).
- Nhận diện các vấn đề chất lượng dữ liệu còn tồn đọng (outlier, missing ẩn dưới giá trị 0, thiên lệch thời gian).
- Rút ra các insight về thị trường để phục vụ cho giai đoạn trực quan hóa trên Power BI.

## Dữ liệu đầu vào

- **File:** `data/processed/fulldata_clean_4web.csv`
- **Shape:** 116,863 dòng × 19 cột
- Dữ liệu đã được làm sạch và chuẩn hóa ở bước Preprocessing.

## Các phần phân tích trong notebook

### 1. Thiết lập môi trường và khảo sát dữ liệu

- Import thư viện: `pandas`, `numpy`, `matplotlib`, `seaborn`.
- Đọc dữ liệu và kiểm tra kiểu dữ liệu.

### 2. Thống kê mô tả các biến số chính

Tính toán mean, median, min, max, std và tứ phân vị cho 3 cột: `Gia`, `DienTich`, `Gia_m2`.

**Phát hiện chính:**

- Phân phối lệch phải rõ rệt: trung bình Giá (~17 tỷ) cao hơn nhiều so với trung vị (~7.5 tỷ), cho thấy một số ít BĐS giá trị rất cao đã kéo lệch trung bình.
- Độ lệch chuẩn của Giá (~71.7 tỷ) gấp hơn 4 lần trung bình, phản ánh sự phân hóa cực lớn giữa các phân khúc.
- Vùng min/max vẫn còn dấu hiệu dữ liệu nhiễu (giá trị phi thực tế ở cả hai đầu).

### 3. Phân tích phân phối bằng Histogram

Vẽ histogram (thang log) cho 3 biến: `Gia`, `DienTich`, `Gia_m2`.

**Phát hiện chính:**

- **Giá:** Đỉnh phân phối tập trung ở khoảng 7–10 tỷ VNĐ, phản ánh đúng tầm giá giao dịch phổ biến nhất. Đuôi phải kéo dài cho thấy sự tồn tại của BĐS siêu sang với giá trị rất cao.
- **Diện tích:** Phần lớn nguồn cung nằm trong khoảng 50–100 m², phù hợp với đặc thù nhà phố và căn hộ tại đô thị lớn. Đuôi phải kéo dài đến hàng nghìn m² phản ánh phân khúc đất nền, kho bãi vùng ven.
- **Giá/m²:** Trung vị khoảng 100 triệu/m². Vẫn tồn tại một số giá trị đơn giá phi lý, chủ yếu thuộc nhóm `Loại BĐS = Không xác định`.

### 4. Phân bố các biến kiến trúc

Vẽ countplot cho: `SoTang`, `SoPhongNgu`, `SoPhongVeSinh`, `SoPhongTam`, `ChoDeXe`.

**Phát hiện chính:**

- Số tầng, phòng ngủ và phòng vệ sinh đều tập trung ở các mốc 2, 3, 4 — phù hợp với cấu trúc nhà phố và căn hộ phổ biến tại TP.HCM.
- `SoPhongTam` có lượng dữ liệu rất thấp so với `SoPhongVeSinh`, do đặc thù crawl dữ liệu thiếu trường này ở nhiều nguồn.
- `ChoDeXe = 0` chiếm tỷ lệ áp đảo (~90%), phản ánh thói quen đăng tin thiếu chi tiết hoặc đặc thù nhà hẻm không có bãi đỗ riêng.

### 5. Phân tích biến phân loại

Thống kê tần suất và trực quan hóa cho: `QuanHuyen`, `Nguon`, `LoaiBDS`.

**Phát hiện chính:**

- Nhà phố chiếm tỷ trọng lớn nhất trong nguồn cung, đặc biệt tại các quận Tân Bình, Gò Vấp, Bình Tân.
- Căn hộ có tỷ trọng đáng kể tại Quận 7, phù hợp với đặc thù phát triển đô thị khu vực Phú Mỹ Hưng.
- Dữ liệu phân bố không đều giữa các nguồn: `batdongsan` và `nhadat24h` đóng góp phần lớn số lượng tin.

### 6. Phân tích chuỗi thời gian

Khảo sát xu hướng số lượng tin đăng theo thời gian.

**Phát hiện chính:**

- Số lượng tin đăng tăng mạnh vào năm 2025. Đây không phản ánh xu hướng thị trường thực tế mà là hiệu ứng thời điểm crawl — các tin cũ (2021–2023) đã bị website gỡ bỏ, chỉ còn lại tin gần thời điểm thu thập.
- Kết luận: Biến `NgayDang` trong tập dữ liệu này không phù hợp để phân tích xu hướng giá theo thời gian hoặc xây dựng mô hình Time Series.

### 7. Ma trận tương quan và Boxplot theo khu vực

- Ma trận tương quan cho thấy `DienTich` là biến có tương quan cao nhất với `Gia`. Các biến kiến trúc khác (`SoPhongNgu`, `SoTang`, `MatTien`...) bị ảnh hưởng bởi lượng missing lớn (đã fill 0), dẫn đến tương quan thấp hơn thực tế.
- Boxplot Giá trung vị theo quận cho thấy sự phân cực rõ rệt: Quận 1 dẫn đầu (~33 tỷ/căn), giảm dần qua các quận trung tâm, và thấp nhất ở các huyện ngoại thành Bình Chánh, Hóc Môn (~3.5 tỷ/căn).
- So sánh Mean vs Median cho thấy tại một số quận (Quận 3, Bình Thạnh), trung bình bị kéo cao đáng kể bởi một số ít BĐS giá trị rất lớn, khiến Mean không phải thước đo phù hợp cho mặt bằng chung.

## Kết luận và định hướng

EDA đã giúp nhận diện các đặc điểm quan trọng của tập dữ liệu:

1. **Phân phối lệch phải:** Giá, Diện tích và Giá/m² đều có phân phối Right-Skewed với nhiều giá trị ngoại lai cực đoan.
2. **Dữ liệu thiếu:** Nhiều biến kiến trúc có tỷ lệ missing cao (đã fill 0), ảnh hưởng đến chất lượng phân tích tương quan.
3. **Thiên lệch thời gian:** Biến `NgayDang` phản ánh thời điểm crawl hơn là vòng đời thực của thị trường.
4. **Giá trị ngoại lai:** Nhóm `Loại BĐS = Không xác định` là nguồn chính của các giá trị phi lý còn sót lại sau bước tiền xử lý.

Các phát hiện này được sử dụng làm cơ sở để thiết kế dashboard phân tích trên Power BI ở giai đoạn tiếp theo.
