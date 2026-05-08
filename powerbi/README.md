# Trực quan hóa dữ liệu trên Power BI

File dashboard: [Housing_Price_Analysis_2021_2025.pbix](./Housing_Price_Analysis_2021_2025.pbix)

> Bạn có thể truy cập phiên bản công khai của báo cáo để tương tác trực tiếp với các biểu đồ tại liên kết sau: [Báo cáo Power BI]()

## Cấu trúc Dashboard

Báo cáo gồm **5 trang**, được thiết kế theo hướng đi từ **tổng quan → chi tiết từng phân khúc**:

| Trang | Nội dung |
|---|---|
| **Tổng quan** | Bức tranh toàn cảnh thị trường BĐS TP.HCM 2021–2025 |
| **Biệt thự** | Phân tích chuyên sâu phân khúc hạng sang |
| **Căn hộ** | Phân tích phân khúc căn hộ chung cư |
| **Đất** | Phân tích phân khúc đất nền, đất thổ cư |
| **Nhà** | Phân tích phân khúc nhà phố — chiếm tỷ trọng lớn nhất |

Mỗi trang đều tích hợp **Year Filter (2021–2025)** để so sánh biến động theo thời gian, cùng hệ thống **Slicer** cho phép tương tác chéo giữa các biểu đồ.

---

## Trang 1: Tổng quan thị trường

<!-- ![Tổng quan](./screenshots/tong_quan.png) -->

### KPI Cards

| Chỉ số | Giá trị |
|---|---|
| Tổng số tin đăng | **116.08K** |
| Giá trung bình | **15.70 tỷ** |
| Giá cao nhất | **850 tỷ** |

**Insight:** Khoảng cách cực lớn giữa giá trung bình (15.70 tỷ) và giá cao nhất (850 tỷ) cho thấy thị trường bị lệch rất mạnh bởi sự tồn tại của các BĐS phân khúc siêu sang và các giá trị ngoại lai.

### Phân tích theo vị trí địa lý

- **Biểu đồ thanh ngang (Clustered Bar Chart):** Xếp hạng giá trung bình theo quận/huyện. Quận 1 dẫn đầu tuyệt đối (~54 tỷ), tiếp theo là Quận 3 (~39 tỷ), Quận 5 (~25 tỷ) và Quận 10 (~20 tỷ). Cuối bảng là Hóc Môn (~7 tỷ), Bình Chánh (~5 tỷ), Củ Chi (~4 tỷ).
- **Treemap:** Bổ sung trực quan hóa, giúp nhận diện nhanh tỷ trọng giá trị của từng khu vực.

**Insight:** Cả hai biểu đồ xác nhận **vị trí (đặc biệt các quận trung tâm) là yếu tố hàng đầu quyết định giá trị BĐS** tại TP.HCM.

### Biến động giá theo thời gian

- **Biểu đồ đường (Line Chart):** Trực quan hóa biến động giá trung bình từ 2022 đến 2025.

**Insight:** Biểu đồ cho thấy sự biến động (volatility) rất cao, với nhiều "đỉnh nhọn" lên đến 50–100 tỷ. Điều này gợi ý có nhiều đợt sốt giá cục bộ, hoặc do dữ liệu đầu vào ở một số thời điểm chứa nhiều BĐS giá trị cao bất thường.

### Phân tích theo loại hình BĐS

- **Biểu đồ cột (Clustered Column Chart):** So sánh đơn giá trung bình/m² giữa các loại BĐS. Biệt thự (~180 triệu/m²) và Nhà (~152 triệu/m²) dẫn đầu. Căn hộ (~73 triệu/m²) và Đất (~70 triệu/m²) có mức giá tương đương nhau.
- **Biểu đồ hộp (Box Plot):** Phân tích sâu sự phân bố giá trong từng loại hình.

---

## Trang 2: Phân khúc Biệt thự

<!-- ![Biệt thự](./screenshots/biet_thu.png) -->

| Chỉ số | Giá trị |
|---|---|
| Số tin đăng | **4,954** |
| Giá trung bình | **41.07 tỷ** |
| Giá cao nhất | **400 tỷ** |

**Insight:** Ngay cả trong cùng phân khúc Biệt thự, sự chênh lệch giá gấp 10 lần giữa trung bình và giá cao nhất cho thấy sự phân hóa cực kỳ mạnh mẽ — từ biệt thự liền kề bình dân đến biệt thự đơn lập siêu sang.

- **Vị trí:** Quận 1 dẫn đầu (giá trung bình ~125 tỷ/căn), Quận 3 (~85 tỷ/căn), theo sau là Quận 5, Quận 10, Bình Thạnh.

---

## Trang 3: Phân khúc Căn hộ

<!-- ![Căn hộ](./screenshots/can_ho.png) -->

| Chỉ số | Giá trị |
|---|---|
| Số tin đăng | **11.62K** |
| Giá trung bình | **7.06 tỷ** |
| Giá cao nhất | **150 tỷ** |

**Insight:** Dù thường được xem là phân khúc ổn định, thị trường căn hộ vẫn có sự chênh lệch giá rất lớn (~21 lần) giữa trung bình và giá cao nhất, phản ánh sự phân hóa mạnh mẽ giữa căn hộ bình dân, cao cấp và penthouse.

- **Vị trí:** Quận 1 dẫn đầu (~15 tỷ/căn), Bình Thạnh (~10 tỷ), Quận 3 (~9 tỷ), Thủ Đức (~8 tỷ).
- **Mean vs Median:** Giá trung bình/m² là 73 triệu, nhưng Box Plot cho thấy 50% căn hộ có giá/m² tập trung trong khoảng 22–35 triệu, với trung vị chỉ ~28 triệu/m². **Giá trung bình đang bị các căn hộ cao cấp/penthouse kéo cao gấp gần 3 lần so với giá trị điển hình.**

---

## Trang 4: Phân khúc Đất

<!-- ![Đất](./screenshots/dat.png) -->

| Chỉ số | Giá trị |
|---|---|
| Số tin đăng | **22.35K** |
| Giá trung bình | **10.82 tỷ** |
| Giá cao nhất | **700 tỷ** |

**Insight:** Đây là phân khúc có sự chênh lệch giá lớn nhất (~65 lần), phản ánh sự đa dạng cực lớn về diện tích và vị trí (đất nền dự án, đất thổ cư trung tâm, đất nông nghiệp).

- **Vị trí:** Quận 1 dẫn đầu tuyệt đối (~74 tỷ/lô).
- **Mean vs Median:** Đây là phát hiện gây sốc nhất — Giá trung bình/m² là 70 triệu, nhưng 50% số lô đất có giá/m² chỉ trong khoảng 4–6.8 triệu, trung vị chỉ ~5 triệu/m². **Giá trung bình cao gấp 14 lần giá trung vị** — bằng chứng rõ ràng nhất về việc outlier bóp méo chỉ số trung bình.

---

## Trang 5: Phân khúc Nhà

<!-- ![Nhà](./screenshots/nha.png) -->

| Chỉ số | Giá trị |
|---|---|
| Số tin đăng | **65.66K** (chiếm tỷ trọng lớn nhất) |
| Giá trung bình | **16.30 tỷ** |
| Giá cao nhất | **850 tỷ** |

**Insight:** "Nhà" là danh mục rất rộng, bao gồm từ nhà trong hẻm, nhà mặt tiền đến các dinh thự — thậm chí có thể chứa các BĐS bị phân loại nhầm từ danh mục khác. Sự chênh lệch giá gấp ~52 lần giữa trung bình và giá cao nhất phản ánh tính đa dạng này.

- **Vị trí:** Quận 1 (~51 tỷ/căn) và Quận 3 (~38 tỷ/căn) bỏ xa phần còn lại.
- **Mean vs Median:** Giá trung bình/m² là 152 triệu, nhưng 50% số căn nhà có giá/m² trong khoảng 19–31 triệu, trung vị chỉ ~25 triệu/m². **Giá trung bình cao gấp 6 lần giá trung vị.**

---

## Phát hiện quan trọng nhất: Mean vs Median

Phân tích quan trọng nhất của dashboard đến từ việc kết hợp **Biểu đồ Cột** (giá trung bình) và **Biểu đồ Hộp** (giá trung vị) trên cùng một trang.

| Phân khúc | Mean/m² | Median/m² | Tỷ lệ chênh lệch |
|---|---|---|---|
| **Đất** | 70 triệu | 5 triệu | **×14** |
| **Nhà** | 152 triệu | 25 triệu | **×6** |
| **Căn hộ** | 73 triệu | 28 triệu | **×2.6** |

**Kết luận:** Việc chỉ sử dụng "Giá trung bình" (Mean) là cực kỳ sai lệch do ảnh hưởng của các giá trị ngoại lai. Box Plot là công cụ bắt buộc để hiểu đúng giá trị "điển hình" của một BĐS.

## Các yếu tố ảnh hưởng giá được xác nhận

Dashboard đã xác nhận một cách trực quan các yếu tố ảnh hưởng chính đến giá:

- **Vị trí (QuanHuyen):** Yếu tố hàng đầu quyết định giá trị, với Quận 1 và Quận 3 luôn dẫn đầu trong mọi phân khúc.
- **Loại hình (LoaiBDS):** Mỗi loại hình có phân bố giá hoàn toàn khác nhau, với Biệt thự và Nhà có giá trị/m² cao nhất.

## Kỹ thuật Power BI đã sử dụng

- **DAX Measures:** Xây dựng hệ thống measure tính toán giá trung bình, trung vị, giá cao nhất và đơn giá/m² để hỗ trợ phân tích đa chiều.
- **Data Modeling:** Thiết lập mối quan hệ giữa các bảng dữ liệu, đảm bảo bộ lọc Year và Loại BĐS đồng bộ trên toàn bộ biểu đồ.
- **Custom Visualization:** Kết hợp Treemap, Bar Chart, Line Chart, Column Chart và Box Plot trên cùng một trang để cung cấp góc nhìn đa chiều.
- **Interactive Slicers:** Year Filter (2021–2025) và Loại BĐS Slicer cho phép người dùng tự khám phá dữ liệu theo nhu cầu.
- **Cross-filtering:** Tính năng tương tác chéo giữa các biểu đồ cho phép lọc và phân tích chuyên sâu vào từng phân khúc cụ thể.
