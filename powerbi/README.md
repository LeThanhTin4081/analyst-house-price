# 📊 Trực quan hóa dữ liệu trên Power BI

> 🔗 **Trải nghiệm trực tiếp:** [Báo cáo Power BI Public](https://app.powerbi.com/view?r=eyJrIjoiYTE3OWVkZWMtYzMzZi00N2IwLWE4MDMtOTdhNTQzNzM4YWQ4IiwidCI6ImVkOGYxNjczLTM4OTAtNGRiNC1hM2YwLTk3YWQ5NDI3Yzc0ZiIsImMiOjEwfQ%3D%3D&disablecdnExpiration=1778348335)


## 📑 Cấu trúc thiết kế: Single-page Interactive Dashboard

Thay vì thiết kế báo cáo nhiều trang rời rạc, tôi đã xây dựng hệ thống dưới dạng **Dashboard tương tác 1 trang duy nhất**. Bằng cách sử dụng hệ thống **Slicer (Bộ lọc)** và tính năng **Cross-filtering (Tương tác chéo)**, toàn bộ sức mạnh phân tích được nén vào một màn hình.

Người dùng chỉ cần thao tác click vào **Năm (2021-2025)** hoặc **Loại BĐS**, toàn bộ biểu đồ, KPI và Boxplot sẽ lập tức "nhảy" dữ liệu theo thời gian thực để đi sâu vào từng phân khúc.

---

## 🖼️ 1. Góc nhìn Tổng quan (Khi Slicer chọn "All")

![Góc nhìn Tổng quan](https://drive.google.com/uc?export=view&id=16wq-9_jMWz8XIvuCMrOr5zDs3XbBXSer)

### KPI Cards (Chỉ số tổng lượng)
| Chỉ số | Giá trị |
|---|---|
| Tổng số BĐS rao bán | **116.1K** |
| Giá trung bình | **15.70 Tỷ VNĐ** |
| Giá cao nhất (Max) | **850 Tỷ VNĐ** |
> **🔑 Insight:** Khoảng cách khổng lồ giữa giá trung bình (15.70 tỷ) và giá cao nhất (850 tỷ) khẳng định thị trường TP.HCM bị kéo lệch mạnh bởi các BĐS siêu sang và giá trị ngoại lai (Outliers).
### Phân tích Vị trí (Location)
- **Treemap & Bar Chart:** Quận 1 dẫn đầu tuyệt đối về giá trị trung bình (~54.26 tỷ), tiếp đến là Quận 3 (~39.25 tỷ) và Quận 5 (~25.12 tỷ). Vị trí lõi trung tâm chính là thỏi nam châm quyết định toàn bộ giá trị BĐS, lấn át hoàn toàn các khu vực vùng ven.
### Phân tích theo Loại hình BĐS (Property Type)
- **Biểu đồ Cột (Column Chart):** So sánh đơn giá trung bình/m². Nhóm **"Không xác định"** (chưa phân loại) đội giá cao nhất (~238.8 triệu/m²). Xét trên các nhóm chuẩn, **Biệt thự** (~177.8 triệu/m²) và **Nhà phố** (~152.2 triệu/m²) là 2 phân khúc đắt đỏ nhất. Phân khúc **Căn hộ** (~73.5 triệu/m²) và **Đất** (~70.4 triệu/m²) ở mức giá sàn ngang bằng nhau.
- **Biểu đồ Hộp (Box Plot):** Bóc tách độ phân tán giá trong từng loại hình (ảnh minh họa đang filter xem Box Plot của riêng nhóm Biệt thự), công cụ đắc lực giúp loại bỏ "ảo giác" của phép tính Trung bình cộng.
### Biến động theo Dòng thời gian (Time-Series)
- **Biểu đồ Đường (Line Chart 2021-2025):** Trực quan hóa toàn bộ dòng thời gian 5 năm. Đường xu hướng cho thấy thị trường không hề bằng phẳng mà biến động với Volatility (độ dao động) rất cao. Biểu đồ liên tục xuất hiện các "đỉnh nhọn" (Spikes) giật lên mốc 50–100 tỷ, phơi bày các đợt sốt giá cục bộ hoặc các thời điểm mà rổ hàng siêu sang đồng loạt được tung ra thị trường.

---

## ⏳ 2. Khám phá biến động qua các năm (Slicer 2021 - 2025)
*(Hệ thống tự động render lại dữ liệu khi chọn Năm trên thanh Slicer)*

### 📍 Năm 2021 (Giai đoạn khởi điểm)
![Dashboard 2021](https://drive.google.com/uc?export=view&id=1Yjp6iZrTlECnKxLStGAs-MqSqwzF8SAT)
- **KPI:** Ghi nhận **1,456** BĐS. Giá trung bình **9.61 tỷ**, cao nhất **300 tỷ**.
- **Vị trí & Loại hình:** Quận 1 (~41 tỷ) và Quận 3 (~33.3 tỷ) giữ ngôi vương trên Treemap. Về loại hình, **Biệt thự** vọt lên mốc rất cao (~159.8 triệu/m²), theo sau là Nhà phố (~121.7 triệu/m²). Căn hộ và Đất nền giao dịch ở mốc khá rẻ (chỉ xấp xỉ 37-41 triệu/m²).
- **Thời gian:** Biểu đồ đường thưa thớt (do dữ liệu thu thập bắt đầu từ giữa năm), nhưng ghi nhận một đợt sốt giá (spike) cực kỳ dị biệt vào khoảng tháng 8/2021 giật lên mức 100 tỷ.

### 📍 Năm 2022 (Sự bùng nổ nguồn cung, giảm giá bán)
![Dashboard 2022](https://drive.google.com/uc?export=view&id=1aSnOmxenwSbIEsOPkx8IRu9klHk4FB--)
- **KPI:** Nguồn cung tăng gấp 4 lần lên **6,316** BĐS. Tuy nhiên, giá trung bình lại giảm xuống **7.83 tỷ**. Đỉnh giá đạt **470 tỷ**.
- **Vị trí & Loại hình:** Giá trung bình tại Quận 1 hạ nhiệt xuống còn ~29.6 tỷ. Biệt thự và Nhà phố vẫn dẫn đầu đơn giá, nhưng Căn hộ đã rục rịch tăng giá lên mức ~52 triệu/m².
- **Thời gian:** Line Chart biến động cực kỳ dữ dội từ tháng 6 đến tháng 12, báo hiệu dòng tiền thị trường luân chuyển rất mạnh. Cuối tháng 12 xuất hiện cú giật giá đột biến.

### 📍 Năm 2023 (Chạm đáy & Thanh lọc)
![Dashboard 2023](https://drive.google.com/uc?export=view&id=1vIrLzZpg2vop38wM5EvsdFyDYB5MCARi)
- **KPI:** Nguồn cung giảm nhẹ còn **4,967** BĐS. Giá trung bình chạm đáy ở mức **6.65 tỷ** (thấp nhất trong chu kỳ 5 năm).
- **Vị trí & Loại hình:** Sự suy giảm diện rộng thể hiện rõ khi Quận 1 rớt xuống chỉ còn ~18.6 tỷ/BĐS. Mức giá/m² của "Biệt thự" giảm sâu xuống ~147 triệu/m². 
- **Thời gian:** Đường xu hướng đi ngang ở vùng đáy (low volume), thỉnh thoảng mới xuất hiện các giao dịch giá cao (spikes ~40-50 tỷ vào tháng 8 và tháng 11). Sự ách tắc thanh khoản thể hiện rõ rệt.

### 📍 Năm 2024 (Dòng tiền quay trở lại)
![Dashboard 2024](https://drive.google.com/uc?export=view&id=1HSgflUKA8BmCyNJ3n0bCxMxOCAHTrzI5)
- **KPI:** Nguồn cung bắt đầu tăng vọt, đạt **11.1K** BĐS. Giá trung bình phục hồi lên mức **9.32 tỷ**. Giá cao nhất thiết lập mốc mới **800 tỷ**.
- **Vị trí & Loại hình:** Việc xuất hiện các tài sản siêu sang kéo theo khu vực lõi Quận 1 vọt lên lại mức ~33.7 tỷ/BĐS. Nhóm Biệt thự phục hồi mạnh mẽ (~188.2 triệu/m²), kéo theo sự nóng lên của Đất nền (~78.2 triệu/m²).
- **Thời gian:** Mật độ Line Chart dày đặc hơn hẳn 2023. Giao dịch diễn ra đều đặn, tần suất xuất hiện các đợt giật giá cục bộ cao hơn, cho thấy phân khúc cao cấp đã rục rịch giao dịch.

### 📍 Năm 2025 (Bùng nổ Nguồn cung & Giá)
![Dashboard 2025](https://drive.google.com/uc?export=view&id=1ye5iheNi8FR3NI8oAXone59vkx61BKDL)
- **KPI:** Nguồn cung bùng nổ chưa từng có với **92.2K** BĐS (chiếm phần lớn tổng tập dữ liệu). Giá trung bình vọt lên **17.59 tỷ**, mức cao kỷ lục.
- **Vị trí & Loại hình:** Treemap cho thấy Quận 1 (~56.7 tỷ) và Quận 3 (~45.3 tỷ) kéo giãn khoảng cách tuyệt đối so với phần còn lại. Nhóm BĐS "Không xác định" và "Biệt thự" thống trị đơn giá.
- **Thời gian:** Đường Line Chart cực kỳ đặc đặc và gai góc. Biên độ dao động (Volatility) đạt mức cực đại, minh chứng cho một thị trường hoạt động với cường độ cao nhất.

---

## 🔍 3. Phá vỡ định kiến "Giá Trung Bình" qua từng Phân Khúc

| Phân khúc | Tỷ trọng (Nguồn cung) | Giá Trung Bình / Max | Mean/m² | Median/m² | Chênh lệch | Insight cốt lõi (Phá vỡ định kiến) |
|---|---|---|---|---|---|---|
| **Biệt thự** | 4,954 | 41.07 tỷ / 400 tỷ | **178 tr** | **75 tr** | **× 2.4** | Phân hóa mạnh (Max gấp 10 lần TB). Khoảng cách lớn giữa biệt thự liền kề và đơn lập siêu sang tại Q1, Q3. |
| **Căn hộ** | 11.62K | 7.06 tỷ / 150 tỷ | **73 tr** | **28 tr** | **× 2.6** | Giá TB bị kéo lệch bởi Penthouse. Thực tế 50% căn hộ chỉ dao động ở mức "vừa túi tiền" 22–35 triệu/m². |
| **Nhà phố** | 65.66K *(Áp đảo)* | 16.30 tỷ / 850 tỷ | **152 tr** | **25 tr** | **× 6.0** | Phân khúc rộng nhất. Đơn giá trung bình bị thổi phồng bởi nhà mặt tiền các quận lõi trung tâm. |
| **Đất nền** | 22.35K | 10.82 tỷ / 700 tỷ | **70 tr** | **5 tr** | **× 14.0** | **Cú sốc dữ liệu:** Outlier đất dự án trung tâm bóp méo hoàn toàn thực tế 50% lô đất nền chỉ có giá ~5 triệu/m². |

---

## 🛠️ 4. Kỹ thuật Power BI đã áp dụng

- **Single-page Layout:** Nén khối lượng dữ liệu khổng lồ vào 1 trang duy nhất mà không gây nghẽn UI.
- **DAX Measures:** Xây dựng hệ thống logic hàm để linh hoạt tính toán Mean, Median, Max và Đơn giá/m² dựa trên ngữ cảnh bộ lọc.
- **Data Modeling (Star Schema):** Thiết lập cấu trúc quan hệ chuẩn xác để Slicer "Loại BĐS" và Slicer "Năm" có thể lan truyền bộ lọc mượt mà qua mọi visual.
- **Tư duy phân tích kép:** Cố ý xếp cạnh nhau Biểu đồ Cột (Mean) và Box Plot (Median) để chứng minh cho người xem thấy "Bẫy thống kê" trong phân tích dữ liệu BĐS.
