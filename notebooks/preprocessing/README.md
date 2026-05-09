# 📂 Tiền xử lý dữ liệu BĐS TP.HCM

> **Xem chi tiết toàn bộ Source Code và thuật toán xử lý dữ liệu tại Notebook:** [`preprocessing_bds_hcm.ipynb`](./preprocessing_bds_hcm.ipynb)


## 🔑 Notebook này làm gì

- Đọc dữ liệu thô từ `data/raw/fulldata_raw_4web.csv`.
- Làm sạch và chuẩn hóa dữ liệu bất động sản TP.HCM lấy từ 4 nguồn: `batdongsan`, `alonhadat`, `meeyland`, `nhadat24h`.
- Xử lý thiếu dữ liệu, chuẩn hóa địa chỉ, chuẩn hóa loại BĐS, kiểm tra và loại bỏ outliers.
- Xuất dữ liệu cuối cùng ra dữ liệu đã qua xử lý `data/processed/fulldata_clean_4web.csv`.

## 💡 12 phần chính đã thực hiện trong notebook

### 1. Làm sạch và khảo sát ban đầu

Notebook mở đầu bằng phần đọc dữ liệu và kiểm tra nhanh dữ liệu thô:

- Import các thư viện cần dùng: `pandas`, `numpy`, `re`, `unicodedata`, `matplotlib`, `seaborn`, `Path`.
- Đọc file `fulldata_raw_4web.csv`.
- Khảo sát ban đầu dữ liệu: xem cột, kiểu dữ liệu, và mẫu dữ liệu.
- Kiểm tra kiểu dữ liệu và ghi nhận:
  - Các cột đã đúng định dạng số: `Số phòng ngủ`, `Số phòng vệ sinh`, `Số phòng tắm`.
  - Các cột quan trọng đang ở dạng `object` cần chuẩn hóa: `Giá`, `Diện tích`, `Giá/m2`, `Số tầng`, `Mặt tiền`, `Đường vào`.
  - Các cột cần xử lý riêng:
    - `Ngày đăng`: chuyển sang `datetime`.
    - `Kích thước`: tách thành `Chiều rộng`, `Chiều dài`.
    - `Địa chỉ`: tách để lấy `Quận/Huyện`.
    - `Loại BĐS`: chuẩn hóa và suy luận thêm từ `Tiêu đề`.
- Kiểm tra dữ liệu trùng lặp:
  - Notebook kết luận không có dòng nào bị trùng lặp hoàn toàn.
- Phân tích giá trị thiếu trên toàn bộ dữ liệu:
  - Gần như không thiếu ở các cột cốt lõi: `Tiêu đề`, `Giá`, `Diện tích`, `Địa chỉ`, `Ngày đăng`, `Nguồn`.
  - Thiếu rất nặng ở các đặc trưng kiến trúc chi tiết (thiếu >50%):

| Thuộc tính (Feature) | Tỷ lệ rỗng (Missing %) | Đánh giá & Hướng xử lý (Data Science POV) |
|---|---|---|
| **Số phòng tắm** | 96.88% | Mức rỗng quá lớn. Tiến hành điền khuyết (Impute) sẽ gây nhiễu nặng. |
| **Kích thước** | 90.46% | Đề xuất Drop hoặc tạo Missing Indicators trong pha Modeling. |
| **Mặt tiền** | 76.59% | Như trên. |
| **Số tầng** | 75.46% | Như trên. |
| **Đường vào** | 64.51% | Như trên. |
| **Chỗ để xe** | 52.64% | Như trên. |
| **Giá/m2** | ~51% | Có thể tái tạo tự động bằng phép chia `Giá / Diện tích`. Khắc phục thành công. |

- Phân tích giá trị thiếu theo từng nguồn (Nhiều cột bị thiếu 100%):

| Nguồn dữ liệu | Các đặc trưng bị thiếu hụt nghiêm trọng (Missing = 100%) |
|---|---|
| `batdongsan` | Số phòng tắm, Số tầng, Chỗ để xe, Kích thước, Mặt tiền, Đường vào, Loại BĐS |
| `alonhadat` | Giá/m2, Số phòng vệ sinh, Số phòng tắm, Mặt tiền, Loại BĐS |
| `meeyland` | Số phòng vệ sinh, Số tầng, Chỗ để xe, Kích thước, Mặt tiền, Đường vào, Loại BĐS |
| `nhadat24h` | Giá/m2, Số phòng tắm, Kích thước |

### 2. Xây dựng các hàm tiền xử lý

Notebook tự xây các hàm để chuẩn hóa dữ liệu thô về đúng kiểu dữ liệu. Nhóm các hàm xử lý cốt lõi bao gồm:

| Tên hàm (Function) | Nhóm xử lý | Chức năng chi tiết |
|---|---|---|
| `parse_number` | Số & Số đo | Đọc chuỗi số, phân biệt linh hoạt dấu phân tách hàng nghìn (`,`) và dấu thập phân (`.`) theo ngữ cảnh. |
| `clean_gia` | Số & Số đo | Chuẩn hóa giá về VND. Quy đổi các đơn vị `tỷ`, `triệu`, `k`. Xử lý hoàn hảo các mẫu khó như `11,787 tỷ` hay `3 tỷ 386 triệu`. Trả về `None` với giá `thỏa thuận`. |
| `clean_dien_tich` | Số & Số đo | Đưa diện tích về số thực (`float`), tự động quy đổi `ha` sang `m²`. |
| `tach_kich_thuoc` | Số & Số đo | Tách chuỗi `Rộng x Dài` thành 2 cột số độc lập: `ChieuRong` và `ChieuDai`. |
| `clean_so` | Số nguyên | Trích xuất số nguyên đầu tiên từ text (Dùng cho `Số tầng`, `Số phòng...`). |
| `clean_chodexe` | Categorical | Chuẩn hóa mảng text thành bộ mã hóa số: `1` (Có), `0` (Không), `None` (CXD). |

- **Nhóm hàm chuẩn hóa địa chỉ:**
  - Chuẩn hóa chuỗi địa chỉ bằng cách bỏ dấu, đưa về chữ thường, loại ký tự đặc biệt.
  - Tạo danh sách chuẩn `LIST_QUAN` cho các quận/huyện tại TP.HCM.
  - Tạo mapping phường/xã về quận/huyện để hỗ trợ suy luận địa chỉ.
  - Gộp các cách gọi như `Quận 2`, `Quận 9`, `Thủ Đức`, `Thành Phố Thủ Đức` về `Thủ Đức`.
  - Trả về `CXD` nếu không xác định được `Quận/Huyện`.

### 3. Áp dụng các hàm clean cơ bản

Notebook tạo `df_processed` từ dữ liệu thô và tiến hành ép kiểu (Data Casting):

- Ánh xạ tên cột gốc sang tên cột không dấu để xử lý nội bộ (VD: `Giá -> Gia`, `Địa chỉ -> DiaChiRaw`).

| Feature / Nhóm Feature | Kiểu dữ liệu đích | Thao tác chuẩn hóa (Casting) |
|---|---|---|
| `Gia` | Float | Chuyển về số thực với đơn vị quy chiếu chuẩn (VND). |
| `DienTich`, `MatTien`, `DuongVao` | Float | Chuyển đổi sang dạng số thực. |
| `SoTang`, `SoPhong...` | Integer | Trích xuất và lưu dưới dạng số nguyên. |
| `NgayDang` | Datetime | Parse chuỗi `%d/%m/%Y` sang cấu trúc thời gian của Pandas. |
| `ChoDeXe` | Integer / None | Map về tập giá trị boolean cơ bản: `0, 1, None`. |

- Tách `KichThuoc` thành `ChieuRong` và `ChieuDai`.
- Tính lại `Gia_m2` bằng công thức: `Gia / DienTich` (Chỉ tính khi Giá và Diện tích hợp lệ và `DienTich > 0`).

### 4. Chuẩn hóa loại bất động sản

Notebook chuẩn hóa `LoaiBDS` theo hai nguồn thông tin:

- Nếu cột loại gốc có dữ liệu thì làm sạch trước.
- Nếu cột loại gốc thiếu hoặc rơi vào nhóm khó dùng, notebook suy luận thêm từ `Tiêu đề`.
- Các nhóm loại chính được chuẩn hóa trong code: `Nhà`, `Đất`, `Căn hộ`, `Biệt thự`.
- Nếu vẫn không phân loại được, gán: `Không xác định`.

### 5. Áp dụng tách địa chỉ

Notebook áp dụng pipeline tách địa chỉ lên cột `DiaChiRaw`:

- Tạo 3 cột mới: `ThanhPho`, `QuanHuyen`, `DiaChiFull`.
- Nhận diện `ThanhPho = Hồ Chí Minh` nếu trong địa chỉ có các mẫu như: `Hồ Chí Minh`, `TP.HCM`, `tphcm`, `hcm`.
- Suy luận `QuanHuyen` từ:
  - Tên quận/huyện xuất hiện trực tiếp trong địa chỉ.
  - Mapping phường/xã sang quận/huyện đã định nghĩa trong notebook.
- Nếu không xác định được quận/huyện thì gán: `CXD`.

### 6. Xóa dữ liệu thiếu thông tin quan trọng

Notebook loại bỏ các dòng chưa đủ dữ liệu cốt lõi cho phân tích giá:
- Xóa các dòng thiếu `Gia`.
- Xóa các dòng thiếu `DienTich`.
- Xóa các dòng có `QuanHuyen = 'CXD'`.

### 7. Lựa chọn và sắp xếp các cột cuối cùng

Sau khi làm sạch, notebook giữ lại bộ cột cuối cùng cho `df_final`:
`TieuDe`, `Nguon`, `ThanhPho`, `QuanHuyen`, `DiaChiFull`, `NgayDang`, `LoaiBDS`, `Gia`, `DienTich`, `Gia_m2`, `MatTien`, `DuongVao`, `ChieuRong`, `ChieuDai`, `SoTang`, `SoPhongNgu`, `SoPhongVeSinh`, `SoPhongTam`, `ChoDeXe`.

### 8. Xử lý giá trị thiếu còn lại

Notebook tiếp tục xử lý `NaN` còn sót lại trong `df_final`:

- Điền `0` cho các cột số còn thiếu: `MatTien`, `DuongVao`, `ChieuRong`, `ChieuDai`, `SoTang`, `SoPhongNgu`, `SoPhongVeSinh`, `SoPhongTam`, `ChoDeXe`.
- Với `NgayDang`: Nếu còn thiếu thì xóa dòng đó (Ghi nhận đã xóa 43 dòng).
- Nhận xét được ghi ngay trong notebook:
  - Sau bước này, dữ liệu số và thời gian đã sẵn sàng cho bước xử lý outlier.
  - Sau khi điền `0`, dữ liệu chỉ còn thiếu ở `ThanhPho` với 36,231 dòng (~29.68%) do nguồn không ghi rõ `TP.HCM`.
  - **Nhận định:** Việc điền `0` cho các cột số là chiến lược đơn giản để phục vụ EDA/báo cáo; nếu dùng cho modeling thì nên cân nhắc giữ `NaN` và tạo thêm cờ missing.

### 9. Kiểm tra outliers

Notebook kiểm tra ngoại lai trước khi lọc bằng boxplot cho 3 cột: `Gia`, `DienTich`, `Gia_m2`.

Chi tiết cách làm:
- Dùng `boxplot` để xem phân bố dữ liệu.
- Riêng `Gia` được hiển thị theo thang `log` vì khoảng giá quá rộng.
- Ghi nhận trực quan rằng tồn tại nhiều giá trị rất lớn và phi thực tế.

### 10. Loại bỏ outliers

Notebook tạo bản sao `df_before_outlier` rồi lọc outliers trên `df_final` theo từng nhóm:

- Với `Gia`: Lọc theo từng `LoaiBDS` bằng ngưỡng phân vị `0.5% - 99.5%`. Bỏ qua nhóm `Không xác định` và `CXD`. Chỉ xử lý khi nhóm có ít nhất 50 dòng.
- Với `Gia_m2`: Lọc theo từng `LoaiBDS` bằng ngưỡng phân vị `1% - 99%`. Điều kiện tương tự như `Gia`.
- Với `DienTich`: Dùng giới hạn cứng (Hard-limits) theo Loại BĐS để ép dữ liệu về các khoảng hợp lý thực tế:

| Phân khúc BĐS | Diện tích tối thiểu | Diện tích tối đa |
|---|---|---|
| **Căn hộ** | 20 m² | 500 m² |
| **Nhà** | 20 m² | 1000 m² |
| **Biệt thự** | 80 m² | 2000 m² |
| **Đất** | 30 m² | 5000 m² |

*(Các nhóm không có trong bảng giới hạn sẽ được giữ nguyên. Sau mỗi bước đều in ra số lượng dòng bị loại bỏ).*

### 11. Trực quan hóa outliers sau khi xử lý

Sau khi lọc outliers, notebook vẽ lại boxplot. Phần diễn giải Data Science:

- Outlier đã được làm mềm rõ rệt ở các nhóm cốt lõi `Nhà`, `Đất`, `Căn hộ`, `Biệt thự`.
- Sự phân bố của `DienTich` đã sát hình mẫu dữ liệu thực tế (Real-world Data): phần lớn dữ liệu (IQR) nằm gọn trong khoảng vài chục đến vài trăm m². Phần đuôi dài kéo dài ra tối đa chỉ khoảng 3 hecta (30,000 m²) - một quy mô hoàn toàn khả thi cho các dự án nghỉ dưỡng.
- Đối với `Gia` và `Gia_m2`, biểu đồ boxplot vẫn hiển thị nhiều chấm ngoại lai cực viễn vươn xa tới các mốc phi lý (VD: 10.000 tỷ/Căn hay 135 tỷ/m²).
- **Nguyên nhân cốt lõi (Root Cause):** Các sample nhiễu này "sống sót" qua bước filter là do chúng rơi vào nhóm `Loại BĐS = Không xác định` (nhóm được thuật toán chủ động lược trừ khỏi ranh giới cắt Percentile).
- **Đề xuất cho giai đoạn Modeling:** Sự hiện diện của các Outlier cực đoan từ nhóm này sẽ phá vỡ hàm mất mát (Loss Function) của mô hình định giá. Vì vậy, ta có cơ sở để **drop toàn bộ** các bản ghi `Không xác định` ra khỏi tập huấn luyện để bảo vệ chất lượng tín hiệu.

### 12. Xuất kết quả

Notebook xuất dữ liệu đã tiền xử lý bằng bước cuối:
- Tạo thư mục output nếu chưa tồn tại.
- Lưu `df_final` ra file với chuẩn encoding `utf-8-sig`.

## 📌 Kết quả đầu ra của notebook

- Input:
  - `data/raw/fulldata_raw_4web.csv`
- Output:
  - `data/processed/fulldata_clean_4web.csv`
