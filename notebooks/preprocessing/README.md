# Tiền xử lý dữ liệu BĐS TP.HCM

Notebook chính: [preprocessing_bds_hcm.ipynb](./preprocessing_bds_hcm.ipynb)

## Notebook này làm gì

- Đọc dữ liệu thô từ `data/raw/fulldata_raw_4web.csv`.
- Làm sạch và chuẩn hóa dữ liệu bất động sản TP.HCM lấy từ 4 nguồn: `batdongsan`, `alonhadat`, `meeyland`, `nhadat24h`.
- Xử lý thiếu dữ liệu, chuẩn hóa địa chỉ, chuẩn hóa loại BĐS, kiểm tra và loại bỏ outliers.
- Xuất dữ liệu cuối cùng ra `data/processed/fulldata_clean_4web.csv`.

## 12 phần chính đã thực hiện trong notebook

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
- Phân tích giá trị thiếu toàn bộ dữ liệu:
  - Gần như không thiếu ở các cột: `Tiêu đề`, `Giá`, `Diện tích`, `Địa chỉ`, `Ngày đăng`, `Nguồn`.
  - Thiếu rất ít nhưng không phải bằng 0 ở: `Ngày đăng` (12 dòng), `Diện tích` (1 dòng), `Địa chỉ` (1 dòng).
  - Thiếu rất nặng ở các cột: `Số phòng tắm` (96.88%), `Kích thước` (90.46%), `Mặt tiền` (76.59%), `Số tầng` (75.46%), `Loại BĐS` (75.14%), `Đường vào` (64.51%).
  - Thiếu đáng chú ý ở các cột: `Chỗ để xe` (52.64%), `Giá/m2` (51.23%), `Số phòng vệ sinh` (48.48%), `Số phòng ngủ` (44.78%).
  - `Giá/m2` có thể tính lại từ `Giá / Diện tích`, nhưng chỉ đúng với các dòng mà `Giá` đã được chuẩn hóa đúng sang số.
- Phân tích giá trị thiếu theo từng nguồn:
  - `batdongsan`: thiếu hoàn toàn `Số phòng tắm`, `Số tầng`, `Chỗ để xe`, `Kích thước`, `Mặt tiền`, `Đường vào`, `Loại BĐS`.
  - `alonhadat`: thiếu hoàn toàn `Giá/m2`, `Số phòng vệ sinh`, `Số phòng tắm`, `Mặt tiền`, `Loại BĐS`.
  - `meeyland`: thiếu hoàn toàn `Số phòng vệ sinh`, `Số tầng`, `Chỗ để xe`, `Kích thước`, `Mặt tiền`, `Đường vào`, `Loại BĐS`; ngoài ra `Số phòng tắm` còn thiếu 45.41%.
  - `nhadat24h`: đầy đủ hơn các nguồn khác nhưng vẫn thiếu nhiều ở `Giá/m2` (100%), `Số phòng tắm` (100%), `Kích thước` (100%), `Số phòng ngủ` (69.48%), `Số tầng` (59.34%), `Số phòng vệ sinh` (39.04%), `Mặt tiền` (36.11%), `Đường vào` (32.31%), `Loại BĐS` (32.13%).

### 2. Xây dựng các hàm tiền xử lý

Notebook tự xây các hàm để chuẩn hóa dữ liệu thô về đúng kiểu dữ liệu:

- Nhóm hàm chuẩn hóa số và số đo:
  - `parse_number`: đọc chuỗi số có thể chứa cả dấu `.` và `,`, đồng thời cần phân biệt trường hợp dấu phân tách hàng nghìn với dấu thập phân theo ngữ cảnh.
  - `clean_gia`: chuẩn hóa giá về VND, xử lý các đơn vị như `tỷ`, `triệu`, `k`, `nghìn`; các giá trị như `thỏa thuận`, `liên hệ` sẽ trả về `None`. Với dữ liệu thực tế, cần đặc biệt lưu ý các mẫu như `11,787 tỷ`, `1,035 tỷ`, `3 tỷ 386 triệu`.
  - `clean_dien_tich`: chuẩn hóa diện tích về số thực, hỗ trợ cả trường hợp `ha` sang `m2`.
  - `tach_kich_thuoc`: tách chuỗi kích thước dạng `rộng x dài` thành 2 cột số.
- Nhóm hàm chuẩn hóa địa chỉ:
  - Chuẩn hóa chuỗi địa chỉ bằng cách bỏ dấu, đưa về chữ thường, loại ký tự đặc biệt.
  - Tạo danh sách chuẩn `LIST_QUAN` cho các quận/huyện tại TP.HCM.
  - Tạo mapping phường/xã về quận/huyện để hỗ trợ suy luận địa chỉ.
  - Gộp các cách gọi như `Quận 2`, `Quận 9`, `Thủ Đức`, `Thành Phố Thủ Đức` về `Thủ Đức`.
  - Trả về `CXD` nếu không xác định được `Quận/Huyện`.
- Nhóm hàm chuẩn hóa thuộc tính số nguyên:
  - `clean_so`: lấy số nguyên đầu tiên từ chuỗi để xử lý `Số tầng`, `Số phòng ngủ`, `Số phòng vệ sinh`, `Số phòng tắm`.
  - `clean_chodexe`: chuẩn hóa `Chỗ để xe` thành:
    - `1` nếu xác định là có chỗ để xe.
    - `0` nếu xác định là không có.
    - `None` nếu không xác định được.

### 3. Áp dụng các hàm clean cơ bản

Notebook tạo `df_processed` từ dữ liệu thô và áp dụng các bước clean cơ bản:

- Ánh xạ tên cột gốc sang tên cột không dấu để xử lý nội bộ:
  - Ví dụ: `Tiêu đề -> TieuDe`, `Giá -> Gia`, `Diện tích -> DienTich`, `Địa chỉ -> DiaChiRaw`, `Ngày đăng -> NgayDangRaw`.
- Chuẩn hóa các cột:
  - `Gia`: chuyển về số VND.
  - `DienTich`: chuyển về số thực.
  - `MatTien`, `DuongVao`: chuyển về số thực.
  - `NgayDang`: chuyển từ chuỗi định dạng `%d/%m/%Y` sang `datetime`.
  - `SoTang`, `SoPhongNgu`, `SoPhongVeSinh`, `SoPhongTam`: lấy số nguyên từ text.
  - `ChoDeXe`: đưa về `0/1/None`.
- Tách `KichThuoc` thành:
  - `ChieuRong`
  - `ChieuDai`
- Tính lại `Gia_m2` bằng công thức:
  - `Gia / DienTich`
  - Chỉ tính khi `Gia` và `DienTich` hợp lệ và `DienTich > 0`.

### 4. Chuẩn hóa loại bất động sản

Notebook chuẩn hóa `LoaiBDS` theo hai nguồn thông tin:

- Nếu cột loại gốc có dữ liệu thì làm sạch trước.
- Nếu cột loại gốc thiếu hoặc rơi vào nhóm khó dùng, notebook suy luận thêm từ `Tiêu đề`.
- Các nhóm loại chính được chuẩn hóa trong code:
  - `Nhà`
  - `Đất`
  - `Căn hộ`
  - `Biệt thự`
- Nếu vẫn không phân loại được, gán:
  - `Không xác định`

### 5. Áp dụng tách địa chỉ

Notebook áp dụng pipeline tách địa chỉ lên cột `DiaChiRaw`:

- Tạo 3 cột mới:
  - `ThanhPho`
  - `QuanHuyen`
  - `DiaChiFull`
- Nhận diện `ThanhPho = Hồ Chí Minh` nếu trong địa chỉ có các mẫu như:
  - `Hồ Chí Minh`
  - `TP.HCM`
  - `tphcm`
  - `hcm`
- Suy luận `QuanHuyen` từ:
  - tên quận/huyện xuất hiện trực tiếp trong địa chỉ.
  - mapping phường/xã sang quận/huyện đã định nghĩa trong notebook.
- Nếu không xác định được quận/huyện thì gán:
  - `CXD`

### 6. Xóa dữ liệu thiếu thông tin quan trọng

Notebook loại bỏ các dòng chưa đủ dữ liệu cốt lõi cho phân tích giá:

- Xác định các cột bắt buộc:
  - `Gia`
  - `DienTich`
  - `QuanHuyen`
- Cách lọc:
  - Xóa các dòng thiếu `Gia`.
  - Xóa các dòng thiếu `DienTich`.
  - Xóa các dòng có `QuanHuyen = 'CXD'`.

### 7. Lựa chọn và sắp xếp các cột cuối cùng

Sau khi làm sạch, notebook giữ lại bộ cột cuối cùng cho `df_final`:

- `TieuDe`
- `Nguon`
- `ThanhPho`
- `QuanHuyen`
- `DiaChiFull`
- `NgayDang`
- `LoaiBDS`
- `Gia`
- `DienTich`
- `Gia_m2`
- `MatTien`
- `DuongVao`
- `ChieuRong`
- `ChieuDai`
- `SoTang`
- `SoPhongNgu`
- `SoPhongVeSinh`
- `SoPhongTam`
- `ChoDeXe`

### 8. Xử lý giá trị thiếu còn lại

Notebook tiếp tục xử lý `NaN` còn sót lại trong `df_final`:

- Điền `0` cho các cột số còn thiếu:
  - `MatTien`
  - `DuongVao`
  - `ChieuRong`
  - `ChieuDai`
  - `SoTang`
  - `SoPhongNgu`
  - `SoPhongVeSinh`
  - `SoPhongTam`
  - `ChoDeXe`
- Với `NgayDang`:
  - Nếu còn thiếu thì xóa dòng đó.
  - Notebook ghi nhận đã xóa 43 dòng thiếu `NgayDang`.
- Nhận xét được ghi ngay trong notebook:
  - Sau bước này, dữ liệu số và thời gian đã sẵn sàng cho bước xử lý outlier.
  - Sau khi điền `0`, dữ liệu chỉ còn thiếu ở `ThanhPho` với 36,231 dòng (~29.68%).
  - `ThanhPho` vẫn còn thiếu khá nhiều vì nhiều địa chỉ nguồn không ghi rõ `TP.HCM`.
  - Việc điền `0` cho các cột số là chiến lược đơn giản để phục vụ EDA/báo cáo; nếu dùng cho modeling thì nên cân nhắc giữ `NaN` và tạo thêm cờ missing.

### 9. Kiểm tra outliers

Notebook kiểm tra ngoại lai trước khi lọc bằng boxplot cho 3 cột:

- `Gia`
- `DienTich`
- `Gia_m2`

Chi tiết cách làm:

- Dùng `boxplot` để xem phân bố dữ liệu.
- Riêng `Gia` được hiển thị theo thang `log` vì khoảng giá quá rộng.
- Notebook có ghi nhận trực quan rằng tồn tại nhiều giá trị rất lớn và phi thực tế ở cả `Gia`, `DienTich`, `Gia_m2`.

### 10. Loại bỏ outliers

Notebook tạo bản sao `df_before_outlier` rồi lọc outliers trên `df_final` theo từng nhóm:

- Với `Gia`:
  - Lọc theo từng `LoaiBDS`.
  - Bỏ qua nhóm `Không xác định` và `CXD`.
  - Chỉ xử lý khi nhóm có ít nhất 50 dòng.
  - Dùng ngưỡng phân vị `0.5% - 99.5%`.
- Với `DienTich`:
  - Dùng giới hạn cứng theo loại BĐS.
  - Trong code có các khoảng như:
    - `Căn hộ`: `20 - 500 m²`
    - `Nhà`: `20 - 1000 m²`
    - `Đất`: `30 - 5000 m²`
    - `Biệt thự`: `80 - 2000 m²`
  - Các nhóm không có trong bảng giới hạn sẽ được giữ nguyên.
- Với `Gia_m2`:
  - Lọc theo từng `LoaiBDS`.
  - Bỏ qua nhóm `Không xác định` và `CXD`.
  - Chỉ xử lý khi nhóm có ít nhất 50 dòng.
  - Dùng ngưỡng phân vị `1% - 99%`.
- Sau mỗi bước, notebook ghép lại dữ liệu và in ra số lượng dòng bị loại bỏ.
- Vì bỏ qua nhóm `Không xác định`, dữ liệu cuối vẫn có thể còn lại một số giá trị cực lớn chưa được lọc hết.

### 11. Trực quan hóa outliers sau khi xử lý

Sau khi lọc outliers, notebook vẽ lại boxplot cho:

- `Gia`
- `DienTich`
- `Gia_m2`

Phần markdown sau xử lý nên diễn giải thận trọng hơn:

- Outlier đã giảm rõ ở các nhóm `Nhà`, `Đất`, `Căn hộ`, `Biệt thự`, nhưng chưa thể xem là đã sạch hoàn toàn trên toàn bộ file cuối.
- `DienTich` sau lọc tập trung hơn ở nhóm diện tích thực tế, chủ yếu từ vài chục đến vài trăm `m²`, dù vẫn còn một số giá trị rất lớn ở nhóm chưa được lọc triệt để.
- `Gia` và `Gia_m2` vẫn còn một số cực trị lớn, chủ yếu do nhóm `Không xác định` bị bỏ qua ở bước lọc percentile và do một số giá trị giá dạng text cần được parse chặt hơn.

### 12. Xuất kết quả

Notebook xuất dữ liệu đã tiền xử lý bằng bước cuối:

- Tạo thư mục output nếu chưa tồn tại.
- Lưu `df_final` ra file:
  - `data/processed/fulldata_clean_4web.csv`
- File được ghi với encoding:
  - `utf-8-sig`

## Kết quả đầu ra của notebook

- Input:
  - `data/raw/fulldata_raw_4web.csv`
- Output:
  - `data/processed/fulldata_clean_4web.csv`

README này chỉ mô tả đúng các bước đang có trong notebook `preprocessing_bds_hcm.ipynb`.
