# 📂 Dữ liệu dự án (Data Directory)

Thư mục này lưu trữ toàn bộ dữ liệu phục vụ phân tích thị trường BĐS TP.HCM, được tổ chức theo hai giai đoạn xử lý: **raw** (dữ liệu thô) và **processed** (dữ liệu đã làm sạch).

---

## 1. Dữ liệu thô (raw data)

Dữ liệu được crawl tự động từ 4 sàn giao dịch BĐS trực tuyến, chưa qua bất kỳ bước xử lý nào.

| Thông tin | Chi tiết |
|---|---|
| **File tổng hợp** | `fulldata_raw_4web.csv` |
| **Tổng số bản ghi** | 163,571 dòng |
| **Số cột** | 16 |
| **Giai đoạn** | 2021 – 2025 |

### Cấu trúc cột

| # | Tên cột | Mô tả |
|---|---|---|
| 1 | `Tiêu đề` | Tiêu đề tin đăng bán BĐS |
| 2 | `Giá` | Giá bán (đơn vị gốc, chưa chuẩn hóa) |
| 3 | `Diện tích` | Diện tích BĐS (chưa chuẩn hóa) |
| 4 | `Giá/m2` | Đơn giá trên mỗi m² |
| 5 | `Địa chỉ` | Địa chỉ đầy đủ |
| 6 | `Ngày đăng` | Ngày đăng tin |
| 7 | `Số phòng ngủ` | Số phòng ngủ |
| 8 | `Số phòng vệ sinh` | Số phòng vệ sinh |
| 9 | `Số phòng tắm` | Số phòng tắm |
| 10 | `Số tầng` | Số tầng |
| 11 | `Chỗ để xe` | Thông tin chỗ để xe |
| 12 | `Kích thước` | Kích thước (chiều rộng × chiều dài) |
| 13 | `Mặt tiền` | Chiều rộng mặt tiền (m) |
| 14 | `Đường vào` | Chiều rộng đường vào (m) |
| 15 | `Loại BĐS` | Phân loại loại hình BĐS |
| 16 | `Nguồn` | Tên sàn giao dịch gốc |

### Các file nguồn riêng lẻ

Dữ liệu được crawl riêng biệt từ 4 sàn, sau đó gộp thành file tổng hợp:

| File | Nguồn |
|---|---|
| `raw_data_bds_full.csv` | batdongsan.com.vn |
| `raw_data_nhadat24h_full.csv` | nhadat24h.net |
| `raw_data_alonhatdat_full.csv` | alonhadat.com.vn |
| `raw_data_meeyland_full.csv` | meeyland.com |

---

## 2. Dữ liệu đã xử lý (clean data)

Bộ dữ liệu đã qua tiền xử lý và làm sạch, sẵn sàng phục vụ phân tích khám phá (EDA) và trực quan hóa trên Power BI.

| Thông tin | Chi tiết |
|---|---|
| **File** | `fulldata_clean_4web.csv` |
| **Tổng số bản ghi** | 116,875 dòng |
| **Số cột** | 19 |
| **Tỷ lệ giảm** | ~28.5% bản ghi bị loại sau khi làm sạch |

### Cấu trúc cột

| # | Tên cột | Mô tả | Ghi chú |
|---|---|---|---|
| 1 | `TieuDe` | Tiêu đề tin đăng | Giữ nguyên |
| 2 | `Nguon` | Tên sàn giao dịch gốc | Giữ nguyên |
| 3 | `ThanhPho` | Thành phố | Chuẩn hóa: TP.HCM |
| 4 | `QuanHuyen` | Quận / Huyện | Tách từ `Địa chỉ` gốc |
| 5 | `DiaChiFull` | Địa chỉ đầy đủ | Chuẩn hóa |
| 6 | `NgayDang` | Ngày đăng tin | Chuẩn hóa format |
| 7 | `LoaiBDS` | Loại hình BĐS | 4 loại: Nhà, Đất, Căn hộ, Biệt thự |
| 8 | `Gia` | Giá bán (VNĐ) | Quy đổi về đơn vị thống nhất |
| 9 | `DienTich` | Diện tích (m²) | Chuẩn hóa đơn vị |
| 10 | `Gia_m2` | Đơn giá (VNĐ/m²) | Tính toán: `Gia / DienTich` |
| 11 | `MatTien` | Chiều rộng mặt tiền (m) | Trích xuất |
| 12 | `DuongVao` | Chiều rộng đường vào (m) | Trích xuất |
| 13 | `ChieuRong` | Chiều rộng (m) | Tách từ `Kích thước` gốc |
| 14 | `ChieuDai` | Chiều dài (m) | Tách từ `Kích thước` gốc |
| 15 | `SoTang` | Số tầng | Chuẩn hóa kiểu số |
| 16 | `SoPhongNgu` | Số phòng ngủ | Chuẩn hóa kiểu số |
| 17 | `SoPhongVeSinh` | Số phòng vệ sinh | Chuẩn hóa kiểu số |
| 18 | `SoPhongTam` | Số phòng tắm | Chuẩn hóa kiểu số |
| 19 | `ChoDeXe` | Chỗ để xe | Chuẩn hóa |

> Chi tiết quy trình tiền xử lý (Raw → Clean) được trình bày tại [`notebooks/preprocessing/`](../notebooks/preprocessing/).
