# 📁 Data Collection Case Study (Crawler)

Dự án này xây dựng mô-đun thu thập dữ liệu (Data Ingestion) về thị trường bất động sản TP.HCM, đóng vai trò là tầng dữ liệu nền tảng cho toàn bộ pipeline phân tích:


`Web Crawling -> Data Preprocessing -> Exploratory Data Analysis (EDA) -> Power BI Dashboard`

Mục tiêu cốt lõi không chỉ là cào được dữ liệu, mà là thiết kế một **hệ thống thu thập bền bỉ (Robust System)** nhằm:
- Thu thập đa nguồn (Multi-source), triệt tiêu thiên lệch dữ liệu (Data Bias) từ một website duy nhất.
- Hoạt động ổn định trên quy mô lớn, có cơ chế phục hồi (Recovery) khi bị gián đoạn.
- Đảm bảo tính nhất quán của Schema ngay từ đầu vào, tránh làm "vỡ" pipeline ở các bước xử lý sau (Downstream).

## 📌 1. Phạm vi dữ liệu (Data Scope)

Hệ thống tiến hành rà quét trên 4 nền tảng giao dịch BĐS hàng đầu:

- `Batdongsan.com.vn`: Là trang có mức độ phủ sóng toàn quốc, đặc biệt phong phú tại khu vực TP.Hồ Chí Minh
Dữ liệu có cấu trúc rõ ràng → dễ trích xuất

Thu thập các trường dữ liệu trên trang “batdongsan.com.vn” 8 trường dữ liệu, chi tiết như ảnh sau:

![Thu thập batdongsan.com.vn](https://drive.google.com/uc?export=view&id=1Sy6o9SKlYTX5BNSJj9U2C1J1tI3isFkE)


- `Alonhadat.com.vn`: Là website BĐS lâu đời, có uy tín tại Việt Nam. Giao diện đơn giản nhưng vẫn có lượng tin lớn

Thu thập các trường dữ liệu trên trang “Alonhadat.com.vn” 10 trường dữ liệu, chi tiết như ảnh sau:

![Thu thập Alonhadat.com.vn](https://drive.google.com/uc?export=view&id=1dut2AZNRR-vzzZZ0m21c0Eom722IcCWI)


- `Meeyland.com`: Là nền tảng BĐS mới nổi nhưng giao diện hiện đại, trẻ trung. Mặc dù mới nhưng vẫn cung cấp lượng thông tin đáng kể tại TP. HCM

Thu thập các trường dữ liệu trên trang “Meeyland.com” 8 trường dữ liệu, 

![Thu thập [Tên_Trang_Web]](https://drive.google.com/uc?export=view&id=1eLT1e52JpSU5CdlUy7i8m--OpuTy8WXE)
  
- `Nhadat24h.net`: Là một trong những website rao vặt BĐS top đầu, uy tín và phổ biến. Lượng thông tin cũng rất đa dạng và chi tiết

Thu thập các trường dữ liệu trên trang “Nhadat24h.net” 12 trường dữ liệu,

![Thu thập [Tên_Trang_Web]](https://drive.google.com/uc?export=view&id=1FE1P8w4HPY45CY-REewVgAVNxMZwAeju)



Các trường thông tin (Features) được bóc tách và phân nhóm rõ ràng:
- **Thông tin nhận diện:** Tiêu đề, Loại BĐS, Địa chỉ, Ngày đăng.
- **Định lượng giá trị:** Giá bán, Đơn giá (Giá/m²).
- **Đặc trưng kiến trúc:** Diện tích, Số tầng, Số phòng ngủ, Số phòng tắm/WC, Mặt tiền, Đường vào, Chỗ để xe.


## 💡 2. Kiến trúc & Thiết kế kỹ thuật (Technical Design)

Kiến trúc Crawler được tổ chức theo Pipeline chức năng bao gồm 7 chu trình như ảnh sau:

![Sơ đồ quy trình crawl](https://drive.google.com/uc?export=view&id=1UyY3Ljz3SOjJbauPj0niVYBwH75r7FQp)

**Các nguyên tắc thiết kế (Design Principles) được áp dụng:**
- **Decoupling (Giảm phụ thuộc):** Xây dựng Extractor độc lập cho từng website. Khi DOM của một trang thay đổi, chỉ module đó bị ảnh hưởng, toàn bộ hệ thống vẫn chạy bình thường.
- **Resilience (Độ bền bỉ):** Kết hợp chiến lược bóc tách bằng CSS Selector + Fallback Regex để đảm bảo không rớt dữ liệu khi cấu trúc UI website thay đổi nhẹ.
- **State Management (Quản lý trạng thái):** Áp dụng cơ chế ghi dữ liệu theo Batch và lưu Checkpoint theo từng trang. Nếu crawler bị crash, nó sẽ resume chính xác tại trang đang crawl dở.
- **Anti-Detection (Chống chặn):** Tích hợp Random Delay, xoay vòng User-Agent và Browser Profile để bypass các cơ chế chống bot của website.

## 🔦 3. Thách thức thực tế & Giải pháp (Troubleshooting)

- **Nhiễu dữ liệu (Ads Card xen lẫn Real Card):** Xây dựng bộ lọc logic để nhận diện và loại bỏ các thẻ HTML chứa bài quảng cáo tài trợ.
- **Xung đột định dạng thời gian:** Xây dựng hàm Parser linh hoạt để chuẩn hóa các format ngày tháng khác nhau về cùng một chuẩn `datetime` duy nhất.
- **Cấu trúc DOM dị biệt đa nguồn:** Từ bỏ việc dùng chung một parser cứng nhắc. Sử dụng Factory Pattern để gọi đúng Extractor tương ứng cho từng website.
- **Dữ liệu khuyết thiếu (Missing Values):** Không tự ý điền bừa (Fill) khi cào. Thiết lập chiến lược gán `N/A` có kiểm soát để bảo toàn tính trung thực của dữ liệu trước khi chuyển giao cho pha Preprocessing.

## 🔑 4. Kết quả đầu ra (Output)
Sau khi đã thu thập dữ liệu, hệ thống đã trích xuất thành công:

| Nguồn dữ liệu | Số lượng tin thu thập |
|---|---|
| **Batdongsan.com.vn** | 76,748 |
| **Nhadat24h.net** | 59,913 |
| **Alonhadat.com.vn** | 17,537 |
| **Meeyland.com** | 9,340 |
| **Tổng dung lượng** | **163,538 bản ghi** |

Toàn bộ dữ liệu thô (Raw Data) được hợp nhất và lưu trữ tại `data/raw/`, sẵn sàng cho bước Tiền xử lý.


