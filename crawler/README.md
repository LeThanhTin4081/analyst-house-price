# Data Collection Case Study (Crawler)

Dự án này xây dựng mô-đun thu thập dữ liệu bất động sản TP.HCM để tạo dữ liệu đầu vào cho pipeline phân tích:

![Sơ đồ quy trình crawl](https://drive.google.com/thumbnail?id=1fw30wnc66BwRzmhrge1I1T-VmrKALZfu)

`crawl -> preprocessing -> EDA -> Power BI`

Mục tiêu không chỉ là lấy được dữ liệu, mà là tạo một hệ thu thập đủ ổn định để:

- Thu thập đa nguồn, giảm thiên lệch từ một website đơn lẻ.
- Có thể chạy dài và khôi phục khi bị gián đoạn.
- Chuẩn bị dữ liệu ở mức đủ sạch để bước xử lý sau không bị “vỡ” pipeline.

## Phạm vi dữ liệu thu thập

Tôi thu thập dữ liệu từ 4 nguồn tin đăng:

- Batdongsan.com.vn
- Alonhadat.com.vn
- Meeyland.com
- Nhadat24h.net

Các trường dữ liệu xoay quanh nhóm thông tin cốt lõi của tin đăng:

- Thông tin nhận diện: tiêu đề, loại BĐS, địa chỉ, ngày đăng.
- Thông tin giá trị: giá, giá/m2.
- Thông tin quy mô và đặc điểm: diện tích, số tầng, số phòng ngủ, số phòng tắm/WC, mặt tiền, đường vào, chỗ để xe.

## Thiết kế kỹ thuật tôi áp dụng

Kiến trúc thu thập được tổ chức theo pipeline chức năng, gồm các bước:

`Navigator -> Scroller -> Collector -> Extractor -> Writer -> Checkpoint -> Retry`

Những điểm kỹ thuật tôi tập trung:

- Tách crawler theo từng website để giảm coupling và dễ bảo trì khi DOM thay đổi.
- Dùng chiến lược selector + fallback regex để tăng độ bền khi giao diện thay đổi nhẹ.
- Ghi dữ liệu theo batch và lưu checkpoint theo trang để tránh mất dữ liệu khi dừng giữa chừng.
- Bổ sung retry, delay ngẫu nhiên và profile trình duyệt để giảm rủi ro bị chặn.

## Thách thức thực tế và cách tôi xử lý

- Card quảng cáo xen card thật: tôi thêm điều kiện lọc để giảm nhiễu dữ liệu.
- Định dạng ngày đăng không đồng nhất: tôi chuẩn hóa về cùng một chuẩn thời gian.
- DOM giữa các nguồn khác nhau rõ rệt: tôi tách extractor theo website, không dùng một parser chung cứng nhắc.
- Dữ liệu thiếu cục bộ: tôi giữ chiến lược `N/A` có kiểm soát để không làm gãy pipeline downstream.

## Kết quả thu thập

Ở phiên bản dữ liệu lớn trước của dự án, mô-đun này đã thu được:

- Batdongsan: 76,748 tin
- Nhadat24h: 59,913 tin
- Alonhadat: 17,537 tin
- Meeyland: 9,340 tin
- Tổng cộng: 163,538 tin

Sau hợp nhất đa nguồn, tôi chuẩn hóa thành bộ trường phân tích dùng cho preprocessing, EDA và dashboard.

## Giá trị đối với bài toán Data Science

Phần crawler này là nền tảng để tôi:

- Chứng minh khả năng xây dựng dữ liệu đầu vào thay vì phụ thuộc dataset có sẵn.
- Kiểm soát chất lượng dữ liệu ngay từ đầu pipeline.
- Tạo tính tái lập cho toàn bộ quy trình phân tích dữ liệu của dự án.
