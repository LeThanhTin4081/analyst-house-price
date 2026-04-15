# Script crawl dữ liệu Batdongsan.com.vn khu vực TP.HCM và ghi ra CSV.

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time, random, csv, os

# Cấu hình chạy crawl.
PROFILE_DIR = r"C:\selenium_profiles\bds_profile_hcm"  # Thư mục lưu cookie/session.
os.makedirs(PROFILE_DIR, exist_ok=True)

BASE = "https://batdongsan.com.vn/nha-dat-ban-tp-hcm"  # Chỉ crawl khu vực TP.HCM.
MAX_PAGES = 4  # Số trang cần crawl.
OUTPUT_FILE = "ket_quakk.csv"  # File CSV đầu ra.


def build_url(page: int) -> str:
    """Tạo URL cho từng trang danh sách."""
    return BASE if page == 1 else f"{BASE}/p{page}"


def init_driver():
    """Khởi tạo trình duyệt UC với profile cố định."""
    opts = uc.ChromeOptions()
    opts.add_argument(f"--user-data-dir={PROFILE_DIR}")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--start-maximized")
    return uc.Chrome(options=opts)


def wait_cloudflare(driver, timeout=60):
    """Đợi đến khi trang không còn thông báo xác minh Cloudflare."""
    WebDriverWait(driver, timeout).until(
        lambda d: "Verifying you are human" not in d.page_source
    )


def scroll_slow(driver):
    """Cuộn trang theo từng đoạn để kích hoạt lazy-load nội dung."""
    h = driver.execute_script("return document.body.scrollHeight")
    for y in (0.2, 0.5, 0.85):
        driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight*{y});")
        time.sleep(random.uniform(0.8, 1.5))


def get_cards(driver, wait):
    """Lấy danh sách card bài đăng sau khi phần tử xuất hiện."""
    wait.until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".re__card-info-content"))
    )
    return driver.find_elements(By.CSS_SELECTOR, ".re__card-info-content")


def extract_info(card):
    """Trích xuất thông tin cần thiết từ một card bài đăng."""

    def safe_text(sel):
        """Đọc text theo selector, lỗi thì trả chuỗi rỗng."""
        try:
            return card.find_element(By.CSS_SELECTOR, sel).text.strip()
        except:
            return ""

    title = safe_text(".pr-title.js__card-title")
    price = safe_text(".re__card-config-price.js__card-config-item")
    area = safe_text(".re__card-config-area.js__card-config-item")
    address = safe_text(".re__card-address") or safe_text(
        ".re__card-config-location.js__card-config-item"
    )
    date = safe_text(".re__card-published-date") or safe_text(".re__card-config-posted")
    try:
        link = (
            card.find_element(By.CSS_SELECTOR, "a.js__card-click").get_attribute("href")
            or ""
        )
    except:
        link = ""
    return [title, price, area, address, date, link]


def main():
    """Vòng lặp crawl chính: duyệt trang, trích xuất và ghi CSV."""
    driver = init_driver()
    wait = WebDriverWait(driver, 20)
    data = []

    for page in range(1, MAX_PAGES + 1):
        url = build_url(page)
        print(f"Trang {page}: {url}")
        driver.get(url)

        try:
            wait_cloudflare(driver, 60)
        except:
            input(
                "Nếu Cloudflare yêu cầu xác minh, giải xong rồi nhấn ENTER để tiếp tục..."
            )

        time.sleep(random.uniform(2, 4))
        scroll_slow(driver)

        try:
            cards = get_cards(driver, wait)
        except:
            print("Khong lay duoc danh sach bai dang, bo qua trang nay.")
            continue

        for card in cards:
            row = extract_info(card)
            data.append(row)
            print(f"[{page}] {row[0]} | {row[1]} | {row[2]}")

        time.sleep(random.uniform(2, 5))

    # Ghi CSV bằng UTF-8-SIG để mở bằng Excel không lỗi tiếng Việt.
    headers = ["Tiêu đề", "Giá", "Diện tích", "Địa chỉ", "Ngày đăng", "Link"]
    with open(OUTPUT_FILE, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    print(f"\nDa ghi {len(data)} dong vao: {OUTPUT_FILE}")
    driver.quit()


if __name__ == "__main__":
    main()
