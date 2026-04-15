import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException, TimeoutException
from datetime import datetime, timedelta
import time, random, csv, os, sys, tempfile, shutil, re

# Cấu hình chính cho crawler Alonhadat (TP.HCM).
PROFILE_DIR = r"C:\selenium_profiles\alonhadat_profile_hcm"
BASE = "https://alonhadat.com.vn/can-ban-nha-dat/ho-chi-minh"
OUTPUT_FILE = "alonhadat_raw.csv"
CHECKPOINT = "alonhadat_checkpoint.txt"
WAIT_TIMEOUT = 20
SAFETY_CAP = 10000
HEADLESS = False

# Khớp phiên bản major của Chrome
CHROME_MAJOR = 141

# Dùng profile tạm để giảm lỗi lock profile.
# Đặt False nếu cần giữ phiên đăng nhập tại PROFILE_DIR.
USE_TEMP_PROFILE = True

# Tạo thư mục profile nếu chưa tồn tại.
os.makedirs(PROFILE_DIR, exist_ok=True)


def build_url_candidates(page: int):
    """Sinh danh sách URL ứng viên cho từng trang cần crawl."""
    if page == 1:
        return [BASE]
    return [
        f"{BASE}/trang-{page}",  # đúng pattern của alonhadat
        f"{BASE}/page-{page}",
        f"{BASE}/p{page}",
    ]


def _make_options(tmp_profile_dir: str | None):
    """Khởi tạo ChromeOptions cho UC theo profile tạm hoặc profile cố định."""
    opts = uc.ChromeOptions()
    if tmp_profile_dir:
        opts.add_argument(f"--user-data-dir={tmp_profile_dir}")
    else:
        opts.add_argument(f"--user-data-dir={PROFILE_DIR}")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--start-maximized")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--no-sandbox")
    if HEADLESS:
        opts.add_argument("--headless=new")
        opts.add_argument("--window-size=1366,900")
    return opts


def init_driver():
    """Khởi tạo trình duyệt UC với cơ chế retry và ép major version."""
    tmp_profile = tempfile.mkdtemp(prefix="alnd_uc_") if USE_TEMP_PROFILE else None
    opts = _make_options(tmp_profile)
    last_err = None
    for attempt in range(1, 4):
        try:
            drv = uc.Chrome(
                options=opts,
                version_main=CHROME_MAJOR,
                use_subprocess=True,
                suppress_welcome=True,
            )
            drv._tmp_profile_dir = tmp_profile
            return drv
        except WebDriverException as e:
            last_err = e
            time.sleep(2 * attempt)
    if tmp_profile:
        shutil.rmtree(tmp_profile, ignore_errors=True)
    raise last_err


def safe_quit(driver):
    """Đóng driver an toàn và dọn thư mục profile tạm (nếu có)."""
    try:
        driver.quit()
    except:
        pass
    tmp = getattr(driver, "_tmp_profile_dir", None)
    if tmp and os.path.isdir(tmp):
        shutil.rmtree(tmp, ignore_errors=True)


def text_or_na(ele, by, sel):
    """Lấy text theo selector; nếu lỗi thì trả về N/A."""
    try:
        el = ele.find_element(by, sel)
        return el.text.strip().replace("\n", " ")
    except:
        return "N/A"


def first_not_na(*vals):
    """Trả về giá trị đầu tiên hợp lệ khác N/A."""
    for v in vals:
        if v and v != "N/A" and v.strip():
            return v.strip()
    return "N/A"


def only_number(s):
    """Tách phần số đầu tiên trong chuỗi."""
    if not s or s == "N/A":
        return "N/A"
    m = re.search(r"\d+", s)
    return m.group(0) if m else "N/A"


def parse_date_label(label):
    """Chuẩn hóa nhãn ngày đăng về định dạng dd/mm/YYYY khi có thể."""
    label = (label or "").strip()
    if not label:
        return "N/A"
    low = label.lower()
    today = datetime.now()
    if "hôm nay" in low:
        return today.strftime("%d/%m/%Y")
    if "hôm qua" in low:
        return (today - timedelta(days=1)).strftime("%d/%m/%Y")
    # Một số thẻ time trả về dạng YYYY-MM-DD.
    try:
        if re.match(r"\d{4}-\d{2}-\d{2}", label):
            dt = datetime.strptime(label, "%Y-%m-%d")
            return dt.strftime("%d/%m/%Y")
    except:
        pass
    return label


CARD_SEL = "section.list-property-box article.property-item"


def get_cards(driver, wait):
    """Đợi card xuất hiện rồi trả về danh sách card bài đăng."""
    try:
        wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, CARD_SEL)))
    except TimeoutException:
        return []
    return driver.find_elements(By.CSS_SELECTOR, CARD_SEL)


def get_title(card):
    """Trích xuất tiêu đề bài đăng."""
    return text_or_na(card, By.CSS_SELECTOR, "h3.property-title")


def get_price(card):
    """Trích xuất giá; ưu tiên node có itemprop=price."""
    try:
        p = card.find_element(By.CSS_SELECTOR, 'span.price [itemprop="price"]')
        return (p.text or "").strip() or "N/A"
    except:
        return text_or_na(card, By.CSS_SELECTOR, "span.price")


def get_area(card):
    """Trích xuất diện tích; fallback sang regex khi thiếu selector chuẩn."""
    v1 = text_or_na(card, By.CSS_SELECTOR, "span.area")
    if v1 != "N/A":
        return v1.replace("Diện tích:", "").replace("DT:", "").strip()
    try:
        wrap = card.find_element(By.CSS_SELECTOR, "div")
        m = re.search(r"(\d+[\,\.]?\d*)\s*m²", wrap.text)
        return f"{m.group(1)} m²" if m else "N/A"
    except:
        return "N/A"


def get_size(card):
    """Trích xuất kích thước (KT) từ card."""
    # Ưu tiên theo nhãn KT.
    try:
        container = card.find_element(
            By.XPATH, './/*[contains(normalize-space(.),"KT")]'
        )
        m = re.search(r"KT[:\s]*([0-9xX\.\,]+m?\s*[0-9xX\.\,]*m?)", container.text)
        if m:
            return m.group(1).strip()
    except:
        pass
    return text_or_na(card, By.CSS_SELECTOR, "span.size")


def get_address(card):
    """Trích xuất địa chỉ, ưu tiên địa chỉ mới."""
    s1 = text_or_na(card, By.CSS_SELECTOR, "div.property-address p.new-address")
    s2 = text_or_na(card, By.CSS_SELECTOR, "div.property-address p.old-address")
    return first_not_na(s1, s2)


def get_street_width(card):
    """Trích xuất thông tin đường trước nhà."""
    return text_or_na(card, By.CSS_SELECTOR, "span.street-width")


def get_floors(card):
    """Trích xuất số tầng (chỉ giữ phần số)."""
    return only_number(text_or_na(card, By.CSS_SELECTOR, "span.floors"))


def get_bedrooms(card):
    """Trích xuất số phòng ngủ (chỉ giữ phần số)."""
    return only_number(text_or_na(card, By.CSS_SELECTOR, "span.bedroom"))


def get_parking(card):
    """Trích xuất thông tin chỗ để xe từ cờ meta hoặc nhãn hiển thị."""
    try:
        park = card.find_element(By.CSS_SELECTOR, "span.parking")
        # Có meta value=true/false.
        try:
            val = (
                park.find_element(
                    By.CSS_SELECTOR, 'meta[itemprop="value"]'
                ).get_attribute("content")
                or ""
            )
            return "Có" if val.lower() in ("true", "1", "yes") else "Không"
        except:
            # Không có meta: nếu có label thì xem như có chỗ để xe.
            lbl = text_or_na(park, By.CSS_SELECTOR, 'span[itemprop="name"]')
            return "Có" if lbl != "N/A" else "Không"
    except:
        return "Không"


def get_posted(card):
    """Trích xuất ngày đăng từ text hoặc datetime attribute."""
    try:
        t = card.find_element(By.CSS_SELECTOR, "time.created-date")
        txt = (t.text or "").strip()
        dt = (t.get_attribute("datetime") or "").strip()
        return parse_date_label(txt or dt)
    except:
        return "N/A"


def extract_row(card):
    """Ghép dữ liệu từ card thành một dòng CSV theo đúng thứ tự cột."""
    return [
        get_title(card),  # Tiêu đề
        get_street_width(card),  # Đường trước nhà
        get_floors(card),  # Số tầng (số)
        get_bedrooms(card),  # Số phòng ngủ (số)
        get_parking(card),  # Chỗ để xe (Có/Không)
        get_price(card),  # Giá
        get_area(card),  # Diện tích (… m²)
        get_size(card),  # Kích thước (KT)
        get_posted(card),  # Ngày đăng (dd/mm/YYYY | label)
        get_address(card),  # Địa chỉ
    ]


def ensure_csv_header(path: str):
    """Tạo header CSV khi file chưa tồn tại hoặc đang rỗng."""
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f)
            w.writerow(
                [
                    "Tiêu đề",
                    "Đường trước nhà",
                    "Số tầng",
                    "Số phòng ngủ",
                    "Chỗ để xe",
                    "Giá",
                    "Diện tích",
                    "Kích thước",
                    "Ngày đăng",
                    "Địa chỉ",
                ]
            )


def read_checkpoint() -> int:
    """Đọc checkpoint để resume trang crawl; lỗi thì fallback về trang 1."""
    if os.path.exists(CHECKPOINT):
        try:
            with open(CHECKPOINT, "r", encoding="utf-8") as f:
                return max(1, int(f.read().strip()))
        except:
            return 1
    return 1


def write_checkpoint(page: int):
    """Ghi checkpoint sau mỗi trang crawl thành công."""
    with open(CHECKPOINT, "w", encoding="utf-8") as f:
        f.write(str(page))


def main():
    """Vòng lặp crawl chính: lấy card, lọc dòng hợp lệ, ghi CSV và checkpoint."""
    driver = None
    try:
        driver = init_driver()
        wait = WebDriverWait(driver, WAIT_TIMEOUT)
        csv_path = os.path.join(os.getcwd(), OUTPUT_FILE)
        ensure_csv_header(csv_path)

        page = read_checkpoint()
        processed = 0

        while page <= SAFETY_CAP:
            # Thử nhiều pattern URL cho cùng một trang để tăng độ ổn định.
            urls = build_url_candidates(page)
            got_cards = []
            chosen = None
            for url in urls:
                print(f"==> Trang {page}: {url}")
                driver.get(url)
                time.sleep(random.uniform(1.0, 1.8))
                cards = get_cards(driver, wait)
                if cards:
                    got_cards = cards
                    chosen = url
                    break
            if not got_cards:
                print(
                    "Het du lieu (khong con card hop le / khong tim thay trang). Dung."
                )
                break

            print(f"URL dung: {chosen}")
            print(f"So bai dang phat hien: {len(got_cards)}")

            with open(csv_path, "a", encoding="utf-8-sig", newline="") as f:
                w = csv.writer(f)
                for c in got_cards:
                    row = extract_row(c)
                    # Lọc dòng thiếu thông tin cốt lõi.
                    if row[0] == "N/A" or row[5] == "N/A":
                        continue
                    w.writerow(row)
                    processed += 1
                    # Flush để giảm rủi ro mất dữ liệu khi crawl dài.
                    f.flush()
                    try:
                        os.fsync(f.fileno())
                    except:
                        pass

            write_checkpoint(page)
            page += 1
            print(f"Tong da ghi: {processed} dong\n")
            time.sleep(random.uniform(1.4, 2.6))

        print(f"Hoan tat. Da ghi {processed} dong vao {csv_path}")

    except KeyboardInterrupt:
        print("\nDung boi nguoi dung. Co the chay lai tu checkpoint.")
    finally:
        if driver:
            safe_quit(driver)


if __name__ == "__main__":
    main()
