import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException, TimeoutException
from datetime import datetime, timedelta
import time, random, csv, os, sys, tempfile, shutil, re, unicodedata

# Cấu hình crawler Nhadat24h (TP.HCM).
PROFILE_DIR = r"C:\\selenium_profiles\\nhadat24h_profile_hcm"
BASE = "https://nhadat24h.net/nha-dat-ban-thanh-pho-ho-chi-minh"
OUTPUT_FILE = "nhadat24h_raw.csv"
CHECKPOINT = "nhadat24h_checkpoint.txt"
WAIT_TIMEOUT = 20
SAFETY_CAP = 10000
HEADLESS = False

# Khớp major version của Chrome (xem tại chrome://version).
CHROME_MAJOR = 141

# Dùng profile tạm để tránh lock profile.
# Đặt False nếu cần giữ phiên đăng nhập tại PROFILE_DIR.
USE_TEMP_PROFILE = True

# Tạo thư mục profile nếu chưa tồn tại.
os.makedirs(PROFILE_DIR, exist_ok=True)


def wait_cards(driver):
    """Đợi đến khi danh sách card bài đăng xuất hiện trên trang."""
    WebDriverWait(driver, WAIT_TIMEOUT).until(
        EC.presence_of_all_elements_located(
            (By.CSS_SELECTOR, ".dv-item, .dv-pt-item, .post-item, .item")
        )
    )


def _make_options(tmp_profile_dir: str | None):
    """Tạo ChromeOptions cho UC theo profile tạm hoặc profile cố định."""
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
    """Khởi tạo driver UC với retry và ép major version."""
    tmp_profile = tempfile.mkdtemp(prefix="nd24h_uc_") if USE_TEMP_PROFILE else None
    opts = _make_options(tmp_profile)
    last_err = None
    for attempt in range(1, 3 + 1):
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
    """Đóng driver an toàn và dọn profile tạm (nếu có)."""
    try:
        driver.quit()
    except:
        pass
    tmp = getattr(driver, "_tmp_profile_dir", None)
    if tmp and os.path.isdir(tmp):
        shutil.rmtree(tmp, ignore_errors=True)


def text_or_na(ele, by, sel, attr=None):
    """Lấy text/attribute theo selector; lỗi thì trả N/A."""
    try:
        el = ele.find_element(by, sel)
        if attr:
            return (el.get_attribute(attr) or "").strip()
        return (el.text or "").strip().replace("\n", " ")
    except:
        return "N/A"


def first_not_na(*vals):
    """Trả về giá trị đầu tiên hợp lệ khác N/A."""
    for v in vals:
        if v and v != "N/A" and str(v).strip():
            return str(v).strip()
    return "N/A"


def vn_lower(s):
    """Chuẩn hóa Unicode và chuyển chữ thường tiếng Việt."""
    return unicodedata.normalize("NFC", (s or "")).lower()


def parse_date_label(label):
    """Chuẩn hóa nhãn ngày đăng về định dạng dd/MM/YYYY khi có thể."""
    label = (label or "").strip()
    if not label:
        return "N/A"
    low = vn_lower(label)
    now = datetime.now()

    # dd/MM[/YYYY] đã chuẩn.
    m = re.search(r"\b(\d{1,2})/(\d{1,2})(?:/(\d{2,4}))?\b", label)
    if m:
        d, mth, y = m.group(1), m.group(2), m.group(3) or str(now.year)
        if len(y) == 2:
            y = "20" + y
        try:
            return datetime(int(y), int(mth), int(d)).strftime("%d/%m/%Y")
        except:
            pass

    if "hôm nay" in low:
        return now.strftime("%d/%m/%Y")
    if "hôm qua" in low:
        return (now - timedelta(days=1)).strftime("%d/%m/%Y")

    # Chuỗi thời gian tương đối.
    m = re.search(r"(\d+)\s+(phút|giờ|ngày|tuần|tháng)\s+trước", low)
    if m:
        n = int(m.group(1))
        unit = m.group(2)
        if unit == "phút":
            dt = now - timedelta(minutes=n)
        elif unit == "giờ":
            dt = now - timedelta(hours=n)
        elif unit == "ngày":
            dt = now - timedelta(days=n)
        elif unit == "tuần":
            dt = now - timedelta(days=7 * n)
        else:
            dt = now - timedelta(days=30 * n)  # 'tháng'
        return dt.strftime("%d/%m/%Y")

    # Hỗ trợ ISO yyyy-mm-dd.
    try:
        if re.match(r"\d{4}-\d{2}-\d{2}", label):
            dt = datetime.strptime(label, "%Y-%m-%d")
            return dt.strftime("%d/%m/%Y")
    except:
        pass

    return label


def only_meter(text):
    """Tách độ rộng dạng mét từ chuỗi văn bản."""
    if not text:
        return "N/A"
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*m\b", vn_lower(text))
    if not m:
        return "N/A"
    num = m.group(1).replace(",", ".")
    if re.fullmatch(r"\d+\.0+", num):
        num = str(int(float(num)))
    return f"{num}m"


def is_404(driver):
    """Kiểm tra nhanh trang có dấu hiệu 404 hay không."""
    try:
        body = driver.find_element(By.TAG_NAME, "body").text.lower()
        return (
            "404" in body and "không" in body and "thấy" in body
        ) or "not found" in body
    except:
        return False


# Selector card chính của danh sách tin.
CARD_SEL = "div[id^='divlistitemketquatimkiem_'] div[id^='descrip_'].dv-item"


def get_cards(driver, wait):
    """Lấy danh sách card sau khi đợi và cuộn trang."""
    try:
        wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "div[id^='divlistitemketquatimkiem_']")
            )
        )
    except TimeoutException:
        return []
    for _ in range(3):
        cards = driver.find_elements(By.CSS_SELECTOR, CARD_SEL)
        if len(cards) >= 5:
            return cards
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(random.uniform(0.6, 0.9))
    return driver.find_elements(By.CSS_SELECTOR, CARD_SEL)


def get_title(card):
    """Lấy tiêu đề bài đăng."""
    try:
        el = card.find_element(By.CSS_SELECTOR, "a.a-title")
        t = (el.get_attribute("innerText") or el.text or "").strip()
        if t:
            return t
    except:
        pass
    return text_or_na(card, By.CSS_SELECTOR, "a.a-title", attr="title")


def get_price(card):
    """Lấy giá bài đăng từ nhãn chính hoặc fallback text."""
    p = text_or_na(card, By.CSS_SELECTOR, "label.a-txt-cl1")
    if p == "N/A" or not p:
        try:
            el = card.find_element(
                By.XPATH, ".//*[contains(text(),'Tỷ') or contains(text(),'Triệu')]"
            )
            p = el.text.strip()
        except:
            p = "N/A"
    return p


def get_area(card):
    """Lấy diện tích và chuẩn về đơn vị m² khi có thể."""
    raw = text_or_na(card, By.CSS_SELECTOR, "label.a-txt-cl2")
    if raw != "N/A":
        m = re.search(r"(\d+(?:[.,]\d+)?)\s*m(?:2|²)", raw, flags=re.I)
        if m:
            return f"{m.group(1)} m²"
    return raw


def get_address(card):
    """Lấy địa chỉ từ cụm review, fallback N/A."""
    try:
        review1 = card.find_element(By.CSS_SELECTOR, ".review1")
        ex3_spans = review1.find_elements(By.CSS_SELECTOR, "span.ex3")
        if len(ex3_spans) > 1:
            text = ex3_spans[-1].text.strip()
            if text:
                return text
    except:
        pass
    return "N/A"


def get_posted(card):
    """Lấy và chuẩn hóa ngày đăng."""
    return first_not_na(
        parse_date_label(text_or_na(card, By.CSS_SELECTOR, "p.time")),
        parse_date_label(text_or_na(card, By.CSS_SELECTOR, "time")),
    )


def get_details(card):
    """Lấy khối mô tả chi tiết để phục vụ trích xuất regex."""
    try:
        review1 = card.find_element(By.CSS_SELECTOR, ".reviewproperty1")
        return (review1.text or "").strip()
    except:
        return (card.text or "").strip()


def get_front(card):
    """Lấy thông tin mặt tiền từ phần mô tả."""
    details = get_details(card)
    m = re.search(r"mặt tiền[^0-9]*([0-9][0-9\.,]*)\s*m", vn_lower(details))
    if m:
        num = m.group(1).replace(",", ".")
        if re.fullmatch(r"\d+\.0+", num):
            num = str(int(float(num)))
        return f"{num}m"
    return "N/A"


def get_alley(card):
    """Lấy thông tin đường vào/lộ giới từ văn bản card."""
    txt = card.text
    m = re.search(r"(đường vào|lộ giới)[^\,\n\r]*", vn_lower(txt))
    if m:
        seg = m.group(0)
        v = only_meter(seg)
        if v != "N/A":
            return v
    m2 = re.search(r"rộng[^0-9]*([0-9][0-9\.,]*)\s*m", vn_lower(txt))
    if m2:
        num = m2.group(1).replace(",", ".")
        if re.fullmatch(r"\d+\.0+", num):
            num = str(int(float(num)))
        return f"{num}m"
    return "N/A"


def get_floors(card):
    """Lấy số tầng hoặc lầu từ mô tả."""
    details = get_details(card)
    m = re.search(r"(\d+)\s*(?:tầng|lầu)\b", vn_lower(details))
    return f"{m.group(1)}" if m else "N/A"


def get_bedrooms(card):
    """Lấy số phòng ngủ từ card."""
    m = re.search(r"(\d+)\s*(phòng\s*ngủ|pn)\b", vn_lower(card.text))
    return m.group(1) if m else "N/A"


def get_wc(card):
    """Lấy số WC/phòng tắm từ card."""
    m = re.search(r"(\d+)\s*(wc|vệ\s*sinh|phòng\s*tắm)\b", vn_lower(card.text))
    return m.group(1) if m else "N/A"


def get_parking(card):
    """Lấy thông tin chỗ để xe hoặc gara."""
    m = re.search(
        r"(\d+\s*chỗ\s*(?:để|đỗ)\s*(?:xe|ô ?tô|ôtô|oto))", card.text, flags=re.I
    )
    if m:
        return m.group(1).strip()
    if re.search(r"\bgara\b", vn_lower(card.text)):
        return "Có"
    return "Không"


def get_type(card):
    """Lấy loại bất động sản từ nội dung card."""
    m = re.search(
        r"(Nhà\s*Mặt\s*Phố|Nhà\s*Mặt\s*Tiền|Nhà\s*Phố|Nhà\s*Riêng|Căn\s*Hộ\s*Chung\s*Cư|Chung\s*Cư|Căn\s*Hộ|Đất\s*Nền|Đất)",
        card.text,
        flags=re.I,
    )
    return m.group(1) if m else "N/A"


def extract_row(card):
    """Ghép dữ liệu card thành một dòng CSV theo đúng schema."""
    return [
        get_title(card),  # Tiêu đề
        get_price(card),  # Giá
        get_area(card),  # Diện tích
        get_front(card),  # Mặt tiền
        get_alley(card),  # Đường vào
        get_floors(card),  # Số tầng
        get_bedrooms(card),  # Số phòng ngủ
        get_wc(card),  # Số WC
        get_parking(card),  # Chỗ để xe
        get_type(card),  # Loại BĐS
        get_address(card),  # Địa chỉ
        get_posted(card),  # Ngày đăng
    ]


def ensure_csv_header(path: str):
    """Tạo header CSV khi file chưa tồn tại hoặc đang rỗng."""
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f)
            w.writerow(
                [
                    "Tiêu đề",
                    "Giá",
                    "Diện tích",
                    "Mặt tiền",
                    "Đường vào",
                    "Số tầng",
                    "Số phòng ngủ",
                    "Số WC",
                    "Chỗ để xe",
                    "Loại BĐS",
                    "Địa chỉ",
                    "Ngày đăng",
                ]
            )


def read_checkpoint() -> int:
    """Đọc checkpoint để resume, lỗi thì fallback trang 1."""
    if os.path.exists(CHECKPOINT):
        try:
            with open(CHECKPOINT, "r", encoding="utf-8") as f:
                return max(1, int(f.read().strip()))
        except:
            return 1
    return 1


def write_checkpoint(page: int):
    """Ghi checkpoint sau khi xử lý xong mỗi trang."""
    with open(CHECKPOINT, "w", encoding="utf-8") as f:
        f.write(str(page))


def is_good_cards(cards):
    """Đánh giá nhanh chất lượng danh sách card theo số lượng tối thiểu."""
    return len(cards) >= 10


# def page_url(page:int) -> str:
#     if page <= 1: return BASE
#     return f"{BASE}?page={page}"
def page_url(page: int) -> str:
    """Tạo URL theo cấu trúc /pageX của site."""
    # Đảm bảo URL gốc không có dấu / ở cuối
    base_url = BASE.rstrip("/")
    if page <= 1:
        return base_url
    # Nối với /pageX cho các trang tiếp theo
    return f"{base_url}/page{page}"


def _clear_storage(driver):
    """Xóa local/session storage để giảm tác động điều hướng cũ."""
    driver.get(BASE)
    time.sleep(0.5)
    try:
        driver.execute_script("localStorage.clear(); sessionStorage.clear();")
    except Exception:
        pass


def _ensure_url(driver, url, wait):
    """Điều hướng URL mục tiêu và đợi phần tử danh sách xuất hiện."""
    driver.get(url)
    time.sleep(random.uniform(0.6, 1.0))
    if "?page=" in url and str(driver.current_url).rstrip("/") != url.rstrip("/"):
        try:
            driver.execute_script("window.stop(); location.href = arguments[0];", url)
            time.sleep(random.uniform(0.6, 1.0))
        except Exception:
            pass
    try:
        wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "div[id^='divlistitemketquatimkiem_']")
            )
        )
    except TimeoutException:
        pass


def _active_page_text(driver):
    """Đọc số trang active trên thanh phân trang (nếu có)."""
    try:
        act = driver.find_element(
            By.CSS_SELECTOR,
            "button.active, li.active, .pagination .active, .a-paging .active",
        )
        t = (act.text or act.get_attribute("innerText") or "").strip()
        return re.sub(r"\D", "", t)
    except Exception:
        return ""


def _click_page_button(driver, page):
    """Thử bấm trực tiếp số trang trên thanh phân trang."""
    sel_candidates = [
        f"//a[normalize-space()='{page}']",
        f"//button[normalize-space()='{page}']",
        f"//li[.//a[normalize-space()='{page}']]",
    ]
    for xp in sel_candidates:
        try:
            el = WebDriverWait(driver, 3).until(
                EC.element_to_be_clickable((By.XPATH, xp))
            )
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            el.click()
            return True
        except Exception:
            continue
    return False


def nav_to_page(driver, wait, page):
    """Điều hướng đến trang mục tiêu bằng URL + fallback click pagination."""
    target = page_url(page)
    # _clear_storage(driver)
    ts = int(time.time())
    if "?" in target:
        target = f"{target}&_ts={ts}"
    else:
        target = f"{target}?_ts={ts}"

    _ensure_url(driver, target, wait)

    ok_by_url = f"?page={page}" in driver.current_url
    ok_by_ui = (_active_page_text(driver) == str(page)) if page > 1 else True

    if not (ok_by_url and ok_by_ui):
        if _click_page_button(driver, page):
            time.sleep(random.uniform(0.8, 1.2))
        try:
            wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, CARD_SEL)))
        except TimeoutException:
            pass

    act = _active_page_text(driver)
    if (
        page > 1
        and act not in (str(page), "")
        and f"?page={page}" not in driver.current_url
    ):
        print(f"Site dang ep ve trang {act or '1'} (JS redirect). Da dung fallback click.")


def main():
    """Vòng lặp crawl chính: đọc card, ghi CSV, cập nhật checkpoint."""
    driver = None
    try:
        driver = init_driver()
        wait = WebDriverWait(driver, WAIT_TIMEOUT)
        csv_path = os.path.join(os.getcwd(), OUTPUT_FILE)
        ensure_csv_header(csv_path)

        page = read_checkpoint()
        processed = 0

        # Điều hướng đến trang bắt đầu từ checkpoint.
        start_url = page_url(page)
        print(f"Bat dau tu trang {page}: {start_url}")
        driver.get(start_url)
        wait_cards(driver)  # Chờ các tin đăng đầu tiên xuất hiện.

        while page <= SAFETY_CAP:
            print(f"==> Đang xử lý trang {page}")

            # Đảm bảo URL hiện tại khớp trang đang xử lý, nếu không thì tải lại.
            if f"page={page}" not in driver.current_url and page > 1:
                print(f"URL khong khop, dieu huong lai toi trang {page}...")
                driver.get(page_url(page))
                wait_cards(driver)

            cards = get_cards(driver, wait)
            if not cards:
                print("Het du lieu (khong con card). Dung.")
                break

            print(f"Số bài đăng phát hiện: {len(cards)}")

            with open(csv_path, "a", encoding="utf-8-sig", newline="") as f:
                w = csv.writer(f)
                for c in cards:
                    row = extract_row(c)
                    if row[0] == "N/A" or row[1] == "N/A":
                        continue
                    w.writerow(row)
                    processed += 1
                # Ghi file sau mỗi trang để an toàn.
                f.flush()
                try:
                    os.fsync(f.fileno())
                except:
                    pass

            write_checkpoint(page)
            print(f"Da ghi checkpoint cho trang {page}. Tong so dong da ghi: {processed}\n")

            # Chuyển trang bằng nút next trên thanh phân trang.
            try:
                # 1) Tìm nút next.
                next_button = wait.until(
                    EC.element_to_be_clickable((By.XPATH, "//a[normalize-space()='»']"))
                )

                # 2) Cuộn tới nút và click an toàn bằng JavaScript.
                driver.execute_script(
                    "arguments[0].scrollIntoView({block: 'center', inline: 'nearest'});",
                    next_button,
                )
                time.sleep(0.5)
                driver.execute_script("arguments[0].click();", next_button)

                page += 1

                # 3) Chờ trang mới tải xong.
                wait_cards(driver)
                time.sleep(random.uniform(1.5, 2.5))

            except TimeoutException:
                print("Khong tim thay nut next. Co the da o trang cuoi. Dung.")
                break
            except Exception as e:
                print(f"Loi khi nhan nut next: {e}. Dung.")
                break

        print(f"Hoan tat. Da ghi {processed} dong vao {csv_path}")

    except KeyboardInterrupt:
        print("\nDung boi nguoi dung. Co the chay lai tu checkpoint.")
    finally:
        if driver:
            safe_quit(driver)


if __name__ == "__main__":
    main()
