import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException, TimeoutException
from datetime import datetime, timedelta
import time, random, csv, os, re, tempfile, shutil, unicodedata
import random as rnd

# Cấu hình crawler Meeyland (TP.HCM).
PROFILE_DIR   = r"C:\\selenium_profiles\\meeyland_profile_hcm"
BASE          = "https://meeyland.com/mua-ban-nha-dat-ho-chi-minh-b43?page={page}"
OUTPUT_FILE   = "meeyland_raw.csv"
CHECKPOINT    = "meeyland_checkpoint.txt"
WAIT_TIMEOUT  = 20
SAFETY_CAP    = 20000

# Cấu hình trình duyệt.
HEADLESS      = False
CHROME_MAJOR  = 141
USE_TEMP_PROFILE = True

os.makedirs(PROFILE_DIR, exist_ok=True)

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
    """Khởi tạo driver UC và retry khi lỗi khởi tạo."""
    tmp_profile = tempfile.mkdtemp(prefix="meey_uc_") if USE_TEMP_PROFILE else None
    opts = _make_options(tmp_profile)
    last_err = None
    for attempt in range(1, 3 + 1):
        try:
            drv = uc.Chrome(options=opts, version_main=CHROME_MAJOR, use_subprocess=True, suppress_welcome=True)
            drv._tmp_profile_dir = tmp_profile
            return drv
        except WebDriverException as e:
            print(f"Lỗi khởi tạo driver lần {attempt}, thử lại..."); last_err = e; time.sleep(2 * attempt)
    if tmp_profile: shutil.rmtree(tmp_profile, ignore_errors=True)
    raise last_err

def safe_quit(driver):
    """Đóng driver an toàn và xóa profile tạm nếu có."""
    try: driver.quit()
    except: pass
    tmp = getattr(driver, "_tmp_profile_dir", None)
    if tmp and os.path.isdir(tmp): shutil.rmtree(tmp, ignore_errors=True)

def read_checkpoint() -> int:
    """Đọc checkpoint để resume; nếu lỗi thì trả về trang 1."""
    if not os.path.exists(CHECKPOINT): return 1
    try:
        with open(CHECKPOINT, "r", encoding="utf-8") as f:
            return max(1, int(f.read().strip()))
    except:
        return 1

def write_checkpoint(page: int):
    """Ghi checkpoint sau khi xử lý xong từng trang."""
    with open(CHECKPOINT, "w", encoding="utf-8") as f:
        f.write(str(page))

def ensure_csv_header(path: str):
    """Tạo header CSV khi file chưa tồn tại hoặc đang rỗng."""
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f)
            w.writerow(["Tiêu đề", "Giá", "Diện tích", "Giá/m2", "Số phòng ngủ", "Số phòng tắm", "Địa chỉ", "Ngày đăng"])

def text_or_na(ele, by, sel):
    """Lấy text theo selector; lỗi thì trả N/A."""
    try:
        t = ele.find_element(by, sel).text.strip()
        return t if t else "N/A"
    except:
        return "N/A"

def vn_lower(s): 
    """Chuẩn hóa Unicode và chuyển chữ thường tiếng Việt."""
    return unicodedata.normalize("NFC", (s or "")).lower()

def clean_spaces(s): 
    """Chuẩn hóa khoảng trắng dư thừa trong chuỗi."""
    return re.sub(r"\s+", " ", (s or "").strip())

# Regex thường dùng.
RE_AREA      = re.compile(r"\b(\d+(?:[\.,]\d+)*)\s*m²\b", re.I)
RE_PN        = re.compile(r"(\d+)\s*(?:pn|phòng ngủ)\b", re.I)
RE_WC        = re.compile(r"(\d+)\s*(?:wc|phòng tắm|toilet)\b", re.I)
RE_GIAm2     = re.compile(r"([\d.,]+\s*tr/m²)", re.I)

def parse_date_label(label):
    """Chuẩn hóa ngày đăng: hỗ trợ nhãn tương đối và đổi về dd/MM/YYYY."""
    label = (label or "").strip()
    if not label:
        return "N/A"

    low = vn_lower(label)
    now = datetime.now()

    if "hôm nay" in low:
        return now.strftime("%d/%m/%Y")
    if "hôm qua" in low:
        return (now - timedelta(days=1)).strftime("%d/%m/%Y")

    m = re.search(r"(\d+)\s+(giây|phút|tiếng|giờ|ngày|tuần|tháng)\s+trước", low)
    if m:
        n = int(m.group(1))
        unit = m.group(2)
        if unit == "giây":
            dt = now - timedelta(seconds=n)
        elif unit == "phút":
            dt = now - timedelta(minutes=n)
        elif unit in ("tiếng", "giờ"):
            dt = now - timedelta(hours=n)
        elif unit == "ngày":
            dt = now - timedelta(days=n)
        elif unit == "tuần":
            dt = now - timedelta(weeks=n)
        else:  # 'tháng' ~ 30 ngày
            dt = now - timedelta(days=30 * n)
        return dt.strftime("%d/%m/%Y")

    # Không khớp mẫu nào -> giữ nguyên
    return label

# Selector card chính của danh sách tin.
CARD_SELECTOR = "article.relative"

def get_cards(driver, wait):
    """Đợi card xuất hiện, cuộn nhẹ và trả về danh sách card."""
    try:
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, CARD_SELECTOR)))
    except TimeoutException:
        return []
    for _ in range(3):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)
    return driver.find_elements(By.CSS_SELECTOR, CARD_SELECTOR)

def get_title(card):
    """Lấy tiêu đề bài đăng từ các selector ưu tiên."""
    # Tiêu đề nằm trong H3 có class line-clamp/typography
    for xp in [
        ".//h3[contains(@class,'line-clamp')]",
        ".//h3[contains(@class,'meey-sub') or contains(@class,'font-medium') or contains(@class,'text-fs-14')]",
    ]:
        try:
            e = card.find_element(By.XPATH, xp)
            t = (e.get_attribute("textContent") or e.text or "").strip()
            if t:
                return clean_spaces(t)
        except:
            pass
    return "N/A"

def _info_row(card):
    """Tìm hàng thông tin chứa diện tích/giá m2/PN/WC trong card."""
    """
    Tìm hàng info (chứa các ô: tr/m², m², PN, WC).
    Không bám class cứng; dùng tiêu chí: là div flex + flex-wrap và có chữ 'm²' hoặc 'tr/m²'.
    """
    # 1) ứng viên: mọi div có flex + flex-wrap
    rows = card.find_elements(
        By.XPATH,
        ".//div[contains(@class,'flex') and contains(@class,'flex-wrap')]"
    )
    for r in rows:
        t = (r.text or "").strip()
        # hàng info luôn có m² hoặc tr/m²
        if "m²" in t or "tr/m²" in t:
            return r
    # fallback: trả cả card (để regex vẫn chạy được)
    return card

def get_price(card):
    """Lấy giá bài đăng từ các selector theo theme."""
    # Giá màu đỏ/thay class theo theme → thử nhiều selector
    price = text_or_na(card, By.CSS_SELECTOR, "div.text-error-600")
    if price == "N/A":
        price = text_or_na(card, By.CSS_SELECTOR, ".text-error, .text-danger, .text-red-600, .text-primary-600")
    return clean_spaces(price)

def get_price_per_m2(card):
    """Lấy giá theo m2 từ text trực tiếp hoặc regex fallback."""
    row = _info_row(card)

    # 1) phần tử có text 'tr/m²' trong row
    try:
        e = row.find_element(By.XPATH, ".//*[contains(normalize-space(.),'tr/m²')]")
        val = (e.text or "").strip()
        if val:
            return val
    except:
        pass

    # 2) các class chữ nhỏ hay dùng cho 'tr/m²'
    for css in [".meey-caption-10r", ".text-fs-12", ".text-secondary-600", ".qmd\\:text-fs-12"]:
        try:
            for e in row.find_elements(By.CSS_SELECTOR, css):
                t = (e.text or "").strip()
                if "tr/m²" in t:
                    return t
        except:
            pass

    # 3) regex fallback
    m = re.search(r"([\d.,]+\s*tr/m²)", row.text, re.I)
    return m.group(1) if m else "N/A"

def get_area(card):
    """Lấy diện tích từ tooltip hoặc regex fallback."""
    # Ưu tiên selector tooltip nếu còn; fallback regex từ card.text
    area = text_or_na(card, By.CSS_SELECTOR, 'div[data-tippy-content="Diện tích"] span')
    if area == "N/A":
        m = RE_AREA.search(card.text)
        area = m.group(1) + " m²" if m else "N/A"
    return area

def get_bedrooms(card):
    """Lấy số phòng ngủ từ tooltip/icon hoặc regex fallback."""
    # 1) Tooltip "Số phòng ngủ" -> lấy span; ưu tiên attribute title
    try:
        e = card.find_element(By.CSS_SELECTOR, '[data-tippy-content*="Số phòng ngủ"] span')
        val = (e.get_attribute("title") or e.text or "").strip()
        if val:
            return val
    except:
        pass

    # 2) Icon giường -> node kế bên
    try:
        e = card.find_element(By.XPATH, ".//i[contains(@class,'bed')]/following-sibling::*[1]")
        val = (e.get_attribute("title") or e.text or "").strip()
        if val:
            return val
    except:
        pass

    # 3) Regex trên toàn card
    m = re.search(r"(\d+)\s*(?:pn|phòng ngủ)\b", card.text, re.I)
    return m.group(1) if m else "N/A"


def get_bathrooms(card):
    """Lấy số phòng tắm từ tooltip/icon hoặc regex fallback."""
    # 1) Tooltip "Số phòng tắm"
    try:
        e = card.find_element(By.CSS_SELECTOR, '[data-tippy-content*="Số phòng tắm"] span')
        val = (e.get_attribute("title") or e.text or "").strip()
        if val:
            return val
    except:
        pass

    # 2) Icon bồn tắm -> node kế bên
    try:
        e = card.find_element(By.XPATH, ".//i[contains(@class,'bathtub') or contains(@class,'bath')]/following-sibling::*[1]")
        val = (e.get_attribute("title") or e.text or "").strip()
        if val:
            return val
    except:
        pass

    # 3) Regex trên toàn card
    m = re.search(r"(\d+)\s*(?:wc|phòng tắm|toilet)\b", card.text, re.I)
    return m.group(1) if m else "N/A"



def get_address(card):
    """Lấy địa chỉ từ node location, fallback dòng chứa Hồ Chí Minh."""
    # Ưu tiên p có icon location
    try:
        e = card.find_element(By.XPATH, ".//p[i[contains(@class,'location') or contains(@class,'lml-location')]]")
        t = e.text.strip()
        if t: return clean_spaces(t)
    except: 
        pass
    # Fallback: lấy dòng CUỐI có 'Hồ Chí Minh'
    lines = [clean_spaces(ln) for ln in card.text.split("\n") if ln.strip()]
    cand = None
    for ln in lines:
        if "Hồ Chí Minh" in ln or "Tp. Hồ Chí Minh" in ln or "TP. Hồ Chí Minh" in ln:
            cand = ln
    return cand if cand else "N/A"

def get_posted_date(card):
    """Lấy nhãn ngày đăng và chuẩn hóa về định dạng thống nhất."""
    raw = "N/A"
    for xp in [
        ".//span[contains(.,'trước')]",
        ".//span[contains(.,'Hôm nay') or contains(.,'Hôm qua')]",
        ".//*[self::span or self::p or self::div][contains(.,'trước') or contains(.,'Hôm nay') or contains(.,'Hôm qua')]"
    ]:
        try:
            raw = card.find_element(By.XPATH, xp).text.strip()
            if raw: break
        except:
            pass
    return parse_date_label(raw)

def extract_row(card):
    """Ghép dữ liệu từ card thành một dòng theo schema CSV."""
    return [
        get_title(card), get_price(card), get_area(card),
        get_price_per_m2(card), get_bedrooms(card), get_bathrooms(card),
        get_address(card), get_posted_date(card),
    ]

def main():
    """Vòng lặp crawl chính: lấy dữ liệu, ghi CSV, cập nhật checkpoint."""
    driver = None
    try:
        driver = init_driver()
        wait = WebDriverWait(driver, WAIT_TIMEOUT)
        ensure_csv_header(OUTPUT_FILE)
        page = read_checkpoint()
        total_processed = 0

        while page <= SAFETY_CAP:
            url = BASE.format(page=page)
            print(f"Dang xu ly trang {page}: {url}")
            try:
                driver.get(url)
            except Exception as e:
                print(f"Lỗi khi tải URL: {e}. Thử lại sau 5s...")
                time.sleep(5)
                continue

            cards = get_cards(driver, wait)
            if not cards:
                if "Rất tiếc, trang bạn tìm kiếm không tồn tại" in driver.page_source:
                    print("Trang khong ton tai. Da den trang cuoi cung.")
                else:
                    print("Khong tim thay bai dang nao. Co the da het du lieu hoac bi chan.")
                break

            print(f"Phat hien {len(cards)} bai dang.")
            newly_added = 0
            with open(OUTPUT_FILE, "a", encoding="utf-8-sig", newline="") as f:
                w = csv.writer(f)
                for card in cards:
                    row = extract_row(card)
                    # ghi nếu có Title hoặc Giá
                    if row[0] != "N/A" or row[1] != "N/A":
                        w.writerow(row)
                        newly_added += 1
                f.flush()
                try: os.fsync(f.fileno())
                except: pass

            total_processed += newly_added
            print(f"Da ghi {newly_added} dong moi. Tong so: {total_processed}")
            write_checkpoint(page)
            print(f"Da luu checkpoint tai trang {page}.\n")
            page += 1
            time.sleep(rnd.uniform(2.0, 3.5))

        print(f"Hoan tat! Tong cong da ghi {total_processed} dong vao file {OUTPUT_FILE}")
    except KeyboardInterrupt:
        print("\nTam dung boi nguoi dung. Chay lai script de tiep tuc tu checkpoint.")
    except Exception as e:
        print(f"Da xay ra loi nghiem trong: {e}")
    finally:
        if driver: safe_quit(driver)

if __name__ == "__main__":
    main()
