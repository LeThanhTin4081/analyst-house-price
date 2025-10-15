# coding: utf-8
"""
Gộp 4 file CSV theo schema CHUẨN, loại trùng tên cột giữa các trang.
- HỢP NHẤT:
  * "Tiêu đề dự án" -> "Tiêu đề"
  * "Giá/m²" -> "Giá/m2"
  * "Số WC" -> "Số phòng vệ sinh"
  * "Đường trước nhà" -> "Đường vào"
- KHÔNG thêm cột ngoài schema, chỉ thêm "Nguồn" ở CUỐI.
- Thiếu cột/ô -> "NA".
Output: data/file_clean/merged_all_sites.csv
"""

import os
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ====== Đường dẫn file nguồn theo site ======
FILES = {
    "alonhadat":  "raw_data_alonhatdat_full.csv",
    "batdongsan": "raw_data_bds_full.csv",
    "meeyland":   "raw_data_meeyland_full.csv",
    "nhadat24h":  "raw_data_nhadat24h_full.csv",
}

# ====== SCHEMA CHUẨN DUY NHẤT (thứ tự output) ======
CANON_SCHEMA = [
    "Tiêu đề","Giá","Diện tích","Giá/m2","Địa chỉ","Ngày đăng",
    "Số phòng ngủ","Số phòng vệ sinh","Số phòng tắm",
    "Số tầng","Chỗ để xe","Kích thước","Mặt tiền","Đường vào","Loại BĐS"
]
OUTPUT_COLS = CANON_SCHEMA + ["Nguồn"]

# ====== BẢN ĐỒ ALIAS -> TÊN CHUẨN (áp cho MỌI site) ======
# Chỉ map những cặp gây trùng tên. Các cột khác nếu đã trùng đúng tên chuẩn thì giữ nguyên.
ALIAS_TO_CANON = {
    "Tiêu đề dự án": "Tiêu đề",
    "Giá/m²": "Giá/m2",
    "Số WC": "Số phòng vệ sinh",
    "Đường trước nhà": "Đường vào",
    # Các tên chuẩn tự nó map về chính nó để khỏi phải if-check
    "Tiêu đề": "Tiêu đề",
    "Giá": "Giá",
    "Diện tích": "Diện tích",
    "Giá/m2": "Giá/m2",
    "Địa chỉ": "Địa chỉ",
    "Ngày đăng": "Ngày đăng",
    "Số phòng ngủ": "Số phòng ngủ",
    "Số phòng vệ sinh": "Số phòng vệ sinh",
    "Số phòng tắm": "Số phòng tắm",
    "Số tầng": "Số tầng",
    "Chỗ để xe": "Chỗ để xe",
    "Kích thước": "Kích thước",
    "Mặt tiền": "Mặt tiền",
    "Đường vào": "Đường vào",
    "Loại BĐS": "Loại BĐS",
}

READ_KW = dict(
    encoding="utf-8-sig",
    dtype=str,
    keep_default_na=False,
    na_values=["", "NA", "NaN", None]
)

def read_csv_any(path):
    try:
        return pd.read_csv(path, **READ_KW)
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="cp1258", dtype=str, keep_default_na=False,
                           na_values=["", "NA", "NaN", None])

def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    """Chuẩn hoá tên cột: strip khoảng trắng, chuẩn 'm²' -> 'm2' cho header, map alias -> tên chuẩn."""
    cols = []
    for c in df.columns:
        c0 = (c or "").strip()
        # Chuẩn m² -> m2 trong header để tránh sinh thêm cột mới
        c0 = c0.replace("m²", "m2")
        # Map alias -> tên chuẩn nếu có
        c0 = ALIAS_TO_CANON.get(c0, c0)
        cols.append(c0)
    df.columns = cols
    return df

def keep_pad_reorder(df: pd.DataFrame, site_key: str) -> pd.DataFrame:
    """Chỉ giữ các cột trong CANON_SCHEMA, thêm cột còn thiếu = 'NA', reorder, thêm Nguồn cuối."""
    # Chỉ giữ cột chuẩn có trong df
    use_cols = [c for c in CANON_SCHEMA if c in df.columns]
    df2 = df[use_cols].copy()

    # Thêm cột thiếu
    for c in CANON_SCHEMA:
        if c not in df2.columns:
            df2[c] = "NA"

    # Reorder đúng chuẩn
    df2 = df2[CANON_SCHEMA]

    # Chuẩn hoá giá trị trống
    df2 = df2.fillna("NA")
    df2.replace(to_replace=[r"^\s*$"], value="NA", regex=True, inplace=True)

    # Gắn nguồn ở cuối
    df2["Nguồn"] = site_key
    df2 = df2[OUTPUT_COLS]
    return df2

def main():
    frames = []
    for site, fname in FILES.items():
        path = os.path.join(BASE_DIR, fname)
        if not os.path.exists(path):
            print(f"[WARN] Thiếu file: {fname}")
            continue

        df = read_csv_any(path)
        df = normalize_headers(df)

        # Loại bỏ mọi cột không thuộc schema chuẩn (tránh sinh cột lạ)
        keep = [c for c in df.columns if c in CANON_SCHEMA]
        df = df[keep].copy()

        frames.append(keep_pad_reorder(df, site))

    if not frames:
        print("[ERR] Không có dữ liệu để gộp.")
        return

    merged = pd.concat(frames, ignore_index=True)



    out_path = os.path.join(BASE_DIR, "fulldata_raw_4web_rawchuahoanchinh.csv") 

    merged.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"[OK] Gộp xong -> {out_path} ({len(merged):,} dòng)")
    print("[INFO] Schema đã chuẩn hoá, không còn trùng tên cột giữa các trang.")
    print("[INFO] Các alias đã map về tên chuẩn: "
          "'Tiêu đề dự án'->'Tiêu đề', 'Giá/m²'->'Giá/m2', 'Số WC'->'Số phòng vệ sinh', 'Đường trước nhà'->'Đường vào'.")

if __name__ == "__main__":
    main()
