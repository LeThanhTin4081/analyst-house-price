import pandas as pd
from pathlib import Path

BASE = Path(__file__).resolve().parent          # thư mục chứa file .py
INPUT = BASE / "meeyland_raw.csv"               #file goc
DEDUP = BASE / "raw_data_meeyland_full.csv" #file sau khi loc trung
DUPS  = BASE / "meeyland_raw_filetrungs.csv" #file cac dong bi trung

# Nếu chưa đúng, in thông tin để debug
print("CWD:", Path.cwd())
print("Script dir:", BASE)
print("Input path:", INPUT)
if not INPUT.exists():
    raise FileNotFoundError(f"Không thấy file: {INPUT}")

# Đọc CSV (có BOM tiếng Việt dùng utf-8-sig)
df = pd.read_csv(INPUT, encoding="utf-8-sig")

total_rows = len(df)

# Đánh dấu mọi dòng thuộc các nhóm trùng tuyệt đối (so sánh toàn bộ cột)
dup_mask_all = df.duplicated(keep=False)

# Số dòng nằm trong nhóm trùng (tính cả “bản gốc” lẫn “bản lặp”)
num_rows_in_dup_groups = int(dup_mask_all.sum())

# Số bản sao sẽ bị loại khi drop_duplicates (giữ 1 bản duy nhất cho mỗi nhóm)
num_removed = int(total_rows - len(df.drop_duplicates()))


print(f"Tổng số dòng: {total_rows}")
print(f"Số dòng thuộc các nhóm trùng: {num_rows_in_dup_groups}")
print(f"Số bản sao sẽ bị loại khi dedup: {num_removed}")

# Xuất file đã loại trùng
df_dedup = df.drop_duplicates()
df_dedup.to_csv(DEDUP, index=False, encoding="utf-8-sig")
print(f"Đã ghi file sạch: {DEDUP} (còn {len(df_dedup)} dòng)")

# (Tuỳ chọn) xuất riêng danh sách các dòng trùng để kiểm tra
if num_rows_in_dup_groups:
    df[dup_mask_all].to_csv(DUPS, index=False, encoding="utf-8-sig")
    print(f"Đã ghi danh sách các dòng trùng: {DUPS}")
else:
    print("Không có nhóm trùng nào.")
