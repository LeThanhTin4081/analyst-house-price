import pandas as pd, os

base_dir = os.path.dirname(__file__)
file1 = os.path.join(base_dir, "alonhadat_raw1.csv")
file2 = os.path.join(base_dir, "alonhadat_raw2.csv")

df1 = pd.read_csv(file1)
df2 = pd.read_csv(file2)

merged = pd.concat([df1, df2], ignore_index=True)
merged.drop_duplicates(inplace=True)
merged.to_csv(os.path.join(base_dir, "raw_data_alonhatdat_full.csv"), index=False, encoding="utf-8-sig")

print(f" Gộp xong. Tổng {len(merged)} dòng được lưu trong raw_data_bds_full.csv")
