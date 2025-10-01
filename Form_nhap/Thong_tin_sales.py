import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import hashlib
import time
import schedule
from typing import Dict, Any
import gspread

gc = gspread.service_account(filename='D:/PhanVanTruong_Work/Database_Know/Form_nhap/config/gg_sheet_config/credentials.json')
EXCEL_FILE = gc.open_by_key("1mtHFadsGK6KPICPqCE4f6lvvEydNJSv-tVfzTD_axjg")

def parse_date(val, field, idx):
    if pd.isna(val) or str(val).strip() == "":
        return None
    try:
        dt = pd.to_datetime(val, errors="coerce", format="%d/%m/%Y")
        if pd.isna(dt):
            return None
        return dt.to_pydatetime()
    except Exception:
        print(f"[WARNING] Row {idx} : Invalid date in {field} -> {val}")
        return None

def safe_int(val):
    if pd.isna(val):
        return None
    val_str = str(val).strip()
    return int(val_str) if val_str else None

def get_json_data():
    worksheet = EXCEL_FILE.worksheet("Thông tin sale")
    data = worksheet.get_all_records()
    df = pd.DataFrame(data)
    df.columns = [
        "thoi_gian_ghi", "sale_phu_trach", "ma_hop_dong",
        "ten_khach_hang", "sdt", "loai_don", "tinh/thanhpho",
        "quan/huyen", "phuong/xa", "dia_chi_giao_hang", "ngay_lap_dat_du_kien",
        "ten_san_pham", "kich_thuoc", "so_luong", "anh", "ghi_chu"
    ]
    json_data = []
    current_doc = None

    for idx, row in df.iterrows():
        ma_hd = str(row["ma_hop_dong"]).strip() if not pd.isna(row["ma_hop_dong"]) else ""

        if ma_hd:
            if current_doc:
                json_data.append(current_doc)

            thoi_gian_ghi = parse_date(row["thoi_gian_ghi"], "thoi_gian_ghi", idx)
            ngay_du_kien = parse_date(row["ngay_lap_dat_du_kien"], "ngay_lap_dat_du_kien", idx)
            document_id = hashlib.sha256(ma_hd.encode("utf-8")).hexdigest()

            current_doc = {
                "documentId": document_id,
                "data": {
                    "thongtindonhang": {
                        "sale_phu_trach": row["sale_phu_trach"] if not pd.isna(row["sale_phu_trach"]) else None,
                        "ma_hop_dong": ma_hd,
                        "ten_khach_hang": row["ten_khach_hang"] if not pd.isna(row["ten_khach_hang"]) else None,
                        "sdt": row["sdt"] if not pd.isna(row["sdt"]) else None,
                        "loai_don": row["loai_don"] if not pd.isna(row["loai_don"]) else None,
                        "tinh/thanhpho": row["tinh/thanhpho"] if not pd.isna(row["tinh/thanhpho"]) else None,
                        "quan/huyen": row["quan/huyen"] if not pd.isna(row["quan/huyen"]) else None,
                        "phuong/xa": row["phuong/xa"] if not pd.isna(row["phuong/xa"]) else None,
                        "dia_chi_giao_hang": row["dia_chi_giao_hang"] if not pd.isna(row["dia_chi_giao_hang"]) else None,
                        "ngay_lap_dat_du_kien": ngay_du_kien
                    },
                    "SanPham": []
                },
                "metadata": {
                    "createdAt": datetime.now(),
                    "action": "create"
                }
            }
            
        if current_doc:
            sp = {
                "thoi_gian_ghi": thoi_gian_ghi,
                "ten_san_pham": row["ten_san_pham"] if not pd.isna(row["ten_san_pham"]) else None,
                "kich_thuoc": row["kich_thuoc"] if not pd.isna(row["kich_thuoc"]) else None,
                "so_luong": row["so_luong"] if not pd.isna(row["so_luong"]) else None,
                "anh": row["anh"] if not pd.isna(row["anh"]) else None,
                "ghi_chu": row["ghi_chu"] if not pd.isna(row["ghi_chu"]) else None
            }
            current_doc["data"]["SanPham"].append(sp)

    if current_doc:
        json_data.append(current_doc)

    return json_data

def create_indexes(db):
    col = db["tamop_thong_tin_sales_nhap"]
    col.create_index("documentId", unique = True)
    print("[INFO] Indexes created successfully")

def detect_changes_and_overwrite(db, document_id: str, new_data: Dict, existing_doc: Dict) -> bool:
    existing_data = existing_doc.get("data", {})
    if new_data == existing_data:
        return False

    print(f"[INFO] Change detected for {document_id}")
    col = db["tamop_thong_tin_sales_nhap"]

    col.update_one(
        {"_id": existing_doc["_id"]},
        {"$set": {
            "data": new_data,
            "metadata.updatedAt": datetime.now(),
            "metadata.action": "update"
        }}
    )
    return True

def update_database():
    client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=5000)
    db = client["LucThuy_Base"]

    try:
        client.admin.command("ping")
        print("[INFO] Connected to MongoDB")
        create_indexes(db)
        json_data = get_json_data()
        print(f"[INFO] Loaded {len(json_data)} documents from Excel")

        col = db["tamop_thong_tin_sales_nhap"]
        existing_docs = list(col.find({}, {"documentId": 1, "data": 1}))
        existing_map = {doc["documentId"]: doc for doc in existing_docs}

        success, created, updated = 0, 0, 0

        for doc in json_data:
            doc_id = doc["documentId"]
            new_data = doc["data"]
            if doc_id in existing_map:
                if detect_changes_and_overwrite(db, doc_id, new_data, existing_map[doc_id]):
                    updated += 1
            else:
                col.insert_one(doc)
                created += 1
            success += 1

        print(f"[INFO] Done. Success: {success}, Created: {created}, Updated: {updated}")

    except Exception as e:
        print(f"[ERROR] {e}")
    finally:
        client.close()
        print("[🔌] MongoDB connection closed")

def main():
    schedule.every(5).minutes.do(update_database)
    update_database()
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    update_database()
