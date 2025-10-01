import pandas as pd
from pymongo import MongoClient, errors
from datetime import datetime
import hashlib
import time
import schedule
from typing import Dict, Any
import gspread


# ======= Kết nối Google Sheets =======
gc = gspread.service_account(
    filename='D:/PhanVanTruong_Work/Database_Know/Form_nhap/config/gg_sheet_config/credentials.json'
)
EXCEL_FILE = gc.open_by_key("1fEo-yEWRR5BQiEk8Io0lmeLRlqIp8yyuetLYLtYuCO4")


# ======= Helper =======
def parse_date(val, field, idx):
    """Parse date từ Google Sheet"""
    if pd.isna(val) or str(val).strip() == "":
        return None
    try:
        dt = pd.to_datetime(val, errors="coerce", dayfirst=True)
        return None if pd.isna(dt) else dt.to_pydatetime()
    except Exception:
        print(f"[WARNING] Row {idx} : Invalid date in {field} -> {val}")
        return None


def safe_str(val):
    """Chuyển giá trị thành str an toàn"""
    return str(val).strip() if not pd.isna(val) and str(val).strip() != "" else None


def safe_float(val):
    """Chuyển giá trị thành float an toàn"""
    if pd.isna(val) or str(val).strip() == "":
        return None
    try:
        return float(str(val).replace(",", "").strip())
    except Exception:
        return None


# ======= Lấy dữ liệu từ Sheet =======
def get_json_data():
    worksheet = EXCEL_FILE.worksheet("ChiLT")  # đổi đúng tên sheet
    data = worksheet.get_all_records()
    df = pd.DataFrame(data)

    df.columns = [
        "Timestamp", "ma_hop_dong", "li_do_chi", "mo_ta",
        "ten_tai_khoan", "ngan_hang", "so_tai_khoan", "so_tien",
        "han_cuoi_thanh_toan", "nguoi_tao_lenh", "minh_chung", "noi_dung"
    ]

    json_data = []
    for idx, row in df.iterrows():
        ma_hd = safe_str(row["ma_hop_dong"])
        if not ma_hd:
            continue

        document_id = hashlib.sha256(ma_hd.encode("utf-8")).hexdigest()

        doc = {
            "documentId": document_id,
            "data": {
                "timestamp": parse_date(row["Timestamp"], "Timestamp", idx),
                "ma_hop_dong": ma_hd,
                "li_do_chi": safe_str(row["li_do_chi"]),
                "mo_ta": safe_str(row["mo_ta"]),
                "tai_khoan": {
                    "ten": safe_str(row["ten_tai_khoan"]),
                    "ngan_hang": safe_str(row["ngan_hang"]),
                    "so_tai_khoan": safe_str(row["so_tai_khoan"])
                },
                "so_tien": safe_float(row["so_tien"]),
                "han_cuoi_thanh_toan": parse_date(row["han_cuoi_thanh_toan"], "han_cuoi_thanh_toan", idx),
                "nguoi_tao_lenh": safe_str(row["nguoi_tao_lenh"]),
                "minh_chung": safe_str(row["minh_chung"]),
                "noi_dung": safe_str(row["noi_dung"])
            },
            "metadata": {
                "createdAt": datetime.now(),
                "action": "create"
            }
        }
        json_data.append(doc)

    return json_data


# ======= Index MongoDB =======
def create_indexes(db):
    col = db["tamop_chi_phi"]
    col.create_index("documentId", unique=True)
    print("[INFO] Indexes created successfully")


# ======= Detect changes =======
def detect_changes_and_overwrite(col, document_id: str, new_data: Dict, existing_doc: Dict) -> bool:
    """So sánh dữ liệu cũ & mới, nếu khác thì update"""
    existing_data = existing_doc.get("data", {})
    if new_data == existing_data:
        return False

    print(f"[INFO] Change detected for {document_id}")
    col.update_one(
        {"_id": existing_doc["_id"]},
        {"$set": {
            "data": new_data,
            "metadata.updatedAt": datetime.now(),
            "metadata.action": "update"
        }}
    )
    return True
# ======= Update database =======
def update_database():
    client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=5000)
    db = client["LucThuy_Base"]

    try:
        client.admin.command("ping")
        print("[INFO] Connected to MongoDB")
        create_indexes(db)

        json_data = get_json_data()
        print(f"[INFO] Loaded {len(json_data)} documents from Google Sheet")

        col = db["tamop_chi_phi"]
        existing_docs = list(col.find({}, {"documentId": 1, "data": 1}))
        existing_map = {doc["documentId"]: doc for doc in existing_docs}

        success, created, updated = 0, 0, 0

        for doc in json_data:
            doc_id = doc["documentId"]
            new_data = doc["data"]

            if doc_id in existing_map:
                if detect_changes_and_overwrite(col, doc_id, new_data, existing_map[doc_id]):
                    updated += 1
            else:
                # dùng upsert để tránh duplicate key
                col.update_one(
                    {"documentId": doc_id},
                    {"$set": doc},
                    upsert=True
                )
                created += 1
            success += 1

        print(f"[INFO] Done. Success: {success}, Created: {created}, Updated: {updated}")

    except Exception as e:
        print(f"[ERROR] {e}")
    finally:
        client.close()
        print("[🔌] MongoDB connection closed")


# ======= Main scheduler =======
def main():
    schedule.every(5).minutes.do(update_database)
    update_database()  
    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    update_database()
