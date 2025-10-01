import gspread
import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import hashlib
import logging
import schedule
import time
import re

# ======= Logging =======
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# ======= Google Sheets Config =======
gc = gspread.service_account(
    filename="D:/PhanVanTruong_Work/Database_Know/main/config/gg_sheet_config/credentials.json"
)
EXCEL_FILE = gc.open_by_key("1jdZ-rvtch7GQzxCPqM_n4qYF8t3_tF_BWp6FmSAvgeE")
WORKSHEET_NAME = "Chi phí thi công chi tiết"

# ======= Helpers =======
def safe_str(val):
    return str(val).strip() if val is not None and str(val).strip() != "" else None

def safe_number(val):
    if val is None or str(val).strip() == "":
        return 0.0
    cleaned = re.sub(r"[^\d.]", "", str(val))
    try:
        return float(cleaned)
    except Exception:
        return 0.0

def parse_date(val, field="", idx=None):
    if val is None or str(val).strip() == "":
        return None
    try:
        return pd.to_datetime(val, errors="coerce", dayfirst=True)
    except Exception:
        logging.warning(f"[WARNING] Row {idx}: Invalid date in {field} -> {val}")
        return None

def make_document_id(record):
    base_str = "|".join([
        safe_str(record.get("Mã điều thợ")),
        safe_str(record.get("Mã hợp đồng")),
        safe_str(record.get("Loại thi công"))
    ])
    return hashlib.sha256(base_str.encode("utf-8")).hexdigest() if base_str else None

# ======= Load & Transform =======
def get_json_data():
    worksheet = EXCEL_FILE.worksheet(WORKSHEET_NAME)
    all_values = worksheet.get_all_values()

    if len(all_values) < 2:
        logging.info("Sheet không có dữ liệu.")
        return []

    # Header nằm ở hàng đầu tiên
    headers = [h.strip() for h in all_values[0]]
    data_rows = all_values[1:]
    df = pd.DataFrame(data_rows, columns=headers)

    json_data = []
    for idx, row in df.iterrows():
        document_id = make_document_id(row)
        if not document_id:
            continue

        doc = {
            "documentId": document_id,
            "data": {
                "ma_dieu_tho": safe_str(row.get("Mã điều thợ")),
                "ma_hop_dong": safe_str(row.get("Mã hợp đồng")),
                "loai_thi_cong": safe_str(row.get("Loại thi công")),
                "san_pham": safe_str(row.get("Sản phẩm")),
                "hang_muc": safe_str(row.get("Hạng mục")),
                "don_vi": safe_str(row.get("Đơn vị")),
                "so_luong": safe_number(row.get("Số lượng")),
                "thanh_tien": safe_number(row.get("Thành tiền")),
                "ghi_chu": safe_str(row.get("Ghi chú"))
            },
            "metadata": {
                "createdAt": datetime.now(),
                "action": "create"
            }
        }
        json_data.append(doc)
    return json_data

# ======= MongoDB =======
def create_indexes(db):
    col = db["chi_phi_thi_cong"]
    col.create_index("documentId", unique=True)
    logging.info("[INFO] Indexes created successfully")

def detect_changes_and_overwrite(db, document_id, new_data, existing_doc):
    existing_data = existing_doc.get("data", {})
    if new_data == existing_data:
        return False
    logging.info(f"[INFO] Change detected for {document_id}")
    col = db["chi_phi_thi_cong"]
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
    db = client["quan_ly_tho"]

    try:
        client.admin.command("ping")
        logging.info("[INFO] Connected to MongoDB")
        create_indexes(db)

        json_data = get_json_data()
        logging.info(f"[INFO] Loaded {len(json_data)} documents from Google Sheet")

        col = db["chi_phi_thi_cong"]
        existing_docs = list(col.find({}, {"documentId": 1, "data": 1}))
        existing_map = {doc["documentId"]: doc for doc in existing_docs}

        success, created, updated = 0, 0, 0
        for doc in json_data:
            doc_id = doc["documentId"]
            new_data = doc["data"]
            existing_doc = col.find_one({"documentId": doc_id})
            if existing_doc:
                if detect_changes_and_overwrite(db, doc_id, new_data, existing_doc):
                    updated += 1
            else:
                col.insert_one(doc)
                created += 1

        logging.info(f"[INFO] Done. Success: {success}, Created: {created}, Updated: {updated}")

    except Exception as e:
        logging.error(f"[ERROR] {e}")
    finally:
        client.close()
        logging.info("[🔌] MongoDB connection closed")

# ======= Scheduler =======
def main():
    schedule.every(5).minutes.do(update_database)
    update_database()
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()
