import gspread
import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import hashlib
import time

# ======= Google Sheets Config =======
gc = gspread.service_account(
    filename="D:/PhanVanTruong_Work/Database_Know/main/config/gg_sheet_config/credentials.json"
)
EXCEL_FILE = gc.open_by_key("1jdZ-rvtch7GQzxCPqM_n4qYF8t3_tF_BWp6FmSAvgeE")
# ======= Helpers =======
def safe_str(val):
    return str(val).strip() if val is not None and str(val).strip() != "" else None


def parse_date(val, field, idx):
    if val is None or str(val).strip() == "":
        return None
    try:
        dt = pd.to_datetime(val, errors="coerce", dayfirst=True)
        if pd.isna(dt):
            return None
        return dt.to_pydatetime()
    except Exception:
        print(f"[WARNING] Row {idx}: Invalid date in {field} -> {val}")
        return None


# ======= Load & Transform =======
def get_json_data():
    worksheet = EXCEL_FILE.worksheet("Đơn thợ chi tiết")
    all_values = worksheet.get_all_values()

    # Header ngay hàng 1
    headers = all_values[0]
    data_rows = all_values[1:]  # dữ liệu từ hàng 2 trở đi

    # Xóa cột rỗng hoặc không cần thiết nếu có
    clean_headers = [h for h in headers if h.strip() != ""]
    clean_data_rows = []
    for row in data_rows:
        new_row = [row[i] for i, h in enumerate(headers) if h.strip() != ""]
        clean_data_rows.append(new_row)

    df = pd.DataFrame(clean_data_rows, columns=clean_headers)

    json_data = []
    for idx, row in df.iterrows():
        record_id = safe_str(row.get("Mã điều thợ"))
        if not record_id:
            continue

        document_id = hashlib.sha256(record_id.encode("utf-8")).hexdigest()

        doc = {
            "documentId": document_id,
            "data": {
                "ma_dieu_tho": record_id,
                "ma_hop_dong": safe_str(row.get("Mã hợp đồng")),
                "ngay_yeu_cau": parse_date(row.get("Ngày yêu cầu thi công"), "Ngày yêu cầu thi công", idx),
                "loai_thi_cong": safe_str(row.get("Loại thi công")),
                "ten_tho": safe_str(row.get("Tên thợ - SĐT")),
                "tong_chi": safe_str(row.get("Tổng chi")),
                "tin_nhan_tho_chi_tiet": safe_str(row.get("Tin nhắn thợ chi tiết")),
                "dang_ky_duyet_chi": safe_str(row.get("Đăng ký duyệt chi")),
                "xac_nhan_dkdc": safe_str(row.get("Xác nhận đkdc")),
                "da_chi": safe_str(row.get("Đã chi")),
                "so_do": safe_str(row.get("Sơ đồ")),
                "video": safe_str(row.get("Video")),
                "tat_toan": safe_str(row.get("Tất toán")),
            },
            "metadata": {
                "createdAt": datetime.now(),
                "action": "create"
            }
        }
        json_data.append(doc)

    return json_data


# ======= DB Index =======
def create_indexes(db):
    col = db["don_tho_thi_cong"]
    col.create_index("documentId", unique=True)
    print("[INFO] Indexes created successfully")


# ======= Change Detection =======
def detect_changes_and_overwrite(db, document_id: str, new_data: dict, existing_doc: dict) -> bool:
    existing_data = existing_doc.get("data", {})
    if new_data == existing_data:
        return False

    print(f"[INFO] Change detected for {document_id}")
    col = db["don_tho_thi_cong"]
    col.update_one(
        {"_id": existing_doc["_id"]},
        {"$set": {
            "data": new_data,
            "metadata.updatedAt": datetime.now(),
            "metadata.action": "update"
        }}
    )
    return True


# ======= Update DB =======
def update_database():
    client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=5000)
    db = client["quan_ly_tho"]

    try:
        client.admin.command("ping")
        print("[INFO] Connected to MongoDB")
        create_indexes(db)

        json_data = get_json_data()
        print(f"[INFO] Loaded {len(json_data)} documents from Google Sheet")

        col = db["don_tho_thi_cong"]

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
            success += 1

        print(f"[INFO] Done. Success: {success}, Created: {created}, Updated: {updated}")

    except Exception as e:
        print(f"[ERROR] {e}")
    finally:
        client.close()
        print("[🔌] MongoDB connection closed")


# ======= Main Scheduler =======
def main():
    import schedule
    import time
    schedule.every(5).minutes.do(update_database)
    update_database()
    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    main()
