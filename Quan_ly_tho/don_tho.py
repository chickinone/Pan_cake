import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import hashlib
import time
import schedule
from typing import Dict, Any
import gspread

def parse_date(val, field, idx):
    for fmt in ("%d/%m/%Y %H:%M:%S", "%m/%d/%Y %H:%M:%S"):
        try:
            return datetime.strptime(val, fmt)
        except Exception:
            continue
    return pd.to_datetime(val, errors="coerce")  # fallback


# ======= Google Sheets Config =======
gc = gspread.service_account(
    filename="D:/PhanVanTruong_Work/Database_Know/main/config/gg_sheet_config/credentials.json"
)
EXCEL_FILE = gc.open_by_key("1jdZ-rvtch7GQzxCPqM_n4qYF8t3_tF_BWp6FmSAvgeE")


# ======= Helpers =======
def parse_date(val, field, idx):
    if pd.isna(val) or str(val).strip() == "":
        return None
    try:
        dt = pd.to_datetime(val, errors="coerce", dayfirst=True)
        if pd.isna(dt):
            return None
        return dt.to_pydatetime()
    except Exception:
        print(f"[WARNING] Row {idx}: Invalid date in {field} -> {val}")
        return None


def safe_str(val):
    return str(val).strip() if not pd.isna(val) and str(val).strip() != "" else None


def safe_list(val):
    if pd.isna(val) or str(val).strip() == "":
        return []
    items = str(val).replace(",_", "\n").splitlines()
    return [i.strip(" _") for i in items if i.strip()]


# ======= Load & Transform =======
def get_json_data():
    worksheet = EXCEL_FILE.worksheet("Đơn thợ")

    # Lấy toàn bộ dữ liệu từ sheet
    all_values = worksheet.get_all_values()

    # Header thật sự nằm ở hàng 4 (index 3)
    headers = all_values[3]

    # Bỏ cột "Column 11" nếu có
    clean_headers = [h.strip() for h in headers if h and h.strip() != "Column 11"]

    # Lấy data từ hàng 5 trở đi
    data_rows = all_values[4:]

    # Đồng bộ loại bỏ cột "Column 11" ở data
    data_rows = [
        [cell for j, cell in enumerate(row) if headers[j].strip() != "Column 11"]
        for row in data_rows
    ]

    # Tạo DataFrame
    df = pd.DataFrame(data_rows, columns=clean_headers)

    json_data = []
    for idx, row in df.iterrows():
        record_id = safe_str(row["record_id"])
        if not record_id:
            continue

        document_id = hashlib.sha256(record_id.encode("utf-8")).hexdigest()

        doc = {
            "documentId": document_id,
            "data": {
                "meta": {
                    "timestamp": parse_date(row["Timestamp"], "Timestamp", idx),
                    "ma_don_hang": safe_str(row["Mã đơn hàng"]),
                    "khach_hang": safe_str(row["Khách hàng"]),
                    "pic": safe_str(row["PIC"]),
                },
                "dia_chi": {
                    "khu_vuc": safe_str(row["Khu vực"]),
                    "tinh": safe_str(row["Tỉnh/Thành phố"]),
                    "quan_huyen": safe_str(row["Quận/Huyện"]),
                    "chi_tiet": safe_str(row["Địa chỉ công trình"]),
                },
                "thi_cong": {
                    "thoi_gian_yeu_cau": parse_date(row["Thời gian yêu cầu thi công"], "Thời gian yêu cầu thi công", idx),
                    "phuong_an": safe_str(row["Phương án thi công"]),
                    "ghi_chu_tim_tho": safe_str(row["Ghi chú tìm thợ"]),
                    "pic_tim_tho": safe_str(row["PIC Tìm thợ"]),
                    "tin_nhan_tho": safe_str(row["Tin nhắn thợ"]),
                    "nhom": safe_str(row["Nhóm"]),
                    "so_do_mat_bang": safe_str(row["Sơ đồ mặt bằng thi công"])
                },
                "giao_hang": {
                    "trang_thai_van_chuyen": safe_str(row["Tiến độ đơn hàng"]),
                    "trang_thai_thanh_toan": safe_str(row["Tất toán"]),
                    "thoi_gian_giao_du_kien": parse_date(row["Thời gian giao hàng dự kiến"], "Thời gian giao hàng dự kiến", idx),
                    "ghi_chu_giao_hang": safe_str(row["Ghi chú giao hàng"]),
                    "so_tien_thu_ho": safe_str(row["Số tiền thu hộ thợ"])
                },
                "SanPham": safe_list(row["Tin nhắn danh sách hàng"]),
                "Media": safe_list(row["Hình ảnh video mặt bằng"])
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
    col = db["tamop_don_tho"]
    col.create_index("documentId", unique=True)
    print("[INFO] Indexes created successfully")


# ======= Change Detection =======
def detect_changes_and_overwrite(db, document_id: str, new_data: Dict, existing_doc: Dict) -> bool:
    existing_data = existing_doc.get("data", {})
    if new_data == existing_data:
        return False

    print(f"[INFO] Change detected for {document_id}")
    col = db["tamop_don_tho"]

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
    db = client["LucThuy_Base"]

    try:
        client.admin.command("ping")
        print("[INFO] Connected to MongoDB")
        create_indexes(db)

        json_data = get_json_data()
        print(f"[INFO] Loaded {len(json_data)} documents from Google Sheet")

        col = db["tamop_don_tho"]
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


# ======= Main Scheduler =======
def main():
    schedule.every(5).minutes.do(update_database)
    update_database()
    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    main()
