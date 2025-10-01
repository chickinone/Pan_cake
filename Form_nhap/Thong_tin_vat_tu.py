import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import hashlib
import time
import schedule
from typing import Dict, Any, List
import gspread

gc = gspread.service_account(
    filename='D:/LucThuy_knowledge_base-main/LucThuy_knowledge_base-main/QuanLyVanChuyen/config/gg_sheet_config/credentials.json'
)
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
    worksheet = EXCEL_FILE.worksheet("Thông tin vật tư")
    data = worksheet.get_all_records()
    df = pd.DataFrame(data)
    df.columns = [
        "thoi_gian_ghi",
        "ma_hop_dong",
        "stt",
        "ma_vat_tu",
        "don_vi",
        "so_luong",
        "kich_thuoc",
        "ghi_chu",
        "anh",
    ]
    grouped_data: Dict[str, Dict[str, Any]] = {}
    for idx, row in df.iterrows():
        try:
            thoi_gian = parse_date(row["thoi_gian_ghi"], "thoi_gian_ghi", idx)
            ma_hd = str(row["ma_hop_dong"]).strip() if not pd.isna(row["ma_hop_dong"]) else ""
            if not ma_hd:
                continue

            document_id = hashlib.sha256(ma_hd.encode("utf-8")).hexdigest()

            vat_tu = {
                "stt": safe_int(row["stt"]),
                "ma_vat_tu": row["ma_vat_tu"] if not pd.isna(row["ma_vat_tu"]) else None,
                "don_vi": row["don_vi"] if not pd.isna(row["don_vi"]) else None,
                "so_luong": safe_int(row["so_luong"]),
                "kich_thuoc": row["kich_thuoc"] if not pd.isna(row["kich_thuoc"]) else None,
                "ghi_chu": row["ghi_chu"] if not pd.isna(row["ghi_chu"]) else None,
                "anh": row["anh"] if not pd.isna(row["anh"]) else None,
            }

            if document_id not in grouped_data:
                grouped_data[document_id] = {
                    "documentId": document_id,
                    "data": {
                        "thoi_gian_ghi": thoi_gian if not pd.isna(row["thoi_gian_ghi"]) else None,
                        "ma_hop_dong": ma_hd if not pd.isna(row["ma_hop_dong"]) else None,
                        "vattu": [vat_tu],   
                    },
                    "metadata": {
                        "createdAt": datetime.now(),
                        "action": "create",
                    },
                }
            else:
                grouped_data[document_id]["data"]["vattu"].append(vat_tu)

        except Exception as e:
            print(f"[ERROR] Row {idx}: {e}")
            continue

    return list(grouped_data.values())


def create_indexes(db):
    col = db["tamop_thong_tin_klvt"]
    col.create_index("documentId", unique=True)
    print("[INFO] Index created successfully")


def detect_changes_and_overwrite(db, document_id: str, new_data: Dict, existing_doc: Dict) -> bool:
    existing_data = existing_doc.get("data", {})
    if new_data == existing_data:
        return False

    print(f"[INFO] Change detected for {document_id}")
    col = db["tamop_thong_tin_klvt"]

    col.update_one(
        {"_id": existing_doc["_id"]},
        {
            "$set": {
                "data": new_data,
                "metadata.updatedAt": datetime.now(),
                "metadata.action": "update",
            }
        },
    )
    return True


def update_database():
    client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=5000)
    db = client["LucThuy_knowledge_base"]

    try:
        client.admin.command("ping")
        print("[INFO] Connected to MongoDB")
        create_indexes(db)
        json_data = get_json_data()
        print(f"[INFO] Loaded {len(json_data)} hợp đồng từ Excel")

        col = db["tamop_thong_tin_klvt"]
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
