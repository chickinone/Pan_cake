import json
import time
import schedule
import psycopg2
import requests
from decimal import Decimal
import json
from config.pancake_config.api_params import app_token, BASE_URL, HEADERS, DEFAULT_PAGE_SIZE
from crawl_table_don_hang.processing_order import (
    fields,
    extract_text_only,
    extract_number_to_decimal,
    format_any_datetime
)

try:
    from config.pancake_config.api_params import COOKIES
except Exception:
    COOKIES = None


# SAFE VALUE: chuyển dict/list -> JSON string khi cần
def safe_value(v):
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return v


# ========== DATABASE ==========
def create_table(conn):
    sql = """
    CREATE TABLE IF NOT EXISTS don_hang_cua_composite_mien_nam(
        order_id TEXT PRIMARY KEY,
        id TEXT,
        vc TEXT,
        the TEXT,
        ghi_chu TEXT,
        khach_hang TEXT,
        sdt TEXT,
        nhan_hang TEXT,
        ghi_chu_dvvc TEXT,
        san_pham TEXT,
        han_ban_giao_don TEXT,
        tao_luc TEXT,
        cap_nhat_tt TEXT,
        tong_tien NUMERIC,
        trang_thai TEXT,
        is_deleted BOOLEAN DEFAULT FALSE
    );
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()
    print("[DB] Bảng don_hang_cua_composite_mien_nam đã sẵn sàng.")


def insert_on_conflict(conn, records):
    if not records:
        print("[DB] Không có record để insert.")
        return
    placeholders = ", ".join(["%s"] * len(fields))
    updates = ", ".join([f"{c}=EXCLUDED.{c}" for c in fields if c != "order_id"])
    query = f"""
        INSERT INTO don_hang_cua_composite_mien_nam ({", ".join(fields)})
        VALUES ({placeholders})
        ON CONFLICT (order_id) DO UPDATE SET {updates};
    """
    data = [tuple(safe_value(r.get(f)) for f in fields) for r in records]
    with conn.cursor() as cur:
        cur.executemany(query, data)
        conn.commit()
    print(f"[DB] Insert/Update {len(records)} record(s) thành công.")


def update_is_deleted(conn, valid_ids):
    if not valid_ids:
        print("[DB] Không có id hợp lệ để cập nhật is_deleted.")
        return
    valid_ids = [str(i) for i in valid_ids]  # ép về string
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE don_hang_cua_composite_mien_nam SET is_deleted = TRUE WHERE NOT (order_id = ANY(%s))",
            (valid_ids,)
        )
        conn.commit()
    print("[DB] Cập nhật is_deleted thành công.")



# ========== CRAWL ==========
def crawl_batches(page_size=None, max_pages=None):
    page_size = page_size or DEFAULT_PAGE_SIZE or 100  # nếu config để None thì dùng 100
    all_orders = []
    page = 1
    while True:
        params = {
            "access_token": app_token,
            "page_size": page_size,
            "status": -1,
            "page": page,
            "updateStatus": "inserted_at",
            "editorId": "none",
            "option_sort": "inserted_at_desc",
            "es_only": "true"
        }

        try:
            # Sử dụng POST với body {} để match request của browser
            resp = requests.post(BASE_URL, params=params, json={}, headers=HEADERS, cookies=COOKIES, timeout=30)
        except Exception as e:
            print(f"[ERROR] Trang {page}: request lỗi {e}")
            break

        # In debug chi tiết nếu không OK
        if resp.status_code != 200:
            print(f"[ERROR] Trang {page}: HTTP {resp.status_code}")
            body_preview = resp.text[:1000] if resp.text else "<no body>"
            print("Response preview:", body_preview)
            break

        # Thông thường server trả JSON với key "data"
        try:
            data = resp.json()
        except Exception as e:
            print(f"[ERROR] Trang {page}: không parse được JSON: {e}")
            print("Raw response:", resp.text[:1000])
            break

        orders = data.get("data", []) if isinstance(data, dict) else []
        # bảo đảm orders là list
        if not orders:
            print("==> Đã đến trang cuối (API trả về rỗng).")
            break

        all_orders.extend(orders)
        print(f"[OK] Trang {page}: Lấy {len(orders)} đơn hàng (page_size={page_size}). Total={len(all_orders)}")

        # Dừng nếu trang trả về ít hơn page_size (trang cuối)
        if len(orders) < page_size:
            print("==> Trang cuối (số item < page_size). Dừng crawl.")
            break

        page += 1
        if max_pages and page > max_pages:
            print(f"Đạt giới hạn max_pages={max_pages}. Dừng.")
            break

    return all_orders


# ========== PROCESS ==========
def process_orders(orders):
    processed = []
    ids = []
    for o in orders:
        processed.append({
            # ? the, nhan_hang, ghi_chu_dvvc
            "order_id": o.get("id"),  
            "id": extract_text_only(o, "display_id"),
            "vc": extract_text_only(o, "shipments"),
            "the": "",  
            "ghi_chu": extract_text_only(o, "note"),
            "khach_hang": extract_text_only(o, "bill_full_name"),
            "sdt": extract_text_only(o, "bill_phone_number"),
            "nhan_hang": extract_text_only(o, "ship_full_address"),
            "ghi_chu_dvvc": extract_text_only(o, "note_print"),
            "san_pham": (
            "Chưa có sản phẩm"
            if not extract_text_only(o, "items")
            else json.dumps(extract_text_only(o, "items"), ensure_ascii=False)
        ),
            "han_ban_giao_don": extract_text_only(o, "additional_info.delivery_deadline"),
            "tao_luc": format_any_datetime(o.get("inserted_at")),
            "cap_nhat_tt": format_any_datetime(o.get("updated_at")),
            "tong_tien": extract_number_to_decimal(o, "total_price"),
            "trang_thai": str(extract_text_only(o, "status")),

        })
        ids.append(o.get("id"))
    return ids, processed

def save_sample_orders(orders, filename="out_put.json"):
    if not orders:
        print("[DEBUG] Không có order nào để ghi file.")
        return
    try:
        # Ghi 3 đơn đầu tiên check cac thuộc tinh
        sample = orders[:3]
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(sample, f, indent=2, ensure_ascii=False)
        print(f"[DEBUG] Đã ghi {len(sample)} order mẫu vào {filename}")
    except Exception as e:
        print(f"[ERROR] Lỗi khi ghi file {filename}: {e}")

# ========== MAIN ==========
def main():
    start = time.time()
    conn = None
    try:
        conn = psycopg2.connect(
            dbname="don_hang_cua_composite_mien_nam",
            user="postgres",
            password="truong123",
            host="localhost",
            port=5432
        )
        print("[DB] Kết nối thành công.")
        create_table(conn)

        # Có thể truyền page_size=None vào crawl_batches (mình sẽ dùng fallback 100).
        orders = crawl_batches(page_size=None)
        save_sample_orders(orders)
        ids, records = process_orders(orders)
        insert_on_conflict(conn, records)
        update_is_deleted(conn, ids)
        print(f"[DONE] Crawl & Update xong {len(ids)} đơn hàng. Thời gian: {time.time() - start:.2f}s")
    except Exception as e:
        print(f"[MAIN ERROR] {e}")
    finally:
        if conn:
            conn.close()


def run_scheduler():
    schedule.every(5).minutes.do(main)
    print("[Scheduler] Chạy crawl mỗi 5 phút. Nhấn Ctrl+C để dừng.")
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[Scheduler] Dừng.")


if __name__ == "__main__":
    main()
    run_scheduler()
