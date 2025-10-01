from decimal import Decimal, InvalidOperation
from datetime import datetime, timedelta
import json

# ======= Các cột trong DB =======
fields = [
    "order_id", "id", "vc", "the", "ghi_chu",
    "khach_hang", "sdt", "nhan_hang", "ghi_chu_dvvc",
    "san_pham", "han_ban_giao_don", "tao_luc",
    "cap_nhat_tt", "tong_tien", "trang_thai"
]

# ======= Hàm extract =======
def extract_text_only(f, key):
    return f.get(key, "") or ""

def extract_number_to_decimal(f, key):
    default = Decimal("0.00")
    v = f.get(key, default)
    try:
        if isinstance(v, str):
            v = v.replace(",", "").replace("₫", "").strip()
        return Decimal(v).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError, TypeError):
        return default

def safe_value(val):
    """Nếu là dict/list thì chuyển sang JSON string"""
    if isinstance(val, (dict, list)):
        return json.dumps(val, ensure_ascii=False)
    return val

def excel_serial_to_datetime(fields, key):
    serial = fields.get(key, [])
    if not serial:
        return None
    tz_offset_hours = 0 # GMT+7
    # Excel date base (Excel for Windows): 1899-12-30
    base_date = datetime(1899, 12, 30)
    delta = timedelta(days=serial)
    dt = base_date + delta
    # Add timezone offset
    dt = dt + timedelta(hours=tz_offset_hours)
    return dt

from datetime import datetime, timedelta

def format_any_datetime(value, tz_offset_hours=7, fmt="%Y/%m/%d %H:%M:%S"):
    try:
        # Nếu là datetime thì format luôn
        if isinstance(value, datetime):
            value += timedelta(hours=tz_offset_hours)
            return value.strftime(fmt)

        # Nếu là số (epoch milliseconds)
        if isinstance(value, (int, float)):
            dt = datetime.utcfromtimestamp(value / 1000)
            dt += timedelta(hours=tz_offset_hours)
            return dt.strftime(fmt)

        # Nếu là list, lấy phần tử đầu tiên
        if isinstance(value, list) and value:
            return format_any_datetime(value[0], tz_offset_hours, fmt)

        # Nếu không phải chuỗi, ép kiểu thành chuỗi
        if not isinstance(value, str):
            value = str(value)

        # Parse chuỗi ISO
        dt = datetime.fromisoformat(value)
        dt += timedelta(hours=tz_offset_hours)
        return dt.strftime(fmt)

    except Exception as e:
        print(f"[ERROR] Không xử lý được thời gian: {value} ({type(value)}). Lỗi: {e}")
        return None