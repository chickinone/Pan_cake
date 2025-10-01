import os
import time
import json
import base64
import requests
from dotenv import load_dotenv
from config.pancake_config.api_params import * 
load_dotenv()

# ===================== CONFIG =====================
PANCAKE_USER = os.getenv("PANCAKE_USER")
PANCAKE_PASS = os.getenv("PANCAKE_PASS")
SHOP_ID = "1290021436"   # Thay bằng shop ID 
LOGIN_URL = "https://pos.pancake.vn/api/v1/auth/login"  # Endpoint login (giả định)
BASE_URL  = f"https://pos.pancake.vn/api/v1/shops/{SHOP_ID}/orders/get_orders"

app_token = None  # token sẽ được lưu toàn cục


def get_token_expiration(token: str) -> int:
    """Trả về unix timestamp hết hạn của token JWT."""
    try:
        payload = token.split('.')[1]
        padding = '=' * (-len(payload) % 4)
        decoded = base64.urlsafe_b64decode(payload + padding)
        exp = json.loads(decoded).get('exp', 0)
        return exp
    except Exception:
        return 0


def is_token_expired(token: str, buffer_seconds: int = 60) -> bool:
    """Kiểm tra token đã hoặc sắp hết hạn (trước buffer giây)."""
    exp = get_token_expiration(token)
    return time.time() > (exp - buffer_seconds)


def refresh_token() -> str:
    """
    Gọi API login để lấy token mới.
    Nếu Pancake cung cấp API refresh riêng, thay đổi endpoint/param tương ứng.
    """
    payload = {"email": PANCAKE_USER, "password": PANCAKE_PASS}
    print("Đang refresh token...")
    r = requests.post(LOGIN_URL, json=payload)
    r.raise_for_status()
    data = r.json()
    token = data.get("access_token")
    if not token:
        raise RuntimeError("Không tìm thấy access_token trong response.")
    print("Refresh token thành công.")
    return token


def get_valid_token() -> str:
    """Lấy token hợp lệ (refresh nếu hết hạn hoặc chưa có)."""
    global app_token
    if (not app_token) or is_token_expired(app_token):
        app_token = refresh_token()
    return app_token


def crawl_all_orders(max_pages: int = None) -> list:
    """
    Crawl tất cả đơn hàng từng trang **đến khi API trả về rỗng**.
    - max_pages: giới hạn trang tối đa (None = crawl hết).
    - Không cố định page_size → server tự chọn mặc định.
    """
    page = 1
    all_orders = []

    while True:
        token = get_valid_token()
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0",
            "x-client-type": "Web"
        }
        params = {
            "page": page,
            "status": -1,
            "option_sort": "inserted_at_desc",
            "es_only": True
        }

        resp = requests.post(
            f"{BASE_URL}?access_token={token}",
            json=params,
            headers=headers
        )

        if resp.status_code != 200:
            print(f"Lỗi khi tải trang {page}: {resp.status_code} {resp.text}")
            break

        data = resp.json()
        orders = data.get("data", [])
        if not orders:
            print("Đã đến trang cuối, dừng crawl.")
            break

        all_orders.extend(orders)
        print(f"Trang {page}: Lấy {len(orders)} đơn hàng (Tổng: {len(all_orders)})")

        page += 1
        if max_pages and page > max_pages:
            print(f"Đã đạt giới hạn {max_pages} trang.")
            break

    return all_orders


if __name__ == "__main__":
    orders = crawl_all_orders()
    print(f"\n Hoàn tất crawl. Tổng số đơn hàng: {len(orders)}")
