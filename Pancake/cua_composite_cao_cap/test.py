import requests

BASE_URL = "https://pos.pancake.vn/api/v1/shops/<SHOP_ID>/orders/get_orders"
params = {
    "access_token": "<TOKEN_MOI>",
    "page": 1,
    "page_size": 30,            # để số cụ thể
    "status": -1,
    "updateStatus": "inserted_at",
    "editorId": "none",
    "option_sort": "inserted_at_desc",
    "es_only": "true"
}
headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0",
    "x-client-type": "Web"
}
r = requests.get(BASE_URL, params=params, headers=headers, timeout=30)
print(r.status_code)
print(r.text[:500])
