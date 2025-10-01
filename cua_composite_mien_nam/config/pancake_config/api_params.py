#  Thông tin API Pancake
app_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYW1lIjoiUGhhbiBUcsaw4budbmciLCJleHAiOjE3NjU3Njc0ODgsImFwcGxpY2F0aW9uIjoxLCJ1aWQiOiJmNTg5ZGRjMC05MmY0LTQ5ZTYtOTcyNi0xMjU4MzhiN2FjZmUiLCJzZXNzaW9uX2lkIjoiNHoxRFVoWVhDYlJJaDJpVnRYUktHZkFFR2xlNnhtTWFkL050Yko0NjdlcyIsImlhdCI6MTc1Nzk5MTQ4OCwiZmJfaWQiOiI0MTIyODg4Mzk2MjgzMDEiLCJsb2dpbl9zZXNzaW9uIjpudWxsLCJmYl9uYW1lIjoiUGhhbiBUcsaw4budbmcifQ.O9WFHmmGd_TFgw1iVsC4F51Auju8JBzcmmBF-jikjgI"
SHOP_ID = 1290021434
BASE_URL = f"https://pos.pancake.vn/api/v1/shops/{SHOP_ID}/orders/get_orders"

HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0",
    "x-client-type": "Web"
}
DEFAULT_PAGE_SIZE = 1000