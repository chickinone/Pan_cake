#Thong tin data
dbname=don_hang_cua_composite_cao_cap,
user=postgres,
password=truong123,
host=localhost,
port=5432

fields = [
    "order_id",
    "id",
    "vc",
    "the",
    "ghi_chu",
    "khach_hang",
    "sdt",
    "nhan_hang",
    "ghi_chu_dvvc",
    "san_pham",
    "han_ban_giao_don",
    "tao_luc",
    "cap_nhat_tt",
    "tong_tien",
    "trang_thai"
]

insert_on_conflict_query = \
"""INSERT INTO don_hang_cua_composite_cao_cap (
    order_id,
    id,
    vc,
    the,
    ghi_chu,
    khach_hang,
    sdt,
    nhan_hang,
    ghi_chu_dvvc,
    san_pham,
    han_ban_giao_don,
    tao_luc,
    cap_nhat_tt,
    tong_tien,
    trang_thai
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s
)
ON CONFLICT (order_id) DO UPDATE SET
    id = EXCLUDED.id,
    vc = EXCLUDED.vc,
    the = EXCLUDED.the,
    ghi_chu = EXCLUDED.ghi_chu,
    khach_hang = EXCLUDED.khach_hang,
    sdt = EXCLUDED.sdt,
    nhan_hang = EXCLUDED.nhan_hang,
    ghi_chu_dvvc = EXCLUDED.ghi_chu_dvvc,
    san_pham = EXCLUDED.san_pham,
    han_ban_giao_don = EXCLUDED.han_ban_giao_don,
    tao_luc = EXCLUDED.tao_luc,
    cap_nhat_tt = EXCLUDED.cap_nhat_tt,
    tong_tien = EXCLUDED.tong_tien,
    trang_thai = EXCLUDED.trang_thai"""
