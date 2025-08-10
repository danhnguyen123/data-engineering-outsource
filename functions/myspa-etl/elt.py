import pandas as pd
from io import BytesIO
from datetime import datetime
from helper.etl_helper import upsert_bigquery

def process_customer(file_name, bucket, bq):

    blob = bucket.blob(file_name)
    data = blob.download_as_bytes()
    df = pd.read_excel(BytesIO(data))

    rename_column = {
    'Mã khách hàng': 'ma_khach_hang',
    'Họ tên': 'ho_ten',
    'Số điện thoại': 'so_dien_thoai',
    'Email': 'email',
    'Ngày sinh': 'ngay_sinh',
    'Giới tính': 'gioi_tinh',
    'Địa chỉ': 'dia_chi',
    'Phường/Xã': 'phuong_xa',
    'Quận/Huyện': 'quan_huyen',
    'Tỉnh thành': 'tinh_thanh',
    'Nhóm KH': 'nhom_kh',
    'Nguồn KH': 'nguon_kh',
    'Ngày tham gia': 'ngay_tham_gia',
    'Dịch vụ đã sử dụng': 'dich_vu_da_su_dung',
    'Ngày sử dụng dịch vụ': 'ngay_su_dung_dich_vu',
    'Tổng tiền': 'tong_tien',
    'Nghề nghiệp': 'nghe_nghiep',
    'Mã giới thiệu': 'ma_gioi_thieu',
    'NV liên hệ': 'nv_lien_he',
    'NV phụ trách': 'nv_phu_trach',
    'Dịch vụ quan tâm': 'dich_vu_quan_tam',
    'Được tạo bởi': 'duoc_tao_boi',
    'Chi nhánh': 'chi_nhanh',
    }

    df = df[[col for col in rename_column]]

    df = df.rename(columns=rename_column)

    df = df[df["ma_khach_hang"].notna() & (df["ma_khach_hang"].astype(str).str.strip() != "")]

    upsert_bigquery(table_name='customer', 
                    identifier_cols=['ma_khach_hang'],
                    dataframe=df,
                    bigquery=bq
                    )

def process_order(file_name, bucket, bq):

    blob = bucket.blob(file_name)
    data = blob.download_as_bytes()
    df = pd.read_excel(BytesIO(data))

    rename_column = {
    'Mã đơn hàng': 'ma_don_hang',
    'Ngày giờ': 'ngay_gio',
    'Mã khách hàng': 'ma_khach_hang',
    'Khách hàng': 'khach_hang',
    'Email': 'email',
    'Điện thoại': 'dien_thoai',
    'Địa chỉ': 'dia_chi',
    'Nguồn KH': 'nguon_kh',
    'Mã DV/SP 2': 'ma_dv_sp_2',
    'Mã DV/SP': 'ma_dv_sp',
    'Tên sản phẩm': 'ten_san_pham',
    'Nhóm DV/SP': 'nhom_dv_sp',
    'SL': 'sl',
    'Số buổi LT': 'so_buoi_lt',
    'Giá DV/SP': 'gia_dv_sp',
    'Chiết khấu DV/SP': 'chiet_khau_dv_sp',
    'Thành tiền DV/SP': 'thanh_tien_dv_sp',
    'Giá ĐH/TLT': 'gia_dh_tlt',
    'Chiết khấu ĐH/TLT': 'chiet_khau_dh_tlt',
    'VAT': 'vat',
    'Thành tiền ĐH/TLT': 'thanh_tien_dh_tlt',
    }

    df = df[[col for col in rename_column]]

    df = df.rename(columns=rename_column)

    df = df[df["ma_don_hang"].notna() & (df["ma_don_hang"].astype(str).str.strip() != "")]

    df["ngay_gio"] = pd.to_datetime(df["ngay_gio"], format="%d/%m/%Y %H:%M:%S")

    #### Load

    upsert_bigquery(table_name='order', 
                    identifier_cols=['ma_don_hang'],
                    dataframe=df,
                    bigquery=bq
                    )

def process_level(file_name, bucket, bq):

    blob = bucket.blob(file_name)
    data = blob.download_as_bytes()
    df = pd.read_excel(BytesIO(data))

    rename_column = {
    'Họ tên': 'ho_ten',
    'Số điện thoại': 'so_dien_thoai',
    'Email': 'email',
    'Mã khách hàng': 'ma_khach_hang',
    'Hạng': 'hang',
    'Số tiền chi tiêu': 'so_tien_chi_tieu',
    'Chi nhánh': 'chi_nhanh',
    }

    df = df[[col for col in rename_column]]

    df = df.rename(columns=rename_column)

    df = df[df["ma_khach_hang"].notna() & (df["ma_khach_hang"].astype(str).str.strip() != "")]

    upsert_bigquery(table_name='level', 
                    identifier_cols=['ma_khach_hang'],
                    dataframe=df,
                    bigquery=bq
                    )

