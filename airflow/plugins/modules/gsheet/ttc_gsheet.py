
import pandas as pd
from helper.gsheet_helper import GSheetHelper
from helper.gcp_helper import BQHelper
from plugins.helper.logging_helper import LoggingHelper
from config import config

logger = LoggingHelper.get_configured_logger(__name__)

def etl_ttc_external_facebook():
    bq = BQHelper(logger=logger)
    gsheet = GSheetHelper(sheet_id=config.TTC_OUT_FACEBOOK_ID)
    df = gsheet.read_worksheet_by_range(sheet_name="2025", range="A3:M")

    rename_column = {
    'STT (*)': 'stt',
    'Ngày input (*)\n(dd/mm/yyyy)': 'ngay_input',
    'CSKH (*)': 'cskh',
    'Họ và tên KH (*)': 'ho_va_ten_kh',
    'SĐT (*)': 'sdt',
    'Năm sinh (nếu có)': 'nam_sinh',
    'Nguồn lead từ (*)': 'nguon_lead',
    'Nhóm dịch vụ quan tâm (*)': 'nhom_dich_vu_quan_tam',
    'Tên dịch vụ quan tâm (*)': 'ten_dich_vu_quan_tam',
    'Lead type (*)': 'lead_type',
    'Khách hàng status (*)': 'khach_hang_status',
    'Tình trạng tư vấn (*)': 'tinh_trang_tu_van',
    'Note': 'note',
    }

    df = df[[col for col in rename_column]]

    df = df.rename(columns=rename_column)

    df = df[df["sdt"].notna() & (df["sdt"].astype(str).str.strip() != "")]

    df["ngay_input"] = pd.to_datetime(df["ngay_input"], format="%d/%m/%Y").dt.round("s")

    bq.upsert_bigquery(dataframe=df,
                    identifier_cols=['sdt'],
                    table_name='ttc_external_facebook', 
                    dataset_id='gsheet', 
                    )

def etl_ttc_survey():
    bq = BQHelper(logger=logger)
    gsheet = GSheetHelper(sheet_id=config.TTC_SURVEY_ID)
    df = gsheet.read_ws_range_rect(sheet_name="Tổng hợp", range="A2:M")

    rename_column = {
        'No': 'no',
        'Dấu thời gian': 'dau_thoi_gian',
        'Tên khách hàng': 'ten_khach_hang',
        'SĐT Quý khách': 'sdt',
        'Cơ sở vật chất & không gian tại TTCLINIC\n(Mức độ hài lòng về máy móc, trang thiết bị, không gian trị liệu, mùi hương & sự sạch sẽ, chỉn chu của TTCLINIC khi đón tiếp quý khách hàng)': 'co_so_vat_chat',
        'Chất lượng tư vấn, chăm sóc, điều trị trước trong và sau điều trị của khách hàng tại TTCLINIC\n(Mức độ hài lòng của khách hàng với đội ngũ nhân viên của phòng khám)': 'chat_luong_tu_van',
        'Chất lượng dịch vụ tại TTCLINIC\n(Mức độ hài lòng của khách hàng trong quá trình thực hiện dịch vụ tại phòng khám)': 'chat_luong_dich_vu',
        'Mình có sẵn sàng giới thiệu TTCLINIC cho bạn bè người thân không?': 'san_sang_gioi_thieu',
        'Quý khách biết biết tới TTCLINIC qua phương tiện nào?': 'hieu_biet_qua',
        'Ý kiến đóng góp của bạn dành cho TTCLINIC': 'y_kien',
        'Nhân sự': 'nhan_su',
        'PiC': 'pic',
        'Note': 'note'
    }

    df = df[[col for col in rename_column]]

    df = df.rename(columns=rename_column)

    df = df[df["sdt"].notna() & (df["sdt"].astype(str).str.strip() != "")]
    df["sdt"] = df["sdt"].astype(str)
    df["dau_thoi_gian"] = pd.to_datetime(df["dau_thoi_gian"], unit="d", origin="1899-12-30", errors="coerce").dt.round("s")

    bq.upsert_bigquery(dataframe=df,
                    identifier_cols=['sdt'],
                    table_name='ttc_survey', 
                    dataset_id='gsheet', 
                    )