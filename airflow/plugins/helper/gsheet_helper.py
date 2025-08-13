import gspread
import pandas as pd
from config import config

class GSheetHelper:
    def __init__(self, sheet_id, client=None, credentials_file=None):

        self.client = client
        if not self.client:
            self.client = gspread.service_account(filename=config.SERVICE_ACCOUNT)
            self.sheet = self.client.open_by_key(sheet_id)

    def read_worksheet_by_range(self, sheet_name, range):
        ws = self.sheet.worksheet(sheet_name)
        data = ws.get(range)
        df = pd.DataFrame(data[1:], columns=data[0])
        # Lọc bỏ các dòng blank hoàn toàn
        df = df[~df.apply(lambda row: row.astype(str).str.strip().eq("").all(), axis=1)]

        # Reset index sau khi lọc
        df = df.reset_index(drop=True)
        return df
    
    def read_ws_range_rect(self, sheet_name, range):
        ws = self.sheet.worksheet(sheet_name)

        # Lấy dữ liệu, giữ nguyên giá trị đã tính của công thức
        values = ws.get(range, value_render_option='UNFORMATTED_VALUE')  # hoặc 'FORMATTED_VALUE'

        if not values:
            return pd.DataFrame()

        header = values[0]
        # Số cột kỳ vọng theo header (vd A→M là 13)
        ncols = len(header)

        # Nếu header có ô trống, đặt tên tạm để tránh trùng/blank
        header = [c if (c is not None and str(c).strip() != "") else f"col_{i+1}" for i, c in enumerate(header)]

        # Chuẩn hóa từng hàng: thiếu thì pad "", thừa thì cắt
        rows = []
        for r in values[1:]:
            r = (r + [""] * ncols)[:ncols]
            rows.append(r)

        df = pd.DataFrame(rows, columns=header)

        # Loại các dòng hoàn toàn trống
        mask_all_blank = df.astype(str).apply(lambda s: s.str.strip()).eq("").all(axis=1)
        df = df[~mask_all_blank].reset_index(drop=True)

        return df