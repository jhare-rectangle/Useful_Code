import os
import pprint
import pyodbc
from dotenv import load_dotenv
from utils import format_sf_timestamp


table_name = "bridge_pay_import_2022_06_03_06_00_45"
mid_list = ['896225665885', '896225662882', '896225552885', '896225551887', '896225550889', '896225453886', '896225436881', '896225413880', '896225412882', '896225388884', '896225331884', '896225330886', '896225329888', '896225327882', '896225325886', '896225324889', '896225323881', '896225322883', '896225321885', '896225320887', '896225319889', '896225317883', '896225316885', '896225315887', '896225314880']
target_page = 44
rows_per_page = 25

if __name__ == "__main__":
    load_dotenv()
    odbc_driver_str = os.getenv("odbc_driver")
    reporting_server = os.getenv("reporting_server")
    reporting_db = os.getenv("reporting_database")
    reporting_db_user = os.getenv("reporting_user")
    reporting_db_pw = os.getenv("reporting_password")
    reporting_db_connect = f"DRIVER={odbc_driver_str};SERVER={reporting_server};DATABASE={reporting_db};" \
                           f"UID={reporting_db_user};PWD={reporting_db_pw}"

    conn = pyodbc.connect(reporting_db_connect)
    cursor = conn.cursor()

    rows_query_str = "SELECT ReportDate as sReportDate, MID as sMerchantNumber, " \
                     "SUM(CAST(MTDTransactionCount as numeric(18,4))) as sMTDMerchantTransaction, " \
                     "SUM(CAST(MTDTransactionDollarVol as numeric(18,4))) as sMTDMerchantVolume, " \
                     "SUM(CAST(YTDTransactionDollarVol as numeric(18,4))) as sYTDMerchantVolume, " \
                     "SUM(CAST(YTDTransactionCount as numeric(18,4))) as sYTDMerchantTransaction " \
                     f"FROM BP_DAILY.dbo.{table_name} group by MID,ReportDate ORDER BY MID DESC " \
                     "OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"
    entry_query_str = "SELECT ReportDate as sReportDate, MerchantAccountName as sMerchant, " \
                      "MID as sMerchantNumber," \
                      "MerchantAccountCode as sMerchantAccountCode, " \
                      "SUM(CAST(MTDTransactionCount as numeric(18,4))) as sMTDMerchantTransaction, " \
                      "SUM(CAST(MTDTransactionDollarVol as numeric(18,4))) as sMTDMerchantVolume, " \
                      "SUM(CAST(YTDTransactionDollarVol as numeric(18,4))) as sYTDMerchantVolume, " \
                      "SUM(CAST(YTDTransactionCount as numeric(18,4))) as sYTDMerchantTransaction " \
                      f"FROM BP_DAILY.dbo.{table_name} " \
                      "WHERE MID=? group by MerchantAccountCode,MID,ReportDate,MerchantAccountName"
    origional_rows = cursor.execute(rows_query_str, ((target_page - 1) * rows_per_page, rows_per_page)).fetchall()
    original_data = dict()
    for row in origional_rows:
        mnum = f"{row.sMerchantNumber}" if row.sMerchantNumber is not None else "0"
        original_data.setdefault(mnum, list()).append({
            "sMerchantNumber": mnum,
            "sMTDMerchantVolume": f"{row.sMTDMerchantVolume}" if row.sMTDMerchantVolume is not None else "0",
            "sYTDMerchantVolume": f"{row.sYTDMerchantVolume}" if row.sYTDMerchantVolume is not None else "0",
            "sMTDMerchantTransaction": f"{row.sMTDMerchantTransaction}"
            if row.sMTDMerchantTransaction is not None else "0",
            "sYTDMerchantTransaction": f"{row.sYTDMerchantTransaction}"
            if row.sYTDMerchantTransaction is not None else "0",
            "sReportDate": f'{format_sf_timestamp(row.sReportDate)}'
            if row.sReportDate else ""
        })
    results = list()
    for mid in mid_list:
        rows = cursor.execute(entry_query_str, mid).fetchall()
        found_rows = list()
        for row in rows:
            found_rows.append({
                "sMerchant": f"{row.sMerchant}" if row.sMerchant is not None else "",
                "sMerchantAccountCode": f"{row.sMerchantAccountCode}" if row.sMerchantAccountCode is not None else "",
                "sMerchantNumber": f"{row.sMerchantNumber}" if row.sMerchantNumber is not None else "0",
                "sMTDMerchantVolume": f"{row.sMTDMerchantVolume}" if row.sMTDMerchantVolume is not None else "0",
                "sYTDMerchantVolume": f"{row.sYTDMerchantVolume}" if row.sYTDMerchantVolume is not None else "0",
                "sMTDMerchantTransaction": f"{row.sMTDMerchantTransaction}"
                if row.sMTDMerchantTransaction is not None else "0",
                "sYTDMerchantTransaction": f"{row.sYTDMerchantTransaction}"
                if row.sYTDMerchantTransaction is not None else "0",
                "sReportDate": f'{format_sf_timestamp(row.sReportDate)}'
                if row.sReportDate else ""
            })
        if found_rows:
            if len(found_rows) == 1:
                results.append(found_rows[0])
            else:
                matched = False
                for option in found_rows:
                    for original_row in original_data[mid]:
                        if original_row["sMTDMerchantVolume"] == option["sMTDMerchantVolume"] and \
                           original_row["sYTDMerchantVolume"] == option["sYTDMerchantVolume"] and \
                           original_row["sMTDMerchantTransaction"] == option["sMTDMerchantTransaction"] and \
                           original_row["sYTDMerchantTransaction"] == option["sYTDMerchantTransaction"]:
                            results.append(option)
                            matched = True
                            break
                    if matched:
                        break

    print(f"{table_name}: {len(results)}")
    pprint.pprint(results, sort_dicts=False)
