import math
import os
import pprint
import pymssql
from dotenv import load_dotenv
from utils import format_sf_timestamp, time_string
from salesforce_wrapper.salesforce_client import SalesforceClient

done = []
tables = []

rows_per_page = 25

# XXX Error received when SF endpoint is overwhelmed
"""
Update failed. First exception on row 0 with id 0013n00001uAbFGAA0; first error: CANNOT_INSERT_UPDATE_ACTIVATE_ENTITY, boomtownapp.AccountTrigger: execution of AfterUpdate caused by: System.AsyncException: Rate Limiting Exception : AsyncApexExecutions Limit exceeded.
"""

# New errors
"""
Update failed. First exception on row 0 with id 0012S00002GWsZ0QAL; first error: UNABLE_TO_LOCK_ROW, unable to obtain exclusive access to this record or 25 records: 0012S00002GWsZ0QAL,0012S00002GWw1xQAD,0012S00002GWyv7QAD,0012S00002GWsYWQA1,0012S00002GWtORQA1,0012S00002GWxPwQAL,0012S00002GWxmrQAD,0012S00002GWk2oQAD,0012S00002GX1cHQAT,0012S00002GWgnfQAD, ... (15 more): []
"""
"""
Update failed. First exception on row 0 with id 001f400000R8snnAAB; first error: CANNOT_INSERT_UPDATE_ACTIVATE_ENTITY, boomtownapp.AccountTrigger: execution of AfterUpdate caused by: System.QueryException: Record Currently Unavailable: The record you are attempting to edit, or one of its related records, is currently being modified by another user. Please try again.
"""


def send_data_to_salesforce(data, salesforce_client):
    body = {"merchant_data": list()}
    for row in data:
        body["merchant_data"].append({
            # "sMerchant": f"{row['sMerchant']}" if row.get("sMerchant") is not None else "",
            "sMerchantNumber": f"{row['sMerchantNumber']}" if row.get("sMerchantNumber") is not None else "0",
            "sMTDMerchantVolume": f"{row['sMTDMerchantVolume']}" if row.get("sMTDMerchantVolume") is not None else "0",
            "sYTDMerchantVolume": f"{row['sYTDMerchantVolume']}" if row.get("sYTDMerchantVolume") is not None else "0",
            "sMTDMerchantTransaction": f"{row['sMTDMerchantTransaction']}"
            if row.get("sMTDMerchantTransaction") is not None else "0",
            "sYTDMerchantTransaction": f"{row['sYTDMerchantTransaction']}"
            if row.get("sYTDMerchantTransaction") is not None else "0",
            "sReportDate": f'{format_sf_timestamp(row["sReportDate"])}'
            if row.get("sReportDate") else ""
        })
    resp = salesforce_client.post_data(body)
    if resp:
        if not isinstance(resp, dict):
            raise RuntimeError(f"Unexpected response from Salesforce: {resp}")
        return_tuple = int(resp.get("merchant_data_size", 0)), int(resp.get("updating_account_list_size", 0))
        if return_tuple == (0, 0):
            pprint.pprint(resp)
        else:
            return return_tuple
    return 0, 0
    # pprint.pprint(resp)


if __name__ == "__main__":
    load_dotenv()
    reporting_server = os.getenv("reporting_server")
    reporting_db = os.getenv("reporting_database")
    reporting_db_user = os.getenv("reporting_user")
    reporting_db_pw = os.getenv("reporting_password")

    sf_consumer_key = os.getenv("salesForceConsumerKey")
    sf_consumer_secret = os.getenv("salesForceConsumerSecret")
    sf_endpoint = os.getenv("salesForceEndpoint")
    sf_login_url = os.getenv("salesForceLoginUrl")
    sf_user = os.getenv("salesForceUerName")
    sf_password = os.getenv("salesForcePassword")

    sf_client = SalesforceClient(sf_user, sf_password, sf_login_url, sf_consumer_key, sf_consumer_secret, sf_endpoint)

    conn = pymssql.connect(reporting_server, reporting_db_user, reporting_db_pw, reporting_db)
    cursor = conn.cursor(as_dict=True)

    if not tables:
        cursor.execute("SELECT sobjects.name FROM sysobjects sobjects WHERE sobjects.xtype='U'")
        all_tables_list = [t["name"] for t in cursor.fetchall()]
        all_tables_list.sort()
        print('\n'.join(all_tables_list))
        while not tables:
            table = input("Type a table name to process: ")
            if table and table in all_tables_list:
                tables = [table]
            else:
                print("You must enter the name of an existing table!")

    skip_tables = list()
    table_count = 0
    print(f"{time_string()}: Start!")
    for t, table_name in enumerate(tables):
        merch_data_count = 0
        update_count = 0
        if len(tables) == 1:
            skip_yes_no = 'Y'
        else:
            print("Type 'Y' to agree to process the table, 'Q' to quit immediately, anything else to go to next table")
            skip_yes_no = input(f"Process table {table_name}? ")
            print("")
        if skip_yes_no.upper() == 'Y':
            table_count += 1
            cursor.execute(f"SELECT MID FROM BP_DAILY.dbo.{table_name} group by MID")
            total_rows = len(cursor.fetchall())
            total_pages = math.ceil(total_rows / rows_per_page)
            print(f"{time_string()}: Initiating data feed into SF to {table_name} with {total_pages} pages and "
                  f"{total_rows} rows")
            general_query_str = "SELECT ReportDate as sReportDate, MID as sMerchantNumber, " \
                                "SUM(CAST(MTDTransactionCount as numeric(18,4))) as sMTDMerchantTransaction, " \
                                "SUM(CAST(MTDTransactionDollarVol as numeric(18,4))) as sMTDMerchantVolume, " \
                                "SUM(CAST(YTDTransactionDollarVol as numeric(18,4))) as sYTDMerchantVolume, " \
                                "SUM(CAST(YTDTransactionCount as numeric(18,4))) as sYTDMerchantTransaction " \
                                f"FROM BP_DAILY.dbo.{table_name} group by MID,ReportDate ORDER BY MID DESC " \
                                "OFFSET %d ROWS FETCH NEXT %d ROWS ONLY"
            # Nice for getting the merchant name but the query produces slightly different results, so...
            # general_query_str = "SELECT ReportDate as sReportDate, MerchantAccountName as sMerchant, " \
            #                     "MID as sMerchantNumber, " \
            #                     "SUM(CAST(MTDTransactionCount as numeric(18,4))) as sMTDMerchantTransaction, " \
            #                     "SUM(CAST(MTDTransactionDollarVol as numeric(18,4))) as sMTDMerchantVolume, " \
            #                     "SUM(CAST(YTDTransactionDollarVol as numeric(18,4))) as sYTDMerchantVolume, " \
            #                     "SUM(CAST(YTDTransactionCount as numeric(18,4))) as sYTDMerchantTransaction " \
            #                     f"FROM BP_DAILY.dbo.{table_name} group by MID,ReportDate,MerchantAccountName" \
            #                     " ORDER BY MID DESC OFFSET %d ROWS FETCH NEXT %d ROWS ONLY"
            for curr_page in range(1, total_pages + 1):
                cursor.execute(general_query_str, ((curr_page - 1) * rows_per_page, rows_per_page))
                rows = cursor.fetchall()
                try:
                    ds, us = send_data_to_salesforce(rows, sf_client)
                    if ds != us:
                        print(f"mds {ds} != uals {us} in page {curr_page} (MIDs {[r.sMerchantNumber for r in rows]})")
                    merch_data_count += ds
                    update_count += us
                except Exception as e:
                    print(f"Error on page {curr_page}: {e}")
        elif skip_yes_no.upper() == 'Q':
            skip_tables.extend(tables[t:])
            break
        else:
            skip_tables.append(table_name)
            continue
        print(f"{time_string()}:   Final merchant_data_size: {merch_data_count}")
        print(f"{time_string()}:   Final updating_account_list_size: {update_count}")
    print(f"{time_string()}: Ran {table_count} tables")
    if skip_tables:
        print(f"Skipped these tables:\n{skip_tables}")
