import os.path as path
import saspy
import pandas as pd
import time
import happybase
from tqdm import tqdm

host = "localhost"
sas = saspy.SASsession(cfgname='default')

crsp_split_code = open(path.expanduser("sas_files/crsp_split.sas"), 'r').read()
ccm_merged_code = open(path.expanduser("sas_files/ccm_merged_tic.sas"), 'r').read()

sas.submit(crsp_split_code)['LOG'].split('\n')
sas.submit(ccm_merged_code)['LOG'].split('\n')

crsp_split = sas.sd2df(table='crsp_split', libref='work')
ccm_merged = sas.sd2df(table='ccm_merged_tic', libref='work', method="DISK")

# Table Processed
crsp_split["EXDT"] = crsp_split.EXDT.dt.date
crsp_split["PERMNO"] = crsp_split.PERMNO.astype(int)
ccm_merged["datadate"] = ccm_merged.datadate.dt.date
ccm_merged["permno"] = ccm_merged.permno.astype(int)


# Dump Data
def process_facpr_column(df, i, cf="priceVol"):  # only for split
    """
    process column "facpr" per line
    """
    row_key = str(df.loc[i]['PERMNO']) + "#" + str(df.loc[i]['EXDT'])

    row_value = dict()
    if not pd.isnull(df.loc[i]["FACPR"]):
        row_value[':'.join((cf, "FACPR"))] = str(format(df.loc[i]["FACPR"], ".4f"))

    return row_key, row_value


info_col_name = ["cshoc", "ret", "retx", "divd"]


def process_info_columns(df, i, columns, cf="info"):
    """
    process "info" columnsper line
    """
    row_key = str(df.loc[i]['permno']) + "#" + str(df.loc[i]['datadate'])

    row_value = dict()
    for column in columns:
        value = df.loc[i][column]
        if not pd.isnull(value):
            row_value[':'.join((cf, column))] = str(format(abs(value), ".8f")) if (
                        column == "ret" or column == "retx") else str(value)  # format(abs(value),".8f"),abs(value)

    return row_key, row_value


priceVol_col_name = ["prcod", "prccd", "prchd", "prcld", "cshtrd"]


def process_priceVol_columns(df, i, columns, cf="priceVol"):
    """
    process "priceVol" columns per line
    """
    row_key = str(df.loc[i]['permno']) + "#" + str(df.loc[i]['datadate'])

    row_value = dict()
    for column in columns:
        value = df.loc[i][column]
        if not pd.isnull(value):
            row_value[':'.join((cf, column))] = str(value)
    return row_key, row_value


def write_to_hbase(pool, table_name, crsp_split, ccm_merged, step):
    """
    Parameters
    ----------
    pool: happybase.ConnectionPool obj
    table_name: string, table name in hbase database
    crsp_split: dataframe, generated from SAS
    ccm_merged: dataframe, generated from SAS
    step: int, send to database with number of rows per time

    """
    priceVol_col_name = ["prcod", "prccd", "prchd", "prcld", "cshtrd"]
    info_col_name = ["cshoc", "ret", "retx", "divd"]

    start_time = time.time()
    with pool.connection() as connection:
        print("List all tables: ", connection.tables())
        table = connection.table(table_name)
        batch = table.batch()

        # Add all column type, hard coding, all of them are float64
        column_dtype_key = 'column_data_type'
        column_dtype_value = dict()
        for column in priceVol_col_name + ["FACPR"]:
            column_dtype_value[':'.join(("priceVol", column))] = 'float64'

        for column in info_col_name:
            column_dtype_value[':'.join(("info", column))] = 'float64'
        batch.put(column_dtype_key, column_dtype_value)
        batch.send()

        # Dump data from crsp_split for column "facpr"
        for i in tqdm(range(len(crsp_split))):
            #         for i in range(10):
            row_key, row_value = process_facpr_column(crsp_split, i, cf="priceVol")

            batch.put(row_key, row_value)
            if (i + 1) % step == 0:
                batch.send()

        # Dump data from ccm_merged from crsp merged with ccm table
        for i in tqdm(range(len(ccm_merged))):
            #         for i in range(500):
            row_key_ccm_info, row_value_ccm_info = process_info_columns(ccm_merged, i, columns=info_col_name, cf="info")
            batch.put(row_key_ccm_info, row_value_ccm_info)

            row_key_ccm_price, row_value_ccm_price = process_priceVol_columns(ccm_merged, i, columns=priceVol_col_name,
                                                                              cf="priceVol")
            batch.put(row_key_ccm_price, row_value_ccm_price)

            if (i + 1) % step == 0:
                batch.send()

        batch.send()

    print("operation takes ", time.time() - start_time)


pool = happybase.ConnectionPool(size=100, host=host, port=9090, autoconnect=True, timeout=1000000)
write_to_hbase(pool, 'daily_price_vol', crsp_split, ccm_merged, step=100)
