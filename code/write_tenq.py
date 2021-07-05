import subprocess
import time
import happybase
import pandas as pd
import numpy as np
# https://happybase.readthedocs.io/en/latest/user.html
# https://github.com/lyveng/pandas-hbase/blob/master/pdhbase/__init__.py

from pymongo import MongoClient
import pandas as pd
import html2text
from tqdm import tqdm
import concurrent.futures
# wrds-rd thread pool size 300 is optimal
client = MongoClient('mongodb://localhost:27888',maxPoolSize=300)
db = client['10-Q_parsed']['May2021Algo']

def write_to_hbase(pool, mongo_db, table_name, step, cf="cf1"):
    start_time = time.time()
    with pool.connection() as connection:
        print("list all tables: ", connection.tables())
        table = connection.table(table_name)
        batch = table.batch()
        
#         # create a row for column type
#         column_dtype_key = 'column_data_type'
#         column_dtype_value = dict()
#         for column in df.columns:
#             column_dtype_value[':'.join((cf, column))] = df.dtypes[column].name
#         batch.put(column_dtype_key, column_dtype_value)
        
        records = mongo_db.find()
        
        def read_from_mongo():
            line = records.next()
            row_key = line["_id"]
            del line["_id"]
            
            row_value = {':'.join((cf, key)):str(val) for key,val in line.items()}

            return str(row_key), row_value
        
        
        # dump data
        for i in tqdm(range(records.count())): # records.count()
        #         for i in tqdm(range(650)): # records.count()
            row_key, row_value = read_from_mongo()
            
            batch.put(row_key, row_value)
            if (i+1) % step == 0:
                try:
                    batch.send()
                except Exception as e:
                    print(e)
                    print("continue to run")
        
        batch.send()
        
    print("operation takes ", time.time()-start_time)
        
write_thread = 300
host = "localhost"
pool = happybase.ConnectionPool(size=write_thread,host=host,port=9090,autoconnect=True,timeout=1000000)

write_to_hbase(pool, mongo_db = db, table_name = 'tenq', step = 10, cf="cf1")