import subprocess
import time
import happybase
# https://happybase.readthedocs.io/en/latest/user.html
# https://github.com/lyveng/pandas-hbase/blob/master/pdhbase/__init__.py

from pymongo import MongoClient
from tqdm import tqdm

# wrds-rd thread pool size 300 is optimal
client = MongoClient('mongodb://localhost:27888', maxPoolSize=300)
db = client['10-Q_parsed']['May2021Algo']


def write_to_hbase(pool, mongo_db, table_name, step, cf="cf1"):
    """

    Parameters
    ----------
    pool, happybase.ConnectionPool obj
    mongo_db, mongoDB obj
    table_name, string, table name in hbase database
    step, int, send to database with number of rows per time
    cf, string, column name for data to store

    """
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

        # list out all records in a iterator
        records = mongo_db.find()

        def read_from_mongo():
            line = records.next()
            _row_key = line["_id"]
            del line["_id"]

            _row_value = {':'.join((cf, key)): str(val) for key, val in line.items()}

            return str(_row_key), _row_value

        # dump data
        for i in tqdm(range(records.count())):  # records.count()
            #         for i in tqdm(range(650)): # records.count()
            row_key, row_value = read_from_mongo()

            batch.put(row_key, row_value)
            if (i + 1) % step == 0:
                try:
                    batch.send()
                except Exception as e:
                    print(e)
                    print("continue to run")

        batch.send()

    print("operation takes ", time.time() - start_time)


write_thread = 300
host = "localhost"
pool = happybase.ConnectionPool(size=write_thread, host=host, port=9090, autoconnect=True, timeout=1000000)

write_to_hbase(pool, mongo_db=db, table_name='tenq', step=10, cf="cf1")


"""
Hyper parameter tuning on wrds-rd

6500 data, read_thread = 100
write_thread = 50
takes 324s

write_thread = 300
takes 275s

write_thread = 1000
takes 282s
-------------------
6500 data, write_thread = 300
read_thread = 100
takes 275s

read_thread = 300
takes 247s
-------------------
6500 data, read_thread = 300, write_thread = 300

step = 10
takes 247s

step = 100
takes 232s

"""