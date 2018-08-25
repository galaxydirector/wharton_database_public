import re
import os
import os.path as path
import time
from glob import glob
from tqdm import tqdm
import pandas as pd
import argparse
import subprocess
import threading
from datetime import datetime as dt
from itertools import product
from multiprocessing.dummy import Pool as ThreadPool
# improvement needed: '~/sas_scripts' directory needs to be cleaned periodically

"""
WRDS TAQ Data Streamer Server Side

The basic idea of this module is that the client controls this server module through
shell command. The client designates a date range, and the server automatically generates
a sas script to extract the data. In this way, we can take advantage of the built-in distributed
computation capability provided by WRDS. 

The first step to accomplish the goal is to write a SAS script template. Then the server module
can submit jobs by batch to fully utilize the distributed computation capability.


"""

class DataRequestServer(object):
    """ Data request server module receives data requests from a client.
    The server module automatically generates corresponding sas script
    from a template file to fulfill the requests.

    """
    template = ''

    def __init__(self, mode, switch_year=None):
        self.mode = mode
        self.switch_year = switch_year
        self.current_time  = dt.now().strftime("%Y-%m-%d_%H%M%S")
        self.req_cache_dir = self._create_request_cache_dir()
        self.monthly_template = open(path.expanduser("~/sas_scripts/monthly_template.sas"), 'r').read()
        self.daily_template = open(path.expanduser("~/sas_scripts/msec_template.sas"), 'r').read()
        self.template = self.daily_template
        self.allow_switch = True

        if mode == 'auto':
            self.template = self.monthly_template
            self.template_name = 'monthly'
        elif mode in {'SAS', 'monthly', 'sas'}:
            self.template = self.monthly_template
            self.template_name = 'monthly'
        elif mode in {'msec', 'SQL', 'sql', 'daily'}:
            self.template = self.daily_template
            self.template_name = 'daily'

    def _save_sas_script(self, script, symbol, date):
        script_path = path.expanduser("~/sas_scripts/")
        script_path = path.join(script_path, "{}_{}.sas".format(symbol, date))

        open(script_path, 'w+').write(script)

        return script_path

    def _create_request_cache_dir(self, symbol=None):

        if symbol is None: export_dir = path.join(path.expanduser('~/data_cache'), self.current_time)
        else:              export_dir = path.join(path.expanduser('~/data_cache'), self.current_time, symbol)

        if not path.isdir(export_dir): os.makedirs(export_dir)

        return export_dir

    def switch_template(self):
        print("Currently using template for TAQ %s product."%self.template_name)
        if self.allow_switch and self.template_name == 'monthly':
            self.template = self.daily_template
            self.template_name = 'daily'
            self.allow_switch = False
        elif self.allow_switch and self.template_name == 'daily':
            self.template = self.monthly_template
            self.template_name = 'monthly'
            self.allow_switch = False

        print("Switch template to get TAQ %s product."%self.template_name)


    def request_sas(self, symbol, date):
        """ Create a job for WRDS node from template."""
        export_dir = self._create_request_cache_dir(symbol)
        data_file_name = "{}_{}.csv".format(symbol, date)
        full_path = path.join(export_dir, data_file_name)

        new_script = self.template[:]\
                        .replace('{symbol}', repr(symbol))\
                        .replace('{date}', date)\
                        .replace('{filename}', repr(full_path))

        script_path = self._save_sas_script(new_script, symbol, date)
        return subprocess.call(["qsas", script_path])


def setup_args():
    def _str_to_bool(v):
        '''Stolen from <https://stackoverflow.com/a/43357954>.'''
        if    v.lower() in ('yes', 'true', 't', 'y', '1'): return True
        elif  v.lower() in ('no', 'false', 'f', 'n', '0'): return False
        else: raise argparse.ArgumentTypeError('Boolean value expected.')

    _str_to_list = lambda s: s.strip(" ").split(',')

    parser = argparse.ArgumentParser(description='Perform data request on the server side of WRDS.')
    parser.add_argument('-clean', type=_str_to_bool, default=None)
    parser.add_argument('-s_list', '--symbol_list', type=_str_to_list, default=None)
    parser.add_argument('-sd', '--start_date' , type=str, help="follow yyyymmdd format. start_date <= end_date")    
    parser.add_argument('-ed', '--end_date'   , type=str, help="follow yyyymmdd format. start_date <= end_date")    
    parser.add_argument('-xp', '--export_path', type=str, default="~/data_cache/")
    parser.add_argument('-mode', type=str, default='auto')
    parser.add_argument('--switch_year', type=int, default=None)

    return parser.parse_args()

def delete(pattern='./*.log', desc='log'):
    # Clean log files:
    log_files = glob(pattern)
    if len(log_files)>0:
        #print('Cleaning out %d %s files.' % (len(log_files), desc))
        [os.remove(f) for f in log_files]

def main():
    delete('./*.log', desc='log')
    #delete('./data_cache/2018-*', desc='cache')
    os.system('rm -r ./data_cache/2018-*') #TODO: make `delete()` work with directories.

    args = setup_args().__dict__

    server = DataRequestServer(args['mode'], args['switch_year'])

    # We will be iterating over these two lists of arguments:
    symbol_list = args['symbol_list']
    date_range  = pd.date_range(args['start_date'], args['end_date']).strftime('%Y%m%d')

    N = len(symbol_list)*len(date_range)
    assert N>0, 'No job scheduled!'

    # Cartisian-product 'em:
    for date_str, symbol in product(date_range, symbol_list):
        if server.allow_switch and args['switch_year'] is not None and args['mode'] == 'auto' and args['switch_year'] <= int(date_str[:4]):
            server.switch_template()
        server.request_sas(symbol, date_str)

    with tqdm(total=N, desc='Server waiting for cloud.') as pbar:
        while True:
            n_still_going = subprocess.check_output(['qstat', '-u', 'xxxxx']).decode().count('xxxxx')
            n_done = N-n_still_going
            n_registered = pbar.n
            n_to_register = n_done-n_registered
            pbar.update(max(0, n_to_register))
            # print(' -- Now we are having %d jobs done out of %d.' % (N-n, N))
            if n_still_going==0: break
            time.sleep(1)

    # Clean log files:
    delete('./*.log', desc='log')


if __name__ == '__main__':
    main()


