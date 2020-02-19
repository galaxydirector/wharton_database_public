import os, time, wrds, json, re, platform, shutil, argparse, subprocess
import pandas as pd
import datetime as dt
import os.path as path
from glob import glob
from tqdm import tqdm
from pandas._libs.tslib import Timestamp

import multiprocessing as mp

from data_cleaner import DataCleaner
from file_manager import FileManager

import logging, colorlog, colored_traceback.auto
# ------------------------------------------------------
class TqdmLoggingHandler (logging.Handler):
    '''https://stackoverflow.com/a/38739634/1147061'''
    def __init__ (self, level = logging.NOTSET):
        super (self.__class__, self).__init__ (level)
    def emit (self, record):
        try:
            msg = self.format (record)
            tqdm.write (msg)
            self.flush ()
        except (KeyboardInterrupt, SystemExit): raise
        except: self.handleError(record)   

rootLogger = logging.getLogger()            # access the root logger
logger = logging.getLogger(__name__)        # tell the program to send messages on its own behalf.
rootLogger.setLevel(logging.DEBUG)

tqdmHandler = TqdmLoggingHandler()
tqdmHandler.setFormatter(colorlog.ColoredFormatter('%(log_color)s%(name)16s%(reset)s %(bold)s%(levelname)8s%(reset)s|%(message)s'))
rootLogger.addHandler(tqdmHandler)
logger.debug('Logging is up!')


class DataStreamer(object):
    """ Wrapper class for streaming data from wrds taq database

    """
    date_pattern = re.compile(r'\d{8}')
    def __init__(self, symbol_list, start_date, end_date, export_path, cache_path, faulty_data_path, data_src,
                        cols='DATE, TIME_M, SYM_ROOT, PRICE, SIZE', save_result=True, switch_year=None,
                        include_otc_data=True, save_step=10):
        logger = logging.getLogger('DataStreamer Init')        # tell the program to send messages on its own behalf.
        # Validate arguments:
        assert self.date_pattern.match(start_date), ValueError('Start date should be an eight-digit integer.')
        assert self.date_pattern.match(  end_date), ValueError(  'End date should be an eight-digit integer.')

        self.symbol_list      = symbol_list
        self.start_date       = start_date
        self.end_date         = end_date
        self.export_path      = export_path
        self.cache_path       = cache_path
        self.faulty_data_path = faulty_data_path
        self.cols             = cols
        self.include_otc_data = include_otc_data
        self.save_step        = save_step
        self.data_cleaner     = DataCleaner()
        self.switch_year = switch_year

        logger.debug("Export cache to %s." % self.cache_path)

        # Standardize data source name to help locating directory
        if data_src == 'auto':
            self.data_src = 'auto'
        elif data_src in {'MSEC','msec', 'milliseconds', 'daily'}:
            self.data_src = 'daily'
        elif data_src in {'MONTHLY', "monthly", 'Monthly', 'minutes'}:
            self.data_src = 'monthly'
        elif data_src in {'sas', 'sql', 'SAS', 'SQL'}:
            print("Data source options are 'daily' and 'monthly'")
            exit()
        else:
            raise ValueError("Data Source not expected.")

    def _save_data(self, symbol, date, data, to_path, d_name=None): 
        data_name = d_name
        if type(date) is dt.datetime or type(date) is Timestamp: date = date.strftime('%Y%m%d') # Coerce datatype of date to string.
        data_name = "{}_{}.csv".format(symbol, date)

        if not path.isdir(to_path): os.makedirs(to_path) # Ensure directory exists.

        export_file_path = path.join(to_path, data_name)
        data.to_csv(export_file_path, index=False)

    def chunks(self, l, n):
        """Yield successive n-sized chunks from l.
            TODO: Use <https://docs.scipy.org/doc/numpy/reference/generated/numpy.split.html> instead. Should be much quicker.
        """
        for i in range(0, len(l), n):
            yield l[i:i + n]

    def fetch_by_batch(self, symbol_list, mode):
        """ Using multiprocessing to get data in parallel

        """
        logger = logging.getLogger(__name__)        # tell the program to send messages on its own behalf.
        processes = []

        for symbol in symbol_list:
            logger.debug("Preparing worker for fetching {}".format(symbol))
            worker = mp.Process(target=self.fetch_data_from_wrds, args=(symbol, mode, self.save_step))
            processes.append(worker)

        for partial_processes in self.chunks(processes, 2):
            # logger.debug(partial_processes)
            [w.start() for w in partial_processes]
            [w.join() for w in partial_processes]
            time.sleep(10)

    def _get_data(self, symbol_list_str, start_date_str, end_date_str, mode, switch_year=None):
        logger = logging.getLogger(__name__)        # tell the program to send messages on its own behalf.
        
        if switch_year is None:
            server_cmd = "python data_server.py -s_list {} -sd {} -ed {} -mode {}".format(symbol_list_str, start_date_str, end_date_str, mode)
        else:
            server_cmd = "python data_server.py -s_list {} -sd {} -ed {} -mode auto --switch_year {}".format(symbol_list_str, start_date_str, end_date_str, switch_year)
        
        # logger.debug("Command is %s"%server_cmd)
        cmd_list = ["ssh", "xxxxx@wrds-cloud.wharton.upenn.edu", "-t", server_cmd]
        logger.debug('Now attempting: `%s`' % ' '.join(cmd_list))
        conn_out = subprocess.run(cmd_list)
        assert conn_out.returncode==0, RuntimeError('WRDS side failed: `%s`.' % conn_out)
        logger.info('Now downloading data from WRDS to local machine.')
        sync_data_cmd = ["scp", "-r", "xxxxx@wrds-cloud.wharton.upenn.edu:~/data_cache/2018-*", self.cache_path]
        try:
            sync_out = subprocess.run(sync_data_cmd )
        except AssertionError as e:
            logger.debug('No file is downloaded.')
            return 1
        except subprocess.CalledProcessError as e:
            logger.debug(e)
            return 1

        return 0


    def _generate_save_path(self, file_path):
        symbol, year = self._get_symbol_and_year(file_path)
        return path.join(self.export_path, "seconds_data", year, symbol)


    def _get_symbol_and_year(self, d_path):
        f_name = d_path.split('/')[-1].split('_')
        symbol = f_name[0]
        year = f_name[1][:4]

        return symbol, year


    def _process_data(self, symbol_list, start_date_str, end_date_str):
        logger = logging.getLogger(__name__)        # tell the program to send messages on its own behalf.
        digital_start_date = int(start_date_str)
        digital_end_date   = int(end_date_str)
        def moveFile(from_path, to_path):
            logger = logging.getLogger(__name__)        # tell the program to send messages on its own behalf.
            try:                      shutil.move(from_path, to_path)
            except shutil.Error as e: logger.error('%s' % e)

        for sub_dir in os.listdir(self.cache_path):
            if '.DS_Store' in sub_dir: continue
            for symbol in symbol_list:
                root_path = path.join(self.cache_path, sub_dir, symbol)
                full_path = path.join(root_path, "*.csv")

                for d_path in tqdm(glob(full_path), desc=full_path):
                    try:
                        f_name = path.basename(d_path)
                        date   = f_name.split('_')[1].split('.')[0]

                        if not (digital_start_date <= int(date) <= digital_end_date):
                            logger.debug("No need to process %s as its date is not in the range." % f_name)
                            continue
                    except:
                        logger.error('Bad filename for `%s`.' % d_path)
                        continue

                    # We can read the file now:
                    logger.debug("Processing %s as its date is between %s and %s" % (f_name, start_date_str, end_date_str))
                    try:
                        df = pd.read_csv(d_path)
                        assert len(df) > 0
                    except (pd.errors.EmptyDataError, AssertionError) as e:
                        logger.warn("%s at day %s has empty data. Skipped. (Type: `%s`)" % (symbol, date, e))
                        moveFile(d_path, self.faulty_data_path)
                        logger.debug("Faulty data are moved to %s"%self.faulty_data_path)
                        continue
                    except Exception as e:
                        logger.error('Unexpected error occurred: `%s`. Skipping without even trying to move the file.' % e)
                        continue

                    # We can post-process now:
                    try:
                        processed_data, status = self.data_cleaner.post_process(df, symbol)
                        assert len(processed_data) > 0 and status == 0
                    except Exception as e:
                        logger.error("Data Processing failed (%s)! Moved faulty data to %s." % (e, self.faulty_data_path))
                        moveFile(d_path, self.faulty_data_path)
                        continue

                    # We can save the processed file now:
                    save_dest_dir = self._generate_save_path(d_path)
                    try:
                        self._save_data(symbol, date, processed_data, save_dest_dir)
                    except Exception as e:
                        logger.error('Unexpected error occurred: `%s` while trying to save the file to `%s`.' % (e, save_dest_dir))

        for sub_dir in os.listdir(self.cache_path):
            if '.DS_Store' not in sub_dir:
                dir_to_remove = path.join(self.cache_path, sub_dir)

                if path.isdir(dir_to_remove):
                    shutil.rmtree(dir_to_remove)
                    assert not path.isdir(dir_to_remove)

    def fetch_data_from_wrds(self, symbol_list_str, mode, switch_year=None):
        logger = logging.getLogger(__name__)        # tell the program to send messages on its own behalf.
        date_range = pd.date_range(self.start_date, self.end_date)
        logger.debug("A total of %d days are to be fetched." % len(date_range))
        start_dt   = date_range[0]
        end_dt     = date_range[-1]
        symbol_list = symbol_list_str.split(',')

        def _step(symbol, start_dt, end_dt, switch_year=None):
            logger = logging.getLogger(__name__)        # tell the program to send messages on its own behalf.
            '''This is like an abbrivation.'''
            # logger.debug("Fetching data from {} to {}.".format(start_dt, end_dt))
            logger.debug("Symbol requested is %s"%(symbol))

            if switch_year is not None:
                logger.debug("Data source will switch at %s"%switch_year)

            # Convert them to 8-digit strings:
            start_dt_str = start_dt.strftime('%Y%m%d')
            end_dt_str   = end_dt  .strftime('%Y%m%d')
            # Fetch the data via SAS and check returned status code:
            status = self._get_data(symbol, start_dt_str, end_dt_str, mode, switch_year)
            if status == 0: 
                logger.info('Monthly Data is successfully loaded. Processing.')
                self._process_data(symbol_list, start_dt_str, end_dt_str)

        if (end_dt-start_dt).days < self.save_step:
            logger.warn("Did you actually mean to have save step longer than the range of date? This can be done in one go. (Not using a progress bar.)")
            _step(start_dt = self.start_date, 
                  end_dt   = self.end_date,
                  switch_year = switch_year)
        else:
            logger.debug("Save step is fine-grained. Gotta split into steps. (See the progress bar.)")
            steps = pd.np.array_split(date_range, len(date_range) // self.save_step)
            for symbol in symbol_list:
                start_time = time.time()
                for step_range in tqdm(steps, desc='WRDS Loader'):
                    # Get the Pandas datetime objects:
                    _step(symbol,
                           start_dt = step_range[ 0],
                          end_dt   = step_range[-1],
                          switch_year=switch_year)
                duration = time.time() - start_time
                
                # send slack progress information
                sendSlack('{} has being downloaded, taking {} hours'.format(symbol, round(duration/3600,2)))


    def start_streaming(self, sym_list=None):
        logger = logging.getLogger(__name__)        # tell the program to send messages on its own behalf.
        if self.symbol_list is None:
            logger.debug('Starting to stream via batch method. Pulling data from multiple ticker at the same time.') 
            self.fetch_by_batch(sym_list, self.data_src)
        else:
            logger.debug('Starting to stream from WRDS.') 
            self.fetch_data_from_wrds(symbol_list_str = self.symbol_list, mode=self.data_src, switch_year=self.switch_year)



def read_symbols(sym_path):
    with open(sym_path) as f:
        symbols = json.load(f)

    return symbols

def setup_args(defaults):
    parser = argparse.ArgumentParser(description='Data download and streaming module for WRDS TAQ Database.')
    parser.add_argument('-s_list', '--symbol_list', type=str, default=None)
    parser.add_argument('--sandbox', action='store_true', default=False)
    parser.add_argument('--organize_only', action='store_true', default=False)
    parser.add_argument('--pull_data_only', action='store_true', default=False)

    parser.add_argument('-src', type=str, default='auto')
    parser.add_argument('--include_otc_data', action='store_true', default=True)
    parser.add_argument('-sg_path', '--symbol_group_path', type=str, default=defaults['symbol_path'])
    parser.add_argument('--faulty_data_path', type=str, default=defaults['faulty_data_path'])
    parser.add_argument('-sd', '--start_date', type=str, help="follow yyyymmdd format. start_date <= end_date")    
    parser.add_argument('-ed', '--end_date', type=str, help="follow yyyymmdd format. start_date <= end_date")    
    parser.add_argument('-xp', '--export_path', type=str, default=defaults['export_path'])
    parser.add_argument('-cp', '--cache_path', type=str, default=defaults['cache_path'])
    parser.add_argument('--save_step', type=int, default=10)
    parser.add_argument('--switch_at', type=int, default=None)


    args = parser.parse_args()

    return args


def main():
    logger = logging.getLogger('Local')        # tell the program to send messages on its own behalf.
    DEFAULTS = {
        'export_path'     : path.expanduser('~/Time_Series/Data/wrds_data/'),
        'faulty_data_path': path.expanduser('~/Time_Series/Data/wrds_data/faulty_data/'),
        'cache_path'      : path.expanduser('~/Time_Series/Data/wrds_data/cache/'),
        'symbol_path'     : path.expanduser('~/Time_Series/WRDS_TAQ_public/symbols.json')}

    args = setup_args(defaults=DEFAULTS).__dict__
    

    # sandbox mode for testing file organization
    if args['sandbox']:
        args[ 'cache_path'] = path.expanduser('~/Time_Series/Data/sandbox/cache/')
        args['export_path'] = path.expanduser('~/Time_Series/Data/Desktop/sandbox/')

    symbols     = read_symbols(args['symbol_group_path'])['symbols']
    start_date  = args['start_date']
    end_date    = args['end_date']

    # If reaches here, arguments should be okay.
    logger.debug('Gonna read stock data from %s to %s for tickers %s.' % (start_date, end_date, symbols))

    if args['pull_data_only']:
        DataStreamer(
            symbol_list = args['symbol_list'], 
            start_date  = start_date, 
            end_date    = end_date,
            export_path = args['export_path'], 
            cache_path  = args['cache_path'], 
            data_src    = args['src'],
            save_step   = args['save_step'],
            faulty_data_path = args['faulty_data_path'],
            include_otc_data = args['include_otc_data'], 
            switch_year=args['switch_at']
        ).start_streaming(symbols)
    
    if args['organize_only']:
        f_manager = FileManager(args["export_path"])
        f_manager.parellel_zip()
        sendSlack('Compression Completed!')

    print("Task completed.")

if __name__ == '__main__':
    main()
    sendSlack('Done!')
    # try:
    #     main()
    # # except Exception as e:
    # #     sendSlack('Error: %s' % e)
    # else:
    #     sendSlack('Done!')
