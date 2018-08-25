# wharton_database_public
# README #
This is a temporary solution to extract data from database saving in form of SAS and SQL,
cleaning and unifying them into the format needed.

Note: All account sensitive information has been replaced with 'xxxxx'
file_manager depends on Debian os.system

Files:
data_cleaner;
data_server;
data_streamer;
file_manager;

How to run:
For the mode of running through all years and then the next ticker, symbols.json file is not needed.

demo activation:
1. zip all the files
python data_streamer.py --organize_only
2. python data_streamer.py -s_list AAPL,BAC,MSFT -sd 20020101 -ed 20171231 -src auto --switch_at 2014 --save_step 100 --pull_data_only

Collaborator: Steve Lan, Mingyang Li
