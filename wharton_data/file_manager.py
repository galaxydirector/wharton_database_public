"""
This file serves the purpose of zipping a whole directory files into a structured directory
Executing parellel with multi CPU cores.
# This is 7.30.2018 version rewritten by Yancy. Original on is in historical_files/file_manager730 

Compression rate in linux "zip"
when choose -5, it would take 4.5s per ticker to compress 6.5 times from original file
when choose -8, it would take 30s per ticker per year to compress 7 times from original file
"""

import multiprocessing as mp
from multiprocessing import Pool, cpu_count
import time
from glob import glob
from os import system
import os.path as path
from tqdm import tqdm



class FileManager(object):

	def __init__(self, current_folder):
		self.current_folder = current_folder

	def create_archive_name(self, f_path):
		splitted_path = f_path.split('/')
		year = splitted_path[-3]
		symbol = splitted_path[-2]

		return "{}_{}.zip".format(symbol, year)

	def find_ticker_path(self):
		return glob(self.current_folder+"seconds_data/????/*/")

	def zip_it(self, f_path, compress_ratio=5):
		'''
		-m 将文件压缩并加入压缩文件后，删除原始文件，即把文件移到压缩文件中。
		(-f and -u are used only if zip file has been exist)
		-f 此参数的效果和指定"-u"参数类似，但不仅更新既有文件，如果某些文件原本不存在于压缩文件内，使用本参数会一并将其加入压缩文件中。
		-u 更换较新的文件到压缩文件内 (-f is slightly different) 
		(explanation for -f and -u is wrong, -f would update existing files, while -u
		would update all files available in the directory)
		-<压缩效率> 压缩效率是一个介于1-9的数值。
		-r 递归处理，将指定目录下的所有文件和子目录一并处理。
		-j 只保存文件名称及其内容，而不存放任何目录名称
		-q does not show detail messages while zipping

		Input: a single file path
		'''
		archive_name = self.create_archive_name(f_path)
		archive_path = path.join(f_path, archive_name)

		data_path = glob(path.join(f_path, '*.csv'))
		zip_files = glob(path.join(f_path, '*.zip'))

		if len(data_path) == 0 and len(zip_files) ==0:
			print("no file found and no zip found for {}".format(archive_name)) 
		elif len(data_path) == 0 and len(zip_files) !=0:
			print("files have been ziped in {}".format(archive_name))

		elif len(data_path) !=0 and len(zip_files) !=0:
			system('zip -u -m -r -j -q -{} {} {}'.format(compress_ratio, archive_path, f_path))
		elif len(data_path) !=0 and len(zip_files) ==0:
			system('zip -m -r -j -q -{} {} {}'.format(compress_ratio, archive_path, f_path))

	def parellel_zip(self):
		"""This function serves the purpose of zipping files using 
		system zip command to parellel executing the task"""
		folder_paths = self.find_ticker_path()
		NUM_JOBS = len(folder_paths)
		NUM_PROCESSES = 3 # number of cores you want to ultilize

		with Pool(processes=NUM_PROCESSES) as p:
			with tqdm(total=NUM_JOBS, desc='Parallel Processing') as pbar:
				for result in p.imap_unordered(self.zip_it, folder_paths):
					pbar.update()

	def single_thread_zip(self):
		"""The functionality of this function is same to parellel_zip."""
		folder_paths = self.find_ticker_path()
		for i in tqdm(folder_paths):
			self.zip_it(i)

def main():
	current_folder = path.expanduser('~/Desktop/research/parellel_zip/')
	f_manager = FileManager(current_folder)

	f_manager.parellel_zip()


if __name__ == '__main__':
	start = time.time()
	main()
	duration = time.time() - start
	print("duration {}".format(duration))






