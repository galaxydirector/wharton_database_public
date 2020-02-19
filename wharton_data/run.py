import time
from os import system


"""

421-440
DWDP,DTE,ETN,EBAY,EIX,ETR,ES,EXC,FITB,FE,F,BEN,FCX,GD,GE,GT,HAL,HOG,HIG

441-460
HSY,HUM,HBAN,IBM,IP,JNPR,K,KMB,KLAC,KSS,KR,LLL,LEG,L,MTB,M,MAR,MKC,MCK,MRK

461-506
MET,MCO,MS,MSI,NI,NSC,NTRS,OMC,PAYX,PEP,PCG,PNW,PPG,PPL,PG,PEG,QCOM,RTN,ROK,SEE,SRE,SO,SPGI,SBUX,STT,TROW,TPR,TXN,TMO,TSN,USB,UNP,UTX,VLO,VIAB,VNO,WM,WAT,WU,WRK,WY,WHR,XEL,XRX,XL


"""

try:
	system('python data_streamer.py -s_list MET,MCO,MS,MSI,NI,NSC,NTRS,OMC,PAYX,PEP,PCG,PNW,PPG,PPL,PG,PEG,QCOM,RTN -sd 20020101 -ed 20171231 -src auto --switch_at 2014 --save_step 100 --pull_data_only')
except Exception as e:
	print("error occured ", e)
else:
	try:
		time.sleep(300)
		system('python data_streamer.py -s_list ROK,SEE,SRE,SO,SPGI,SBUX,STT,TROW,TPR,TXN,TMO,TSN,USB,UNP,UTX,VLO,VIAB,VNO,WM,WAT,WU,WRK,WY,WHR,XEL,XRX,XL -sd 20020101 -ed 20171231 -src auto --switch_at 2014 --save_step 100 --pull_data_only')
	except Exception as e:
		print("error occured ", e)
	# else:
	# 	system('python data_streamer.py --organize_only')


if __name__ == '__main__':
	pass
