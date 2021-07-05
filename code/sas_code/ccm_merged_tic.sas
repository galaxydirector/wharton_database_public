/* Compustat Data: 53,448,781 */

proc sql;
create table comp as
select datadate, gvkey, iid, tic, conm, prcod, prccd, prchd, prcld, cshtrd, cshoc, divd, div 
from comp.secd
where exchg in (11, 12, 14);
/* 11	New York Stock Exchange	United States
12	American Stock Exchange	United States
14	NASDAQ-NMS Stock Market	United States*/
quit;

/* CRSP Data: 63,407,872 */

proc sql ;
create table crsp as
select a.date, a.permno, a.permco, coalesce(ret,.) as ret, coalesce(retx,.) as retx, vol, shrout, openprc, prc
from crspm.dsf(where=(date>='01Jan1983'd)) as a
inner join crsp.dsenames(where=(exchcd in (1,2,3))) as b
/* 1)NYSE(exchcd=1), AMEX(=2), and NASDQA (=3) */
on a.permno=b.permno and NAMEDT<=date<=NAMEENDT;
quit;

/* CCM: 48,460,877 */

proc sql ;
create table ccm as
select a.*, b.lpermno as permno, b.linkprim
from comp as a, crsp.Ccmxpf_lnkhist as b
where a.gvkey=b.gvkey
and linktype in ('LC' 'LU') 
/*1)Confirmed Linkage Only*/
and linkprim in ('P','C') 
/*2)Take Primary Shres Only*/
and linkdt<= datadate <=coalesce(linkenddt, today()) 
/*3)Matched based on the first valid June after Fiscal Year END*/ 
order by permno, datadate;
quit;

/* CCM_Merged: 46,723,442 */

proc sql ;
create table ccm_merged as
select a.*, b.*
from ccm as a, crsp as b
where a.permno=b.permno and a.datadate = b.date
order by permno, datadate;
quit;


/* Permno & ticker: 96,310 */
proc sql ;
create table crsp_ticker as
select permno, permco, namedt, nameendt, ticker, comnam, tsymbol
from crspm.dsenames
where exchcd in (1,2,3) ;
quit;
/* Compustat TIC: the newest one */
/* CRSP Ticker: historical */


/* CCM_Merged with CRSP Ticker: ~46,722,939 */

proc sql ;
create table ccm_merged_tic as
select a.*, b.permno, b.ticker
from ccm_merged as a, crsp_ticker as b
where a.permno=b.permno and namedt<= a.date <= nameendt
order by datadate;
/* order by permno, datadate; */
quit;


/* proc export data=ccm_merged_tic
   outfile=
 '/home/wrds/mxiawrds/sasuser.v94/PriceData/ccm_merged_tic.csv'
   dbms=csv replace;
run;

PROC EXPORT DATA=ccm_merged_tic
   OUTFILE='/home/wrds/mxiawrds/sasuser.v94/PriceData/ccm_merged_tic.tsv'
       DBMS=TAB REPLACE;
   PUTNAMES=YES;
RUN; */

/* Compustat TIC: the newest one */
/* CRSP Ticker: historical */

/* END */

