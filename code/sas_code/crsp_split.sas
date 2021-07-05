
/* Split Record: 37,236 */
proc sql ;
create table crsp_split as
select permno, permco, dclrdt, distcd, divamt, facpr, facshr, exdt, rcrddt, paydt
from crspm.dsedist
where 5000 <= distcd <6000 and hexcd in (1,2,3);
quit;

/* proc export data=crsp_split
   outfile=
 '/home/wrds/yanci/data_to_hbase/crsp_split.csv'
   dbms=csv replace;
run; */

/* PROC EXPORT DATA=crsp_split
   OUTFILE='/home/wrds/mxiawrds/sasuser.v94/PriceData/crsp_split.tsv'
       DBMS=TAB REPLACE;
   PUTNAMES=YES;
RUN; */


/* END */

