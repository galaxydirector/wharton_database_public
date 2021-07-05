proc sql;
    create table comp_quarter as
    select distinct *
    from comp.fundq
    where indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C' /*”Standard” Compustat Filters*/;
quit;