proc sql;
    create table comp_annual as
    select distinct *
    from comp.funda
    where indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C' /*”Standard” Compustat Filters*/;
quit;