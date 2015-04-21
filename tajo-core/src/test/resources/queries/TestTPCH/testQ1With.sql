--with a as (select * from lineitem)
--select * from  a join lineitem b on a.l_returnflag=b.l_returnflag;
--select * from lineitem where 1=1;
select * from (select * from lineitem) a join lineitem b on a.l_returnflag=b.l_returnflag;