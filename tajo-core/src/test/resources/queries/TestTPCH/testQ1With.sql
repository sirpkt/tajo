with a as (select * from lineitem)
select * from  a join lineitem b on a.l_returnflag=b.l_returnflag;