#!/bin/bash

#running  date. ./ed.sh -d 2018-07-10
run_date=''

until [ $# -eq 0 ]
do

if [ $1'x' = '-dx' ]
then
shift
run_date=$1
fi

shift

done

if [ ${#run_date} = 10 ]
then
echo "run date is argumnets."
else
run_date=$(date -d last-day "+%Y-%m-%d")
fi

echo "final run date is:${run_date}"

month=`date -d "$run_date" "+%m"`
day=`date -d "$run_date" "+%d"`
#echo $run_date | awk -F "-" '{ }'



#run hql
hive --database lda -e "
from (
select
from_unixtime(cast(dv.s_time/1000 as bigint),'yyyy-MM-dd') dt,
dv.pl pl,
dv.u_ud,
(case
when count(dv.p_url) = 1 then 'pv1'
when count(dv.p_url) = 2 then 'pv2'
when count(dv.p_url) = 3 then 'pv3'
when count(dv.p_url) = 4 then 'pv4'
when count(dv.p_url) >= 5 and count(dv.p_url) < 10 then 'pv5_10'
when count(dv.p_url) >= 10 and count(dv.p_url) < 30 then 'pv10_30'
when count(dv.p_url) >= 30 and count(dv.p_url) < 60 then 'pv30_60'
else 'pv60pluss' end) as pv
from dw_view dv
where dv.month = "${month}" and dv.day = "${day}"
and dv.pl is not null
group by dv.pl,dv.s_time,dv.u_ud) tmp
insert overwrite table dw_view_tmp
select pl,dt,pv,count(distinct u_ud) as ct
group by pl,dt,pv
;
"

#extend dim
hive --database lda -e "
with tmp as(
select pl,dt,ct as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dw_view_tmp where col='pv1' union all
select pl,dt,0 as pv1,ct as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dw_view_tmp where col='pv2' union all
select pl,dt,0 as pv1,0 as pv2,ct as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dw_view_tmp where col='pv3' union all
select pl,dt,0 as pv1,0 as pv2,0 as pv3,ct as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dw_view_tmp where col='pv4' union all
select pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,ct as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dw_view_tmp where col='pv5_10' union all
select pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,ct as pv10_30,0 as pv30_60,0 as pv60pluss from dw_view_tmp where col='pv10_30' union all
select pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,ct as pv30_60,0 as pv60pluss from dw_view_tmp where col='pv30_60' union all
select pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,ct as pv60pluss from dw_view_tmp where col='pv60pluss' union all
select 'all' as pl,dt,ct as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dw_view_tmp where col='pv1' union all
select 'all' as pl,dt,0 as pv1,ct as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dw_view_tmp where col='pv2' union all
select 'all' as pl,dt,0 as pv1,0 as pv2,ct as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dw_view_tmp where col='pv3' union all
select 'all' as pl,dt,0 as pv1,0 as pv2,0 as pv3,ct as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dw_view_tmp where col='pv4' union all
select 'all' as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,ct as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dw_view_tmp where col='pv5_10' union all
select 'all' as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,ct as pv10_30,0 as pv30_60,0 as pv60pluss from dw_view_tmp where col='pv10_30' union all
select 'all' as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,ct as pv30_60,0 as pv60pluss from dw_view_tmp where col='pv30_60' union all
select 'all' as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,ct as pv60pluss from dw_view_tmp where col='pv60pluss'
)
from tmp
insert into table dm_view
select platform_convert(pl),date_convert(dt),2,sum(pv1),sum(pv2),sum(pv3),sum(pv4),sum(pv5_10),sum(pv10_30),sum(pv30_60),sum(pv60pluss),'$run_date'
group by pl,dt
;
"

#run sqoop
sqoop export --connect jdbc:mysql://hadoop01:3306/result \
--username root --password root --table stats_view_depth \
--export-dir "/hive/lda.db/dm_view/*" \
--input-fields-terminated-by "\\01" --update-mode allowinsert \
--update-key platform_dimension_id,date_dimension_id \
;

echo "event job is finished."



#===========================浏览深度sessions