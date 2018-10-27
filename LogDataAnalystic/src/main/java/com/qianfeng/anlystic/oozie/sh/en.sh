#!/bin/bash

#running  date.如果没有传运行日期的参数，则默认使用昨天的的时间. ./en.sh -z 47839 -re -d 2018-07-10
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
with tmp as(
select
from_unixtime(cast(de.s_time/1000 as bigint),'yyyy-MM-dd') dt,
de.pl pl,
de.ca ca,
de.ac ac
from dw_event de
where de.month = "${month}" and day = "${day}"
and de.pl is not null
) from (
select pl as pl,dt,ca as ca,ac as ac,count(*) as ecount from tmp group by pl,dt,ca,ac union all
select 'all' as pl,dt,ca as ca,ac as ac,count(*) as ecount from tmp group by dt,ca,ac union all
select pl as pl,dt,ca as ca,'all' as ac,count(*) as ecount from tmp group by pl,dt,ca union all
select 'all' as pl,dt,ca as ca,'all' as ac,count(*) as ecount from tmp group by dt,ca
) as tmp2
insert overwrite table dm_event
select platform_convert(pl),date_convert(dt),event_convert(ca,ac),sum(ecount),dt
group by pl,dt,ca,ac
;
"

#run sqoop statment
sqoop export --connect jdbc:mysql://hadoop01:3306/result \
--username root --password root --table stats_event \
--export-dir '/hive/lda.db/dm_event/*' \
--input-fields-terminated-by "\\01" --update-mode allowinsert \
--update-key platform_dimension_id,date_dimension_id,event_dimension_id \
;

echo "event job is finished."









