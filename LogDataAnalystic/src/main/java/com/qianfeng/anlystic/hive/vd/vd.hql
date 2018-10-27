--在原始表中抽取数据，放在dw层
create external table if not exists dw_view(
s_time bigint,
en string,
 pl string,
 p_url string,
 u_ud string,
 u_sid String
 )
 partitioned by(month String,day string)
 row format delimited fields terminated by '\001'
 ;


from ods_logs
insert into table dw_view partition(month=07,day=09)
select s_time,en,pl,p_url,u_ud,u_sid;


创建中间临时表：
create table if not exists dw_view_tmp(
pl string,
dt string,
col string,
ct int
)
;

pl dt pvX count

结果集tmp：
websit 2018-07-09 pv1 1
websit 2018-07-09 pv2 3
websit 2018-07-09 pv3 1
websit 2018-07-09 pv60pluss 5





##创建最终的结果表
CREATE TABLE if not exists dm_view (
  `platform_dimension_id` int,
  `data_dimension_id` int,
  `kpi_dimension_id` int,
  `pv1` int,
  `pv2` int,
  `pv3` int,
  `pv4` int,
  `pv5_10` int,
  `pv10_30` int,
  `pv30_60` int,
  `pv60plus` int,
  `created` String
  );

#查询语句  用户  dv.en = 'e_pv'
from (
select
from_unixtime(cast(dv.s_time/1000 as bigint),"yyyy-MM-dd") dt,
dv.pl pl,
dv.u_ud,
(case
when count(dv.p_url) = 1 then "pv1"
when count(dv.p_url) = 2 then "pv2"
when count(dv.p_url) = 3 then "pv3"
when count(dv.p_url) = 4 then "pv4"
when count(dv.p_url) >= 5 and count(dv.p_url) < 10 then "pv5_10"
when count(dv.p_url) >= 10 and count(dv.p_url) < 30 then "pv10_30"
when count(dv.p_url) >= 30 and count(dv.p_url) < 60 then "pv30_60"
else "pv60pluss" end) as pv
from dw_view dv
where dv.month = 7 and dv.day = 9
and dv.pl is not null
group by dv.pl,dv.s_time,dv.u_ud) tmp
insert overwrite table dw_view_tmp
select pl,dt,pv,count(distinct u_ud) as ct
group by pl,dt,pv
;

2018-07-09	java_server	27F69684-BBE3-42FA-AA62-71F98E208444	pv1
2018-07-09	website	27F69684-BBE3-42FA-AA62-71F98E208444	pv1

2018-07-09 java_server pv1 1
2018-07-09	website pv1 1


##扩维
with tmp as(
select pl,dt,ct as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dw_view_tmp where col="pv1" union all
select pl,dt,0 as pv1,ct as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dw_view_tmp where col="pv2" union all
select pl,dt,0 as pv1,0 as pv2,ct as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dw_view_tmp where col="pv3" union all
select pl,dt,0 as pv1,0 as pv2,0 as pv3,ct as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dw_view_tmp where col="pv4" union all
select pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,ct as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dw_view_tmp where col="pv5_10" union all
select pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,ct as pv10_30,0 as pv30_60,0 as pv60pluss from dw_view_tmp where col="pv10_30" union all
select pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,ct as pv30_60,0 as pv60pluss from dw_view_tmp where col="pv30_60" union all
select pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,ct as pv60pluss from dw_view_tmp where col="pv60pluss" union all
select "all" as pl,dt,ct as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dw_view_tmp where col="pv1" union all
select "all" as pl,dt,0 as pv1,ct as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dw_view_tmp where col="pv2" union all
select "all" as pl,dt,0 as pv1,0 as pv2,ct as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dw_view_tmp where col="pv3" union all
select "all" as pl,dt,0 as pv1,0 as pv2,0 as pv3,ct as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dw_view_tmp where col="pv4" union all
select "all" as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,ct as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dw_view_tmp where col="pv5_10" union all
select "all" as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,ct as pv10_30,0 as pv30_60,0 as pv60pluss from dw_view_tmp where col="pv10_30" union all
select "all" as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,ct as pv30_60,0 as pv60pluss from dw_view_tmp where col="pv30_60" union all
select "all" as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,ct as pv60pluss from dw_view_tmp where col="pv60pluss"
)
from tmp
insert into table dm_view
select platform_convert(pl),date_convert(dt),2,sum(pv1),sum(pv2),sum(pv3),sum(pv4),sum(pv5_10),sum(pv10_30),sum(pv30_60),sum(pv60pluss),'2018-07-09'
group by pl,dt
;

--使用sqoop语句将hive中的最终结果导出到mysql
sqoop export --connect jdbc:mysql://hadoop01:3306/result \
--username root --password root --table stats_view_depth \
--export-dir '/hive/lda.db/dm_view/*' \
--input-fields-terminated-by "\\01" --update-mode allowinsert \
--update-key platform_dimension_id,date_dimension_id \
;




