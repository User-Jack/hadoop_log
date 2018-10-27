--先创建表
create external table if not exists ods_logs(
s_time string,
en string,
ver string,
u_ud string,
u_mid string,
u_sid string,
c_time string,
language  string,
b_iev string,
b_rst string,
p_url string,
p_ref string,
tt string,
pl string,
o_id string,
`on` string,
cut string,
cua string,
pt string,
ca string,
ac string,
kv_ string,
du string,
os string,
os_v string,
browser string,
browser_v string,
country string,
province string,
city string
)
partitioned by(month String,day string)
row format delimited fields terminated by '\001'
;

--加载数据
load data inpath '/ods/month=07/day=09' into table ods_logs partition(month=07,day=09);
load data inpath '/ods/month=07/day=10' into table ods_logs partition(month=07,day=10);


--在原始表中抽取数据，放在dw层
create external table if not exists dw_event(
s_time string,
en string,
pl string,
ca string,
ac string
)
partitioned by(month String,day string)
row format delimited fields terminated by '\001'
;


from ods_logs
insert into table dw_event partition(month=07,day=09)
select s_time,en,pl,ca,ac;


--创建一个和结果表一模一样的临时表：
  create external table if not exists dm_event(
  `platform_dimension_id` int,
  `date_dimension_id` int,
  `event_dimension_id` int,
  `times` int,
  `created` String
  );


--相关维度类的编写和部署
将udf的jar放到hdfs中的某一个目录即可/直接放到本地也可以。
如果jar包放在本地，则需要将的jar进行添加。
如果jar包放在hdfs，则不需要，直接创建udf函数即可。
create function date_convert as 'com.qianfeng.anlystic.hive.DateDimensionUdf' using jar 'hdfs://hadoop01:9000/logDataAnalystic/udflib/LogDataAnalstic-1.0.jar';
create function platform_convert as 'com.qianfeng.anlystic.hive.PlatformDimensionUdf' using jar 'hdfs://hadoop01:9000/logDataAnalystic/udflib/LogDataAnalstic-1.0.jar';
create function event_convert as 'com.qianfeng.anlystic.hive.EventDimensionUdf' using jar 'hdfs://hadoop01:9000/logDataAnalystic/udflib/LogDataAnalstic-1.0.jar';


--统计事件次数   and de.en = 'e_e'
with tmp as(
select
from_unixtime(cast(de.s_time/1000 as bigint),"yyyy-MM-dd") dt,
de.pl pl,
 de.ca ca,
 de.ac ac
 from dw_event de
 where de.month = 7 and day = 9
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


--hbase中和hive映射的表可以使用如下的过滤
select
from_unixtime(cast(de.s_time as bigint)/1000,"yyyy-MM-dd") dt,
de.pl pl,
de.ca ca,
de.ac ac
from dw_event de
where de.s_time >= unix_timestamp('2018-07-09','yyyy-MM-dd')*1000
and de.s_time < unix_timestamp('2018-07-10','yyyy-MM-dd')*1000
and de.pl is not null
;



结果1：
2018-07-09	website	null	null
2018-07-09	java_server	null	null

结果2：
website  2018-07-09		null	null 1
java_server 2018-07-09	null	null 1

all 2018-07-09	null	null 2

website 2018-07-09	nuu	all 1
java_server 2018-07-09	null	all 1

all 2018-07-09	null	all 2

结果2的合并：
java_server	2018-07-09	null	null	1
website	2018-07-09	null	null	1
java_server	2018-07-09	null	all	1
website	2018-07-09	null	all	1
all	2018-07-09	null	null	2
all	2018-07-09	null	all	2

from 结果2
insert overwrite table dm_event
select platform_convert(pl),date_convert(dt),event_convert(ca,ac),sum(ecount),dt
group by pl,dt,ca,ac

最终结果：

1	1	1	2   2018-07-09
1	1	2	2   2018-07-09
3	1	1	1   2018-07-09
3	1	2	1   2018-07-09
2	1	1	1   2018-07-09
2	1	2	1   2018-07-09

事件维度：
1 null	all
2 null	null

--使用sqoop语句将hive中的最终结果导出到mysql
sqoop export --connect jdbc:mysql://hadoop01:3306/result \
--username root --password root --table stats_event \
--export-dir '/hive/lda.db/dm_event/*' \
--input-fields-terminated-by "\\01" --update-mode allowinsert \
--update-key platform_dimension_id,date_dimension_id,event_dimension_id \
;





