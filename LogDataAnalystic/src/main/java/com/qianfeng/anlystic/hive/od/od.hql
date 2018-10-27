udf的测试：
create function payment_convert as 'com.qianfeng.anlystic.hive.paymentDimensionUdf' using jar 'hdfs://hadoop01:9000/logDataAnalystic/udflib/LogDataAnalstic-1.0.jar';
create function currency_convert as 'com.qianfeng.anlystic.hive.currencyDimensionUdf' using jar 'hdfs://hadoop01:9000/logDataAnalystic/udflib/LogDataAnalstic-1.0.jar';
create function platform_convert as 'com.qianfeng.anlystic.hive.PlatformDimensionUdf' using jar 'hdfs://hadoop01:9000/logDataAnalystic/udflib/LogDataAnalstic-1.0.jar';




--在原始表中抽取数据，放在dw层
create external table if not exists dw_order(
 s_time bigint,
 en string,
 pl string,
 o_id String,
 cut String,
 cua double,
 pt String
 )
 partitioned by(month String,day string)
 row format delimited fields terminated by '\001'
 ;


from ods_logs
insert overwrite table dw_order partition(month=7,day=10)
select s_time,en,pl,o_id,cut,cua,pt
where month = 7 and day = 9;

from ods_logs
insert overwrite table dw_order partition(month=7,day=10)
select s_time,en,pl,o_id,cut,cua,pt
where month = 7 and day = 10;


select s_time,en,pl,o_id,cut,cua,pt from ods_logs
where month = 7 and day = 10;


创建中间临时表：
create table if not exists dw_view_tmp(
pl string,
dt string,
col string,
ct int
)
;


创建最终的结果表：
CREATE TABLE dm_order(
platform_dimension_id int,
`date_dimension_id` int,
`currency_type_dimension_id` int,
`payment_type_dimension_id` int,
`orders` int,
`success_orders` int,
`refund_orders` int,
`order_amount` int,
`revenue_amount` int,
`refund_amount` int,
`total_revenue_amount` int,
`total_refund_amount` int,
`created` String
);


CREATE TABLE dm_order(
platform_dimension_id int,
`date_dimension_id` int,
`currency_type_dimension_id` int,
`payment_type_dimension_id` int,
`ct` int,
`created` String
);


#订单总的个数

select
from_unixtime(cast(do.s_time/1000 as bigint),"yyyy-MM-dd") dt,
do.pl,
do.cut,
do.pt,
count(distinct do.oid) as ct
from dw_order do
where do.month = 7 and day = 10
and do.pl is not null
and do.en = 'e_crt'
;


from(
select
from_unixtime(cast(do.s_time/1000 as bigint),"yyyy-MM-dd") dt,
do.pl,
do.cut,
do.pt,
count(distinct do.o_id) as ct
from dw_order do
where do.month = 7 and day = 10
and do.pl is not null
and do.en = 'e_crt'
group by pl,from_unixtime(cast(do.s_time/1000 as bigint),"yyyy-MM-dd"),cut,pt
) as tmp
insert overwrite table dm_order
select platform_convert(pl),date_convert(dt),currency_convert(cut),payment_convert(pt),sum(ct),'2018-07-10'
group by platform_convert(pl),date_convert(dt),currency_convert(cut),payment_convert(pt)
;


--使用sqoop语句将hive中的最终结果导出到mysql
sqoop export --connect jdbc:mysql://hadoop01:3306/result \
--username root --password root --table stats_order \
--export-dir '/hive/lda.db/dm_order/*' \
--input-fields-terminated-by "\\01" --update-mode allowinsert \
--update-key platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id \
--columns 'platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id,orders,created'
;






aaaaaa
cccccc