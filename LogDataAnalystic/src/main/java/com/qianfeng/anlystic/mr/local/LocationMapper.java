package com.qianfeng.anlystic.mr.local;

import com.qianfeng.anlystic.modle.dim.StatsCommonDimension;
import com.qianfeng.anlystic.modle.dim.StatsLocationDimension;
import com.qianfeng.anlystic.modle.dim.StatsUserDimension;
import com.qianfeng.anlystic.modle.dim.base.*;
import com.qianfeng.anlystic.modle.dim.value.TextOutputValue;
import com.qianfeng.anlystic.modle.dim.value.TimeOutputValue;
import com.qianfeng.common.DateEnum;
import com.qianfeng.common.EventLogConstants;
import com.qianfeng.common.KpiType;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * 地域模块的的用户、session的mapper类。
 */
public class LocationMapper extends TableMapper<StatsLocationDimension,TextOutputValue>{

    private static final Logger logger = Logger.getLogger(LocationMapper.class);
    private StatsLocationDimension k = new StatsLocationDimension();
    private TextOutputValue v = new TextOutputValue();
    private byte[] family = Bytes.toBytes(EventLogConstants.EVENT_LOG_FAMILY_NAME);
    private KpiDimension locationKpi = new KpiDimension(KpiType.LOCATION.kpiName);

    @Override
    protected void map(ImmutableBytesWritable key, Result value,
                       Context context) throws IOException, InterruptedException {
        //要从hbase表中获取数据
        String uuid = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_UUID)));
        String sessionId = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SESSION_ID)));
        String serverTime = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME)));
        String platformName = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_PLATFORM)));
        String country = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_COUNTRY)));
        String provice = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_PROVINCE)));
        String city = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_CITY)));

        //判断，三者均不能等于空
        if(StringUtils.isEmpty(serverTime)
                || StringUtils.isEmpty(platformName)){
            logger.warn("uuid&serverTime&platformName must not null." +"  serverTime:"+serverTime+"  platformName:"+platformName);
            return;
        }

        if(StringUtils.isEmpty(uuid)){
            uuid = "";
        }

        if(StringUtils.isEmpty(sessionId)){
            sessionId = "";
        }
        logger.info("输入的数据为:"+value.toString());

        //正常处理
        long timeOfLong = Long.valueOf(serverTime);
        //构造输出的value
        this.v.setUuid(uuid);
        this.v.setSessionId(sessionId);

        //构建输出的key
        DateDimension dateDimension = DateDimension.buildDate(timeOfLong, DateEnum.DAY);
        List<PlatformDimension> platformDimensions = PlatformDimension.buildList(platformName);
        List<LocationDimension> locationDimensions = LocationDimension.buildList(country,provice,city);

        //获取statsCommonDimension
        StatsCommonDimension statsCommonDimension = this.k.getStatsCommonDimension();
        //设置值
        statsCommonDimension.setDateDimension(dateDimension);

        //循环platformDimensions
        for (PlatformDimension pl : platformDimensions){
            //设置kpi和pl维度
            statsCommonDimension.setKpiDimension(locationKpi);
            statsCommonDimension.setPlatformDimension(pl);
            this.k.setStatsCommonDimension(statsCommonDimension);
            //输出用于统计浏览器模块的
            for (LocationDimension local : locationDimensions){

                this.k.setLocationDimension(local);
                //输出
                context.write(this.k,this.v);
            }
        }
    }
}
