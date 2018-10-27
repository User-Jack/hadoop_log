package com.qianfeng.anlystic.mr.am;

import com.qianfeng.anlystic.modle.dim.StatsCommonDimension;
import com.qianfeng.anlystic.modle.dim.StatsUserDimension;
import com.qianfeng.anlystic.modle.dim.base.BrowserDimension;
import com.qianfeng.anlystic.modle.dim.base.DateDimension;
import com.qianfeng.anlystic.modle.dim.base.KpiDimension;
import com.qianfeng.anlystic.modle.dim.base.PlatformDimension;
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
 * 统计活跃的会员的mapper类。
 */
public class ActiveMemberMapper extends TableMapper<StatsUserDimension,TimeOutputValue>{

    private static final Logger logger = Logger.getLogger(ActiveMemberMapper.class);
    private StatsUserDimension k = new StatsUserDimension();
    private TimeOutputValue v = new TimeOutputValue();
    private byte[] family = Bytes.toBytes(EventLogConstants.EVENT_LOG_FAMILY_NAME);
    private KpiDimension activeMemberKpi = new KpiDimension(KpiType.ACTIVE_MEMBER.kpiName);
    private KpiDimension browserActiveMemberKpi = new KpiDimension(KpiType.BROWSER_ACTIVE_MEMBER.kpiName);


    @Override
    protected void map(ImmutableBytesWritable key, Result value,
                       Context context) throws IOException, InterruptedException {
        //要从hbase表中获取数据
        String memberId = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_MEMBER_ID)));
        String serverTime = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME)));
        String platformName = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_PLATFORM)));
        String browserName = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME)));
        String browserVersion = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION)));

        //判断，三者均不能等于空
        if(StringUtils.isEmpty(memberId) || StringUtils.isEmpty(serverTime)
                || StringUtils.isEmpty(platformName)){
            logger.warn("uuid&serverTime&platformName must not null.memberId:"+memberId
                    +"  serverTime:"+serverTime+"  platformName:"+platformName);
            return;
        }

        logger.info("输入的数据为:"+value.toString());

        //正常处理
        long timeOfLong = Long.valueOf(serverTime);
        //构造输出的value
        this.v.setId(memberId);
        this.v.setTime(timeOfLong);

        //构建输出的key
        DateDimension dateDimension = DateDimension.buildDate(timeOfLong, DateEnum.DAY);
        List<PlatformDimension> platformDimensions = PlatformDimension.buildList(platformName);
        List<BrowserDimension> browserDimensions = BrowserDimension.buildList(browserName,browserVersion);
        BrowserDimension default_browserDimension = new BrowserDimension("","");

        //获取statsCommonDimension
        StatsCommonDimension statsCommonDimension = this.k.getStatsCommonDimension();
        //设置值
        statsCommonDimension.setDateDimension(dateDimension);

        //循环platformDimensions
        for (PlatformDimension pl : platformDimensions){
            //设置浏览器的默认的值
            this.k.setBrowserDimension(default_browserDimension);
            //设置kpi和pl维度
            statsCommonDimension.setKpiDimension(activeMemberKpi);
            statsCommonDimension.setPlatformDimension(pl);
            //输出
            context.write(this.k,this.v);
            //输出用于统计浏览器模块的
            for (BrowserDimension br : browserDimensions){
                //设置kpi和pl维度
                statsCommonDimension.setKpiDimension(browserActiveMemberKpi);
                this.k.setStatsCommonDimension(statsCommonDimension);
                this.k.setBrowserDimension(br);
                //输出
                context.write(this.k,this.v);
            }
        }
    }
}
