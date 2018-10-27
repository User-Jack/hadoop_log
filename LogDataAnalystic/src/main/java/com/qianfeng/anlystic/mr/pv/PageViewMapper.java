package com.qianfeng.anlystic.mr.pv;

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
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * pv的mapper类。统计的pv事件中的url的不去重的个数。
 */
public class PageViewMapper extends TableMapper<StatsUserDimension,Text>{

    private static final Logger logger = Logger.getLogger(PageViewMapper.class);
    private StatsUserDimension k = new StatsUserDimension();
    private Text v = new Text();
    private byte[] family = Bytes.toBytes(EventLogConstants.EVENT_LOG_FAMILY_NAME);
    private KpiDimension pageviewKpi = new KpiDimension(KpiType.PAGE_VIEW.kpiName);


    @Override
    protected void map(ImmutableBytesWritable key, Result value,
                       Context context) throws IOException, InterruptedException {
        //要从hbase表中获取数据
        String url = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_CURRENT_URL)));
        String serverTime = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME)));
        String platformName = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_PLATFORM)));
        String browserName = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME)));
        String browserVersion = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION)));

        //判断，三者均不能等于空
        if(StringUtils.isEmpty(url) || StringUtils.isEmpty(serverTime)
                || StringUtils.isEmpty(platformName)){
            logger.warn("url&serverTime&platformName must not null.url:"+url
                    +"  serverTime:"+serverTime+"  platformName:"+platformName);
            return;
        }

        logger.info("输入的数据为:"+value.toString());

        //正常处理
        long timeOfLong = Long.valueOf(serverTime);
        //构造输出的value
        this.v.set(url);

        //构建输出的key
        DateDimension dateDimension = DateDimension.buildDate(timeOfLong, DateEnum.DAY);
        List<PlatformDimension> platformDimensions = PlatformDimension.buildList(platformName);
        List<BrowserDimension> browserDimensions = BrowserDimension.buildList(browserName,browserVersion);

        //获取statsCommonDimension
        StatsCommonDimension statsCommonDimension = this.k.getStatsCommonDimension();
        //设置值
        statsCommonDimension.setDateDimension(dateDimension);

        //循环platformDimensions
        for (PlatformDimension pl : platformDimensions){
            //设置kpi和pl维度
            statsCommonDimension.setKpiDimension(pageviewKpi);
            statsCommonDimension.setPlatformDimension(pl);
            this.k.setStatsCommonDimension(statsCommonDimension);

            //输出用于统计浏览器模块的
            for (BrowserDimension br : browserDimensions){
                this.k.setBrowserDimension(br);
                //输出
                context.write(this.k,this.v);
            }
        }
    }
}
