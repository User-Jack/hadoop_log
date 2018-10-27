package com.qianfeng.anlystic.mr.pv;

import com.google.common.collect.Lists;
import com.qianfeng.anlystic.modle.dim.StatsUserDimension;
import com.qianfeng.anlystic.modle.dim.base.DateDimension;
import com.qianfeng.anlystic.modle.dim.value.MapWritableValue;
import com.qianfeng.anlystic.modle.dim.value.TimeOutputValue;
import com.qianfeng.anlystic.mr.IOutputFormat;
import com.qianfeng.anlystic.service.IDimensionConvert;
import com.qianfeng.anlystic.service.impl.IDimensionConvertImpl;
import com.qianfeng.common.DateEnum;
import com.qianfeng.common.EventLogConstants;
import com.qianfeng.common.GlobalConstants;
import com.qianfeng.util.JdbcUtil;
import com.qianfeng.util.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 pv的runner
 */

public class PageViewRunner implements Tool{
    private static final Logger logger = Logger.getLogger(PageViewRunner.class);
    private static Configuration conf = new Configuration();

    public static void main(String[] args) {
        try {
            ToolRunner.run(conf,new PageViewRunner(),args);
        } catch (Exception e) {
            logger.warn("执行新增用户的main方法失败.",e);
        }
    }


    @Override
    public void setConf(Configuration conf) {
        this.conf.addResource("output-mapping.xml");
        this.conf.addResource("output-writter.xml");
        this.conf = HBaseConfiguration.create(conf);  //带着conf
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }


    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        this.setArgs(conf,args);

        Job job = Job.getInstance(conf,"pv");
        job.setJarByClass(PageViewRunner.class);

        //初始化Tablemapper  addDependencyJars：false  本地提交本地运行
        List<Scan> scans = this.getScans(job);
        TableMapReduceUtil.initTableMapperJob(scans,PageViewMapper.class,
                StatsUserDimension.class,Text.class,job,
                true);

        //本地提交集群运行
        /*TableMapReduceUtil.initTableMapperJob(scans,PageViewMapper.class,
                StatsUserDimension.class,TimeOutputValue.class,job,
                true);*/

        //设置reduer
        job.setReducerClass(PageViewReducer.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(MapWritableValue.class);

        //设置输出类
        job.setOutputFormatClass(IOutputFormat.class);


        return job.waitForCompletion(true)?0:1;
    }

    /**
     *
     * @param conf
     * @param args
     */
    private void setArgs(Configuration conf,String[] args){
        String date = null;
        //循环参数列表
        for (int i=0;i < args.length;i++){
            if("-d".equals(args[i])){
                if(i+1 < args.length){
                    date = args[i+1];
                    break;
                }
            }
        }
        //如果date为空或者无效，则默认使用昨天的date。将date设置到conf中
        if(date == null || !TimeUtil.isRunningValidate(date)){
            date = TimeUtil.getYesterday();
        }
        //将date添加到conf中
        conf.set(GlobalConstants.RUNNING_DATE,date);
    }

    /**
     * 获取扫描hbase的扫描对象   yarn jar  .... -d 2018-07-09
     * @param job
     * @return
     */
    private List<Scan> getScans(Job job) {
        //获取时间
        String date = job.getConfiguration().get(GlobalConstants.RUNNING_DATE);
        //获取起始时间
        long startDate = TimeUtil.parseString2Long(date);
        long endDate = startDate + GlobalConstants.DAY_OF_MILLINSECONDS;

        //获取hbase的扫描对象
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startDate+""));
        scan.setStopRow(Bytes.toBytes(endDate+""));

        //定义hbase的过滤器
        FilterList fl = new FilterList();
        fl.addFilter(new SingleColumnValueFilter(Bytes.toBytes(EventLogConstants.EVENT_LOG_FAMILY_NAME)
        ,Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME), CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes(EventLogConstants.EventEnum.PAGEVIEW.alias)));

        //定义获取的列
        String [] columns = new String[]{
                EventLogConstants.LOG_COLUMN_NAME_CURRENT_URL,
                EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME,
                EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME,
                EventLogConstants.LOG_COLUMN_NAME_PLATFORM,
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME,
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION
        };

        //将扫描的列添加到过滤器中
        fl.addFilter(this.getColumnsFilter(columns));
        //设置scan扫描的表
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME,
                Bytes.toBytes(EventLogConstants.EVENT_LOG_HBASE_NAME));
        //将过滤器链添加到scan中
        scan.setFilter(fl);
        return Lists.newArrayList(scan); //用的Lists
    }

    /**
     * 返回列过滤器
     * @param columns
     * @return
     */
    private Filter getColumnsFilter(String[] columns) {
        int length = columns.length;
        byte[][] filters = new byte[length][];
        for (int i=0;i< length;i++){
            filters[i] = Bytes.toBytes(columns[i]);
        }
        return new MultipleColumnPrefixFilter(filters);
    }
}
