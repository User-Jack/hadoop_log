package com.qianfeng.anlystic.mr.local;

import com.google.common.collect.Lists;
import com.qianfeng.anlystic.modle.dim.StatsLocationDimension;
import com.qianfeng.anlystic.modle.dim.StatsUserDimension;
import com.qianfeng.anlystic.modle.dim.value.LocationReduceOutputWritable;
import com.qianfeng.anlystic.modle.dim.value.MapWritableValue;
import com.qianfeng.anlystic.modle.dim.value.TextOutputValue;
import com.qianfeng.anlystic.modle.dim.value.TimeOutputValue;
import com.qianfeng.anlystic.mr.IOutputFormat;
import com.qianfeng.anlystic.mr.au.ActiveUserMapper;
import com.qianfeng.anlystic.mr.au.ActiveUserReducer;
import com.qianfeng.common.EventLogConstants;
import com.qianfeng.common.GlobalConstants;
import com.qianfeng.util.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.util.List;


/**
 * 地域的runner
 */

public class LocationRunner implements Tool{
    private static final Logger logger = Logger.getLogger(LocationRunner.class);
    private static Configuration conf = new Configuration();

    public static void main(String[] args) {
        try {
            ToolRunner.run(conf,new LocationRunner(),args);
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

        Job job = Job.getInstance(conf,"local");
        job.setJarByClass(LocationRunner.class);

        //初始化Tablemapper  addDependencyJars：false  本地提交本地运行
        List<Scan> scans = this.getScans(job);
        TableMapReduceUtil.initTableMapperJob(scans,LocationMapper.class,
                StatsLocationDimension.class,TextOutputValue.class,job,
                true);

        //本地提交集群运行
        /*TableMapReduceUtil.initTableMapperJob(scans,ActiveUserMapper.class,
                StatsUserDimension.class,TimeOutputValue.class,job,
                true);*/

        //设置reduer
        job.setReducerClass(LocationReducer.class);
        job.setOutputKeyClass(StatsLocationDimension.class);
        job.setOutputValueClass(LocationReduceOutputWritable.class);

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

        //定义获取的列
        String [] columns = new String[]{
                EventLogConstants.LOG_COLUMN_NAME_UUID,
                EventLogConstants.LOG_COLUMN_NAME_SESSION_ID,
                EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME,
                EventLogConstants.LOG_COLUMN_NAME_PLATFORM,
                EventLogConstants.LOG_COLUMN_NAME_COUNTRY,
                EventLogConstants.LOG_COLUMN_NAME_PROVINCE,
                EventLogConstants.LOG_COLUMN_NAME_CITY
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
