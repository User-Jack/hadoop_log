package com.qianfeng.anlystic.mr.nu;

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
 * 新增用户的runner
 truncate dimension_browser;
 truncate dimension_currency_type;
 truncate dimension_date;
 truncate dimension_event;
 truncate dimension_inbound;
 truncate dimension_kpi;
 truncate dimension_location;
 truncate dimension_os;
 truncate dimension_payment_type;
 truncate dimension_platform;
 truncate event_info;
 truncate order_info;
 truncate stats_device_browser;
 truncate stats_device_location;
 truncate stats_event;
 truncate stats_hourly;
 truncate stats_inbound;
 truncate stats_order;
 truncate stats_user;
 truncate stats_view_depth;
 */

public class NewUserRunner implements Tool{
    private static final Logger logger = Logger.getLogger(NewUserRunner.class);
    private static Configuration conf = new Configuration();

    public static void main(String[] args) {
        try {
            ToolRunner.run(conf,new NewUserRunner(),args);
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

        Job job = Job.getInstance(conf,"newUser");
        job.setJarByClass(NewUserRunner.class);

        //初始化Tablemapper  addDependencyJars：false  本地提交本地运行
        List<Scan> scans = this.getScans(job);
        TableMapReduceUtil.initTableMapperJob(scans,NewUserMapper.class,
                StatsUserDimension.class,TimeOutputValue.class,job,
                true);

        //本地提交集群运行
        /*TableMapReduceUtil.initTableMapperJob(scans,NewUserMapper.class,
                StatsUserDimension.class,TimeOutputValue.class,job,
                true);*/

        //设置reduer
        job.setReducerClass(NewUserReducer.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(MapWritableValue.class);

        //设置输出类
        job.setOutputFormatClass(IOutputFormat.class);


//        return job.waitForCompletion(true)?0:1;
        //计算新增总用户
        if(job.waitForCompletion(true)){
            this.caculateTotalUser(job);
            return 0;
        } else {
            return 1;
        }
    }

    /**
     * 计算新增总用户
     * @param job
     */
    private void caculateTotalUser(Job job) {

        Connection conn = null;
        ResultSet rs = null;
        PreparedStatement ps = null;
        try {
            //获取运行日期的当前日期
            Configuration conf = job.getConfiguration();
            long nowDate = TimeUtil.parseString2Long(conf.get(GlobalConstants.RUNNING_DATE));
            long yesterDate = nowDate - GlobalConstants.DAY_OF_MILLINSECONDS;

            //构建维度
            DateDimension nowDateDimension = DateDimension.buildDate(nowDate, DateEnum.DAY);
            DateDimension yesterDateDimension = DateDimension.buildDate(yesterDate, DateEnum.DAY);

            //根据维度获取对应的id
            int nowDateDimensionId = -1;
            int yesterDateDimensionId = -1;

            //1、查询维度Id
            IDimensionConvert convert = new IDimensionConvertImpl();
            nowDateDimensionId = convert.getDimensionIdByValue(nowDateDimension);
            yesterDateDimensionId = convert.getDimensionIdByValue(yesterDateDimension);

            //2、如果今天的维度Id大于1，则将今天(统计日期)的各个维度的新增用户数据查询出来
            /**
             * 1、查询昨天的各个平台维度的新增总用户，然后存储到map中
             * 2、查询今天的各个平台维度的新增用户，然后存和map中的昨天的key进行比较等累加操作
             */
            Map<String,Integer> map = new HashMap<String,Integer>();
            conn = JdbcUtil.getConnection();
            if(yesterDateDimensionId >= 1){
                ps = conn.prepareStatement("select `platform_dimension_id`,`total_install_users` from `stats_user` where `date_dimension_id` = ?");
                ps.setInt(1,yesterDateDimensionId);
                rs = ps.executeQuery();
                while (rs.next()){
                    int platformId = rs.getInt("platform_dimension_id");
                    int totalUsers = rs.getInt("total_install_users");
                    map.put(platformId+"",totalUsers);
                }
            }


            if(nowDateDimensionId >= 1){
                ps = conn.prepareStatement("select `platform_dimension_id`,`new_install_users` from `stats_user` where `date_dimension_id` = ?");
                ps.setInt(1,nowDateDimensionId);
                rs = ps.executeQuery();
                while (rs.next()){
                    int platformId = rs.getInt("platform_dimension_id");
                    int newUsers = rs.getInt("new_install_users");
                    if(map.containsKey(platformId+"")){
                       newUsers += map.get(platformId+"");
                    }
                    map.put(platformId+"",newUsers);
                }
            }

            //将统计的数据储存更新
            ps = conn.prepareStatement("insert into `stats_user` (`date_dimension_id`,`platform_dimension_id`,`total_install_users`) values(?,?,?) on duplicate key update `total_install_users` = ?");
            //赋值
            for(Map.Entry<String,Integer> en:map.entrySet()){
                ps.setInt(1,nowDateDimensionId);
                ps.setInt(2,Integer.parseInt(en.getKey()));
                ps.setInt(3,en.getValue());
                ps.setInt(4,en.getValue());
                ps.execute();
            }


        } catch (Exception e) {
            logger.warn("计算新增总用户失败.",e);
        } finally {
            JdbcUtil.close(conn,rs,ps);
        }
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
                Bytes.toBytes(EventLogConstants.EventEnum.LANUCH.alias)));

        //定义获取的列
        String [] columns = new String[]{
                EventLogConstants.LOG_COLUMN_NAME_UUID,
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
