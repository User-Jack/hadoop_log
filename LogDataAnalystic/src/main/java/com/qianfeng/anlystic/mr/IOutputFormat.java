package com.qianfeng.anlystic.mr;


import com.qianfeng.anlystic.modle.dim.base.BaseDimension;
import com.qianfeng.anlystic.modle.dim.value.BaseStatsValueWritable;
import com.qianfeng.anlystic.service.IDimensionConvert;
import com.qianfeng.anlystic.service.impl.IDimensionConvertImpl;
import com.qianfeng.common.GlobalConstants;
import com.qianfeng.common.KpiType;
import com.qianfeng.util.JdbcUtil;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * 自定义输出到mysql的格式
 */
public class IOutputFormat extends OutputFormat<BaseDimension,BaseStatsValueWritable>{
    private static final Logger logger = Logger.getLogger(IOutputFormat.class);


    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        Connection conn = JdbcUtil.getConnection();
        Configuration conf = context.getConfiguration();
        IDimensionConvert convert = new IDimensionConvertImpl();
        return new IOutputRecordWritter(conn,conf,convert);
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
        //do nothing
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new FileOutputCommitter(null,context); //FileOutputFormat.getOutputPath(context)
    }

    /**
     * 自定义内部类，用于封装输出的记录
     */
    public class IOutputRecordWritter extends RecordWriter<BaseDimension,BaseStatsValueWritable>{
        private Connection conn = null;
        private Configuration conf = null;
        private IDimensionConvert convert = null;
        //存储 kpi:kpi对应的ps
        private Map<KpiType,PreparedStatement> map = new HashMap<KpiType,PreparedStatement>();
        //存储 kpi:count对应的累计数
        private Map<KpiType,Integer> batch = new HashMap<KpiType,Integer>();

        public IOutputRecordWritter(Connection conn, Configuration conf, IDimensionConvert convert) {
            this.conn = conn;
            this.conf = conf;
            this.convert = convert;
        }

        /**
         * 将结果输出的核心方法
         * @param key
         * @param value
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void write(BaseDimension key, BaseStatsValueWritable value) throws IOException, InterruptedException {
            if(key == null || value == null){
                return;  //不用写出
            }

            PreparedStatement ps = null;
            try{
                //获取kpi   根据kpi来获取对应的sql
                KpiType kpi = value.getKpi();
                int count = 1;
                //从map中获取是否有对应的ps
                if(map.get(kpi) == null){
                    //从conf中获取对应的kpi，因为conf中会存储sql
                    ps = this.conn.prepareStatement(conf.get(kpi.kpiName));
                    map.put(kpi,ps);
                } else {
                    ps = map.get(kpi);
                    count = batch.get(kpi);
                    count ++;
                }
                //将count存储
                batch.put(kpi,count);

                //为ps赋值   wirtter_kpiName == 赋值类的类名
                String writterName = conf.get(GlobalConstants.WRITTER_PREFIX+kpi.kpiName);
                Class<?> classz = Class.forName(writterName);  //转换成类
                IOutputWritter iw = (IOutputWritter) classz.newInstance();   //获取classz对应的对象
                iw.write(conf,key,value,ps,convert);  //该方法得对应的类去实现

                //当达到一定batch就可以触发执行
                if(count % GlobalConstants.BATCH_VALUE == 0){
                    ps.executeBatch(); //批量执行
                    conn.commit(); //提交
                    batch.remove(kpi);
                }

            } catch (Throwable e){
                logger.warn("执行getRecordWritter方法失败。",e);
            }
        }

        /**
         * 关闭该关闭的对象
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            try {
                //循环将剩下的map中的ps进行执行
                for (Map.Entry<KpiType,PreparedStatement> en: map.entrySet()) {
                    en.getValue().executeBatch();
                }
            } catch (SQLException e) {
               logger.warn("批量执行剩余的ps异常.",e);
            } finally {
                if(conn != null){
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        //关闭连接抛异常
                    } finally {
                        //循环将剩下的map中的ps进行关闭
                        for (Map.Entry<KpiType,PreparedStatement> en: map.entrySet()) {
                            try {
                                en.getValue().close();
                            } catch (SQLException e) {
                                //循环关闭ps异常
                            }
                        }
                    }
                }
            }
        }
    }
}
