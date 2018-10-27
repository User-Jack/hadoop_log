package com.qianfeng.etl.tohbase;

import com.qianfeng.common.EventLogConstants;
import com.qianfeng.etl.util.LogUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * 清洗数据存储到hbase中
 */
public class ParseLogMapper extends Mapper<Object,Text,NullWritable,Put>{

    private static final Logger logger = Logger.getLogger(ParseLogMapper.class);
    //定义输入、输出、过滤的记录数
    private static int inputRecords,outputRecords,filterRecords = 0;
    private static final byte[] columnFamily = Bytes.toBytes(EventLogConstants.EVENT_LOG_FAMILY_NAME);
    //获取crc32的对象
    private CRC32 crc = new CRC32();

    @Override
    protected void map(Object key, Text value, Context context){
        try{
            this.inputRecords ++;
            logger.info("解析日志为:"+value.toString());
            Map<String,String> info = LogUtil.handleLog(value.toString());
            //判断是否info为空
            if(info.isEmpty()){
                this.filterRecords ++;
                return;
            }
            //获取事件   可以根据事件名来分别存储数据
            String eventName = info.get(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME);
            //获取事件的枚举
            EventLogConstants.EventEnum event = EventLogConstants.EventEnum.valueOfAlias(eventName);
            switch (event){
                case LANUCH:
                case PAGEVIEW:
                case CHARGEREFUND:
                case EVENT:
                case CHARGEREQUEST:
                case CHARGESUCCESS:
                    //处理存储
                    this.handleLog(info,context,eventName);
                    break;
                default:
                    filterRecords ++;
                    logger.warn("该时间的数据暂时不支持处理,事件为:"+eventName);
            }
        } catch (Exception e){
            this.filterRecords ++;
            logger.warn("数据解析异常",e);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        logger.info("inputRecords:"+inputRecords+"  outputRecords:"+outputRecords+"  filterRecords:"+filterRecords);
    }

    /**
     * 处理存储，将数据存储到hbase中。row-key设计：
     * @param info
     * @param context
     */
    private void handleLog(Map<String, String> info, Context context,String eventName) throws IOException,InterruptedException{
        String server_time = info.get(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME);
        String uuid = info.get(EventLogConstants.LOG_COLUMN_NAME_UUID);
        String memberId = info.get(EventLogConstants.LOG_COLUMN_NAME_MEMBER_ID);
        //判断server_time就能是否为空
        if(StringUtils.isNotEmpty(server_time)){
            //获取rowkey
            String rowkey = buildRowKey(server_time,uuid,memberId,eventName);
            //获取hbas的put对象
            Put put = new Put(Bytes.toBytes(rowkey));
            for (Map.Entry<String,String> en : info.entrySet()){
                if(StringUtils.isNotEmpty(en.getValue())){
                    put.add(columnFamily,Bytes.toBytes(en.getKey()),
                            Bytes.toBytes(en.getValue()));
                }
            }
            //将put存储
            context.write(NullWritable.get(),put);
            this.outputRecords ++;
        } else {
            this.filterRecords ++;
        }
    }

    /**
     * row-key的设计:server_time_crc32(uuid_memberId_eventName)
     * @param server_time
     * @param uuid
     * @param memberId
     * @param eventName
     * @return  1523000001  1523000002  a1523000001  b1523000002
     *
     * 问题：
     * 时间戳或者序列数据放再rowkey前面将会造成region的积压问题。
     *
     * region的进行预创建：
     * 将时间戳或者序列数据存储到值，然后进行二级索引查找。
     */
    private String buildRowKey(String server_time, String uuid, String memberId, String eventName) {
        StringBuilder sb = new StringBuilder();
        sb.append(server_time+"_");
        //重置crc32的值
        crc.reset();
        if(StringUtils.isNotEmpty(uuid)){
            crc.update(uuid.getBytes());
        }
        if(StringUtils.isNotEmpty(memberId)){
            crc.update(memberId.getBytes());
        }
        if(StringUtils.isNotEmpty(eventName)){
            crc.update(eventName.getBytes());
        }
        //将crc32的值来模于1亿  模于数越大，长度会越小
        sb.append(this.crc.getValue()%100000000l);
        return sb.toString();
    }
}
