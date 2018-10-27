package com.qianfeng.anlystic.hive;

import com.qianfeng.anlystic.modle.dim.base.DateDimension;
import com.qianfeng.anlystic.modle.dim.base.EventDimension;
import com.qianfeng.anlystic.service.IDimensionConvert;
import com.qianfeng.anlystic.service.impl.IDimensionConvertImpl;
import com.qianfeng.common.DateEnum;
import com.qianfeng.common.GlobalConstants;
import com.qianfeng.util.TimeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @Auther: lyd
 * @Date: 2018/7/12 14:48
 * @Description:获取事件维度的id的udf
 */
public class EventDimensionUdf extends UDF{
    IDimensionConvert convert = new IDimensionConvertImpl();
    /**
     *
     * @return
     */
    public int evaluate(String category,String action){
        if(StringUtils.isEmpty(category)){
            category = action = GlobalConstants.DEFAULT_VALUE;
        }
        if(StringUtils.isEmpty(action)){
            action = GlobalConstants.DEFAULT_VALUE;
        }
        //构建
        EventDimension eventDimension = new EventDimension(category,action);
        try {
            return convert.getDimensionIdByValue(eventDimension);
        } catch (IOException e) {
            throw new RuntimeException("获取事件维度的udf异常.");
        }
    }

    public static void main(String[] args) {
        System.out.println(new EventDimensionUdf().evaluate("aaa","cc"));
    }

}
