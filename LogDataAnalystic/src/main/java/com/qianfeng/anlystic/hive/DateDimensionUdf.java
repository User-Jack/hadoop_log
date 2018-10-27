package com.qianfeng.anlystic.hive;

import com.qianfeng.anlystic.modle.dim.base.DateDimension;
import com.qianfeng.anlystic.service.IDimensionConvert;
import com.qianfeng.anlystic.service.impl.IDimensionConvertImpl;
import com.qianfeng.common.DateEnum;
import com.qianfeng.util.TimeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @Auther: lyd
 * @Date: 2018/7/12 14:48
 * @Description:获取时间维度的id的udf
 */
public class DateDimensionUdf extends UDF{
    IDimensionConvert convert = new IDimensionConvertImpl();

    /**
     *
     * @return
     */
    public IntWritable evaluate(Text day){
        if(StringUtils.isEmpty(day.toString())){
            day = new Text(TimeUtil.getYesterday());
        }
        //构建时间维度
        DateDimension dateDimension = DateDimension.buildDate(
                TimeUtil.parseString2Long(day.toString()), DateEnum.DAY);

        try {
            return new IntWritable(convert.getDimensionIdByValue(dateDimension));
        } catch (IOException e) {
            throw new RuntimeException("获取时间维度的udf异常.");
        }
    }

    public static void main(String[] args) {
        System.out.println(new DateDimensionUdf().evaluate(new Text("2018-07-10")));
    }

}
