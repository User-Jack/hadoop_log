package com.qianfeng.anlystic.mr.au;

import com.qianfeng.anlystic.modle.dim.StatsUserDimension;
import com.qianfeng.anlystic.modle.dim.base.BaseDimension;
import com.qianfeng.anlystic.modle.dim.value.BaseStatsValueWritable;
import com.qianfeng.anlystic.modle.dim.value.MapWritableValue;
import com.qianfeng.anlystic.mr.IOutputWritter;
import com.qianfeng.anlystic.service.IDimensionConvert;
import com.qianfeng.common.GlobalConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 按小时活跃用户的赋值类
 */
public class HourlyActiveUserWritter implements IOutputWritter{

    @Override
    public void write(Configuration conf, BaseDimension key, BaseStatsValueWritable value,
                      PreparedStatement ps, IDimensionConvert convert) throws SQLException, IOException {
        StatsUserDimension statsUserDimension = (StatsUserDimension) key;
        MapWritableValue mapWritableValue = (MapWritableValue) value;

        //为ps设置值
        int i = 0;
        ps.setInt(++i,convert.getDimensionIdByValue(statsUserDimension.getStatsCommonDimension().getDateDimension()));
        ps.setInt(++i,convert.getDimensionIdByValue(statsUserDimension.getStatsCommonDimension().getPlatformDimension()));
        ps.setInt(++i,convert.getDimensionIdByValue(statsUserDimension.getStatsCommonDimension().getKpiDimension()));
        //为每一个小时赋值
        for(i++;i<28;i++){
            int newUser = ((IntWritable)((MapWritableValue) value).getValue().get(new IntWritable(i-4))).get();
            ps.setInt(i,newUser);
            //赋值为更新的最后的24个
            ps.setInt(i+25,newUser);
        }
        ps.setString(i,conf.get(GlobalConstants.RUNNING_DATE));
        //将ps添加到batch
        ps.addBatch();
    }
}
