package com.qianfeng.anlystic.mr.pv;

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
 * pv的赋值类
 */
public class PageViewWritter implements IOutputWritter{

    @Override
    public void write(Configuration conf, BaseDimension key, BaseStatsValueWritable value,
                      PreparedStatement ps, IDimensionConvert convert) throws SQLException, IOException {
        StatsUserDimension statsUserDimension = (StatsUserDimension) key;
        MapWritableValue mapWritableValue = (MapWritableValue) value;
        int pvs = ((IntWritable) mapWritableValue.getValue().get(new IntWritable(-1))).get();

        //为ps设置值
        int i = 0;
        ps.setInt(++i,convert.getDimensionIdByValue(statsUserDimension.getStatsCommonDimension().getDateDimension()));
        ps.setInt(++i,convert.getDimensionIdByValue(statsUserDimension.getStatsCommonDimension().getPlatformDimension()));
        ps.setInt(++i,convert.getDimensionIdByValue(statsUserDimension.getBrowserDimension()));
        ps.setInt(++i,pvs);
        ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE));
        ps.setInt(++i,pvs);
        //将ps添加到batch
        ps.addBatch();
    }
}
