package com.qianfeng.anlystic.mr.local;

import com.qianfeng.anlystic.modle.dim.StatsLocationDimension;
import com.qianfeng.anlystic.modle.dim.base.BaseDimension;
import com.qianfeng.anlystic.modle.dim.value.BaseStatsValueWritable;
import com.qianfeng.anlystic.modle.dim.value.LocationReduceOutputWritable;
import com.qianfeng.anlystic.mr.IOutputWritter;
import com.qianfeng.anlystic.service.IDimensionConvert;
import com.qianfeng.common.GlobalConstants;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Auther: lyd
 * @Date: 2018/7/12 14:27
 * @Description: 地域为的统计
 */
public class LocationWritter  implements IOutputWritter {

    @Override
    public void write(Configuration conf, BaseDimension key, BaseStatsValueWritable value,
                      PreparedStatement ps, IDimensionConvert convert) throws SQLException, IOException {
        StatsLocationDimension statsUserDimension = (StatsLocationDimension) key;
        LocationReduceOutputWritable output = (LocationReduceOutputWritable) value;

        //为ps设置值
        int i = 0;
        ps.setInt(++i,convert.getDimensionIdByValue(statsUserDimension.getStatsCommonDimension().getDateDimension()));
        ps.setInt(++i,convert.getDimensionIdByValue(statsUserDimension.getStatsCommonDimension().getPlatformDimension()));
        ps.setInt(++i,convert.getDimensionIdByValue(statsUserDimension.getLocationDimension()));
        ps.setInt(++i,output.getActiveUsers());
        ps.setInt(++i,output.getSessions());
        ps.setInt(++i,output.getBounceSessions());
        ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE));
        ps.setInt(++i,output.getActiveUsers());
        ps.setInt(++i,output.getSessions());
        ps.setInt(++i,output.getBounceSessions());
        //将ps添加到batch
        ps.addBatch();
    }
}
