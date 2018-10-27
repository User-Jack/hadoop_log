package com.qianfeng.anlystic.mr.session;

import com.qianfeng.anlystic.modle.dim.StatsUserDimension;
import com.qianfeng.anlystic.modle.dim.base.BaseDimension;
import com.qianfeng.anlystic.modle.dim.value.BaseStatsValueWritable;
import com.qianfeng.anlystic.modle.dim.value.MapWritableValue;
import com.qianfeng.anlystic.mr.IOutputWritter;
import com.qianfeng.anlystic.service.IDimensionConvert;
import com.qianfeng.common.GlobalConstants;
import com.qianfeng.common.KpiType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Auther: lyd
 * @Date: 2018/7/11 11:35
 * @Description: session的个数
 */
public class SessionWritter implements IOutputWritter {

    @Override
    public void write(Configuration conf, BaseDimension key, BaseStatsValueWritable value,
                      PreparedStatement ps, IDimensionConvert convert) throws SQLException, IOException {
        StatsUserDimension statsUserDimension = (StatsUserDimension) key;
        MapWritableValue mapWritableValue = (MapWritableValue) value;
        int sessions = ((IntWritable) mapWritableValue.getValue().get(new IntWritable(-1))).get();
        int sessionLength = ((IntWritable) mapWritableValue.getValue().get(new IntWritable(-2))).get();

        //为ps设置值
        int i = 0;
        //根据value中的kpi来进行对应的赋值
        switch (mapWritableValue.getKpi()){
            case SESSION:
            case BROWSER_SESSION:
                ps.setInt(++i,convert.getDimensionIdByValue(statsUserDimension.getStatsCommonDimension().getDateDimension()));
                ps.setInt(++i,convert.getDimensionIdByValue(statsUserDimension.getStatsCommonDimension().getPlatformDimension()));
                if(mapWritableValue.getKpi().kpiName.equals(KpiType.BROWSER_SESSION.kpiName)){
                    ps.setInt(++i,convert.getDimensionIdByValue(statsUserDimension.getBrowserDimension()));
                }
                ps.setInt(++i,sessions);
                ps.setInt(++i,sessionLength);
                ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE));
                ps.setInt(++i,sessions);
                ps.setInt(++i,sessionLength);
                break;

            default:
                throw  new RuntimeException("该kpi暂时不支持赋值.kpi:"+mapWritableValue.getKpi().kpiName);
        }
        //将ps添加到batch
        ps.addBatch();
    }
}