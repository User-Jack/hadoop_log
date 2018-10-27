package com.qianfeng.anlystic.mr;


import com.qianfeng.anlystic.modle.dim.base.BaseDimension;
import com.qianfeng.anlystic.modle.dim.value.BaseStatsValueWritable;
import com.qianfeng.anlystic.service.IDimensionConvert;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 操作最终结果表的接口
 */
public interface IOutputWritter {
    /**
     * 将最终结果存储到mysql中
     * @param conf  用于传递kpi
     * @param key   存储维度
     * @param value 存储统计值
     * @param ps    对应kpi的sql的ps
     * @param convert 获取对应的维度的id值
     * @throws SQLException
     * @throws IOException
     */
    void write(Configuration conf, BaseDimension key, BaseStatsValueWritable value,
               PreparedStatement ps, IDimensionConvert convert) throws SQLException,IOException;
}
