package com.qianfeng.anlystic.modle.dim.value;

import com.qianfeng.common.KpiType;
import org.apache.hadoop.io.Writable;

/**
 * 输出的valueleix的顶级父类
 */
public abstract class BaseStatsValueWritable implements Writable{
    public abstract KpiType getKpi();  //获取kpi的类型
}
