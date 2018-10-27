package com.qianfeng.anlystic.mr.nu;

import com.qianfeng.anlystic.modle.dim.StatsUserDimension;
import com.qianfeng.anlystic.modle.dim.value.MapWritableValue;
import com.qianfeng.anlystic.modle.dim.value.TimeOutputValue;
import com.qianfeng.common.KpiType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * 新增用户的reduce
 */
public class NewUserReducer extends Reducer<StatsUserDimension,TimeOutputValue,
        StatsUserDimension,MapWritableValue> {

    private MapWritableValue v = new MapWritableValue();
    private Set<String> uniquque = new HashSet<String>();



    @Override
    protected void reduce(StatsUserDimension key,
                          Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        //先清空set
        this.uniquque.clear();
        //循环values
        for (TimeOutputValue tv:values){
            //将TV中的id添加到set中
            this.uniquque.add(tv.getId());
        }

        //构造reduce、阶段输出的value
        MapWritable map = new MapWritable();
        map.put(new IntWritable(-1),new IntWritable(this.uniquque.size()));
        this.v.setValue(map);

        //为v设置kpi
        String kpiName = key.getStatsCommonDimension().getKpiDimension().getKpiName();
        /*if(kpiName.equals(KpiType.NEW_INSTALL_USER.kpiName)){
            this.v.setKpi(KpiType.NEW_INSTALL_USER); //KpiType.valueOf(kpiName)
        } else if(kpiName.equals(KpiType.BROWSER_NEW_INSTALL_USER.kpiName)){
            this.v.setKpi(KpiType.BROWSER_NEW_INSTALL_USER); //KpiType.valueOf(kpiName)
        }*/
        this.v.setKpi(KpiType.valueOfName(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
        //输出
        context.write(key,this.v);

    }
}
