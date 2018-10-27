package com.qianfeng.anlystic.mr.pv;

import com.qianfeng.anlystic.modle.dim.StatsUserDimension;
import com.qianfeng.anlystic.modle.dim.value.MapWritableValue;
import com.qianfeng.anlystic.modle.dim.value.TimeOutputValue;
import com.qianfeng.common.KpiType;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * pv的reduce
 */
public class PageViewReducer extends Reducer<StatsUserDimension,Text,
        StatsUserDimension,MapWritableValue> {

    private MapWritableValue v = new MapWritableValue();


    @Override
    protected void reduce(StatsUserDimension key,
                          Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        //循环values
        for (Text t:values){
            if(StringUtils.isNotEmpty(t.toString())){
                count ++;
            }
        }

        //构造reduce、阶段输出的value
        MapWritable map = new MapWritable();
        map.put(new IntWritable(-1),new IntWritable(count));
        this.v.setValue(map);

        //为v设置kpi
        String kpiName = key.getStatsCommonDimension().getKpiDimension().getKpiName();
        this.v.setKpi(KpiType.valueOfName(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
        //输出
        context.write(key,this.v);
    }
}
