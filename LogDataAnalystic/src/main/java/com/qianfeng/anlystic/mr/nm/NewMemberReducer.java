package com.qianfeng.anlystic.mr.nm;

import com.qianfeng.anlystic.modle.dim.StatsUserDimension;
import com.qianfeng.anlystic.modle.dim.value.MapWritableValue;
import com.qianfeng.anlystic.modle.dim.value.TimeOutputValue;
import com.qianfeng.common.KpiType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * 新增会员的reduce
 */
public class NewMemberReducer extends Reducer<StatsUserDimension,TimeOutputValue,
        StatsUserDimension,MapWritableValue> {

    private MapWritableValue v = new MapWritableValue();
    private Set<String> uniquque = new HashSet<String>();

    @Override
    protected void reduce(StatsUserDimension key,
                          Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        //先清空set
        this.uniquque.clear();
        //构造reduce、阶段输出的value
        MapWritable map = new MapWritable();
        //循环values
        for (TimeOutputValue tv:values){
            //将TV中的id添加到set中
            this.uniquque.add(tv.getId());
        }

        //需要将排重过后的memebrId进行存储memberInfo表
       for(String memberId:this.uniquque){
           //将每一个memberid添加到memberInfo表中
           this.v.setKpi(KpiType.MEMBER_INFO);
           map.put(new IntWritable(-1),new Text(memberId));
           this.v.setValue(map);
           //输出
           context.write(key,this.v);
       }


        map.put(new IntWritable(-1),new IntWritable(this.uniquque.size()));
        this.v.setValue(map);

        //为v设置kpi
        String kpiName = key.getStatsCommonDimension().getKpiDimension().getKpiName();
        this.v.setKpi(KpiType.valueOfName(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
        //输出
        context.write(key,this.v);
    }
}
