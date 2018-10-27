package com.qianfeng.anlystic.mr.session;

import com.qianfeng.anlystic.modle.dim.StatsUserDimension;
import com.qianfeng.anlystic.modle.dim.value.MapWritableValue;
import com.qianfeng.anlystic.modle.dim.value.TimeOutputValue;
import com.qianfeng.common.KpiType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * session的个数和长度的reduce
 */
public class SessionReducer extends Reducer<StatsUserDimension,TimeOutputValue,
        StatsUserDimension,MapWritableValue> {

    private MapWritableValue v = new MapWritableValue();

    @Override
    protected void reduce(StatsUserDimension key,
                          Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        //先清空set
//        this.uniquque.clear();
        //构造reduce、阶段输出的value
        MapWritable map = new MapWritable();
        Map<String,List<Long>> ml = new HashMap<String,List<Long>>();
        //循环values
        for (TimeOutputValue tv:values){
            //将TV中的id添加到set中
//            this.uniquque.add(tv.getId());

            //将时间戳存储到list中
            if(ml.containsKey(tv.getId())){
                //将时间戳添加到集合中
                ml.get(tv.getId()).add(tv.getTime());
                ml.put(tv.getId(),ml.get(tv.getId()));
            } else {
                List<Long> l = new ArrayList<Long>();
                l.add(tv.getTime());
                ml.put(tv.getId(),l);
            }
        }

        //计算时长
        int sessionLength = 0;
        for (Map.Entry<String,List<Long>> en:ml.entrySet()){
            //判断list是否大于2
            if(en.getValue().size() >= 2){
                List<Long> ll = en.getValue();
                Collections.sort(ll);
                sessionLength += (ll.get(ll.size()-1) - ll.get(0));
            }
        }
        //不足一秒按照一秒计算
        if(sessionLength % 1000 == 0){
            sessionLength = sessionLength / 1000;
        } else {
            sessionLength = sessionLength / 1000 + 1;
        }

        map.put(new IntWritable(-1),new IntWritable(ml.size()));
        map.put(new IntWritable(-2),new IntWritable(sessionLength));

        this.v.setValue(map);

        //为v设置kpi
        this.v.setKpi(KpiType.valueOfName(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
        //输出
        context.write(key,this.v);
    }
}
