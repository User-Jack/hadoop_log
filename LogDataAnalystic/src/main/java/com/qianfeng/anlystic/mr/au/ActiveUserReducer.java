package com.qianfeng.anlystic.mr.au;

import com.qianfeng.anlystic.modle.dim.StatsUserDimension;
import com.qianfeng.anlystic.modle.dim.value.MapWritableValue;
import com.qianfeng.anlystic.modle.dim.value.TimeOutputValue;
import com.qianfeng.common.DateEnum;
import com.qianfeng.common.KpiType;
import com.qianfeng.util.TimeUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 新增用户的reduce
 */
public class ActiveUserReducer extends Reducer<StatsUserDimension,TimeOutputValue,
        StatsUserDimension,MapWritableValue> {
    //按天统计的
    private MapWritableValue v = new MapWritableValue();
    private Set<String> uniquque = new HashSet<String>();

    //按小时统计的属性  小时:该小时段内存储id的集合
    private Map<Integer,Set<String>> hourlyUnique = new HashMap<Integer,Set<String>>();
    private MapWritable houlyMap = new MapWritable();  //时间数:个数


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //按小时统计的结合需要初始化
        for (int i = 0; i < 24 ;i++){
            this.hourlyUnique.put(i,new HashSet<String>());
            this.houlyMap.put(new IntWritable(i),new IntWritable(0));
        }

    }

    /**
     *
     * 1 1 123  8:set(123,456,789)
     * 1 1 456
     * 1 1 789
     * 1 2 123,999,666  8:set(123,456,789,999,666)
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */

    @Override
    protected void reduce(StatsUserDimension key,
                          Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        try{
           String kn = key.getStatsCommonDimension().getKpiDimension().getKpiName();
           if(KpiType.ACTIVE_USER.kpiName.equals(kn)){
               //处理按小时统计的指标
               for (TimeOutputValue tv:values){
                   //取出时间，然后获取小时数
                   int hour = TimeUtil.getDateInfo(tv.getTime(), DateEnum.HOUR);
                   //获取hourlyUnique中的value
                   this.hourlyUnique.get(hour).add(tv.getId());
               }

               //构造输出的value
               this.v.setKpi(KpiType.HOURLY_ACTIVE_USER);
               //为hourlyMap赋值
               for (Map.Entry<Integer,Set<String>> en:hourlyUnique.entrySet()){
                   this.houlyMap.put(new IntWritable(en.getKey()),
                           new IntWritable(en.getValue().size()));
               }
               this.v.setValue(this.houlyMap);
               //输出
               context.write(key,this.v);


           } else {
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
               this.v.setKpi(KpiType.valueOfName(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
               //输出
               context.write(key,this.v);
           }
       } finally {
           //关闭按小时的统计
           this.houlyMap.clear();
           this.hourlyUnique.clear();
           for (int i = 0; i < 24 ;i++){
               this.hourlyUnique.put(i,new HashSet<String>());
               this.houlyMap.put(new IntWritable(i),new IntWritable(0));
           }
       }
    }
}
