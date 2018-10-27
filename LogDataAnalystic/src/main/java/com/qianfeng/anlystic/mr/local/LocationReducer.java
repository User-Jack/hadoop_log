package com.qianfeng.anlystic.mr.local;

import com.qianfeng.anlystic.modle.dim.StatsLocationDimension;
import com.qianfeng.anlystic.modle.dim.value.LocationReduceOutputWritable;
import com.qianfeng.anlystic.modle.dim.value.TextOutputValue;
import com.qianfeng.common.KpiType;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 地域的reduce
 */
public class LocationReducer extends Reducer<StatsLocationDimension,TextOutputValue,
        StatsLocationDimension,LocationReduceOutputWritable> {

    private LocationReduceOutputWritable v = new LocationReduceOutputWritable();
    private Set<String> uniquque = new HashSet<String>();
    //用于存储sessionId:次数
    private Map<String,Integer> sessions = new HashMap<String,Integer>();


    @Override
    protected void reduce(StatsLocationDimension key, Iterable<TextOutputValue> values, Context context) throws IOException, InterruptedException {
        try {
            //循环values
            for (TextOutputValue tv:values){
                //将TV中的id添加到set中
                if(StringUtils.isNotEmpty(tv.getUuid())){
                    this.uniquque.add(tv.getUuid());
                }
                //处理sessions
                if(sessions.containsKey(tv.getSessionId())){
                    //表示session已经出现过一次
                    this.sessions.put(tv.getSessionId(),2);
                } else {
                    this.sessions.put(tv.getSessionId(),1);// 是跳出会话个数
                }
            }

            //构造reduce、阶段输出的value
            this.v.setActiveUsers(this.uniquque.size());
            this.v.setSessions(this.sessions.size());
            //计算跳出会话的个数
            int bounceNum = 0;
            for (Map.Entry<String,Integer> en:sessions.entrySet()){
                if(en.getValue() == 1){
                    bounceNum ++ ;
                }
            }
            this.v.setBounceSessions(bounceNum);

            //为v设置kpi
            this.v.setKpi(KpiType.valueOfName(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
            //输出
            context.write(key,this.v);
        } finally {
            //先清空set
            this.uniquque.clear();
            this.sessions.clear();
        }
    }
}
