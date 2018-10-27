package com.qianfeng.anlystic.modle.dim.value;

import com.qianfeng.common.KpiType;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 用于reduce阶段输出的value的数据类型
 */
public class MapWritableValue extends BaseStatsValueWritable{
    private MapWritable value = new MapWritable();
    private KpiType kpi;
    public MapWritableValue(){

    }

    public MapWritableValue(MapWritable value, KpiType kpi) {
        this.value = value;
        this.kpi = kpi;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        this.value.write(out); //自带mapwritable类型
        WritableUtils.writeEnum(out,kpi); //持久化枚举类型
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.value.readFields(in);
        WritableUtils.readEnum(in,KpiType.class);
    }


    public MapWritable getValue() {
        return value;
    }

    public void setValue(MapWritable value) {
        this.value = value;
    }

    public void setKpi(KpiType kpi) {
        this.kpi = kpi;
    }

    @Override
    public KpiType getKpi() {
        return this.kpi;
    }
}
