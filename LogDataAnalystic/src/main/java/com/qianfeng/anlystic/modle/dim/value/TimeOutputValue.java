package com.qianfeng.anlystic.modle.dim.value;

import com.qianfeng.common.KpiType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 用于map的输出的value的数据类型
 */
public class TimeOutputValue extends BaseStatsValueWritable{
    private String id; //泛指id:uuid memnerId  sessionId  ...
    private long time;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.id);
        out.writeLong(this.time);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.time = in.readLong();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    @Override
    public KpiType getKpi() {
        return null;
    }

}
