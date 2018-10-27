package com.qianfeng.anlystic.modle.dim.value;

import com.qianfeng.common.KpiType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 用于map的输出的value的数据类型
 */
public class TextOutputValue extends BaseStatsValueWritable{
    private String uuid = "";
    private String sessionId = "";

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.uuid);
        out.writeUTF(this.sessionId);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.uuid = in.readUTF();
        this.sessionId = in.readUTF();
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    @Override
    public KpiType getKpi() {
        return null;
    }

}
