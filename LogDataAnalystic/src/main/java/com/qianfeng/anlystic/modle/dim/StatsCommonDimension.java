package com.qianfeng.anlystic.modle.dim;

import com.qianfeng.anlystic.modle.dim.base.BaseDimension;
import com.qianfeng.anlystic.modle.dim.base.DateDimension;
import com.qianfeng.anlystic.modle.dim.base.KpiDimension;
import com.qianfeng.anlystic.modle.dim.base.PlatformDimension;
import org.apache.hadoop.fs.Stat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 封装常用的维度类（dateDimension,platformDimension,kpiDimension）
 */
public class StatsCommonDimension extends StatsDimension{
    private DateDimension dateDimension = new DateDimension();
    private PlatformDimension platformDimension = new PlatformDimension();
    private KpiDimension kpiDimension = new KpiDimension();

    public StatsCommonDimension(){

    }

    public StatsCommonDimension(DateDimension dateDimension,
                                PlatformDimension platformDimension,
                                KpiDimension kpiDimension) {
        this.dateDimension = dateDimension;
        this.platformDimension = platformDimension;
        this.kpiDimension = kpiDimension;
    }

    /**
     * 根据当前对象克隆一个对象
     * @param dimension
     * @return
     */
    public static StatsCommonDimension clone(StatsCommonDimension dimension){
        DateDimension dateDimension = new DateDimension(dimension.dateDimension.getId(),
                dimension.dateDimension.getYear(),
                dimension.dateDimension.getSeason(),dimension.dateDimension.getMonth(),
                dimension.dateDimension.getWeek(),dimension.dateDimension.getDay(),
                dimension.dateDimension.getType(),dimension.dateDimension.getCalendar());
        PlatformDimension platformDimension = new PlatformDimension(dimension.platformDimension.getId(),
                dimension.platformDimension.getPlatformName());
        KpiDimension kpiDimension = new KpiDimension(dimension.kpiDimension.getId(),
                dimension.kpiDimension.getKpiName());
        return new StatsCommonDimension(dateDimension,platformDimension,kpiDimension);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.dateDimension.write(out); //持久对象
        this.platformDimension.write(out);
        this.kpiDimension.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.dateDimension.readFields(in); //读对象
        this.platformDimension.readFields(in);
        this.kpiDimension.readFields(in);
    }

    @Override
    public int compareTo(BaseDimension o) {
        if(this == o){
            return  0;
        }
        StatsCommonDimension other = (StatsCommonDimension) o;
        int tmp = this.dateDimension.compareTo(other.dateDimension);
        if(tmp != 0){
            return tmp;
        }
        tmp = this.platformDimension.compareTo(other.platformDimension);
        if(tmp != 0){
            return tmp;
        }
        tmp = this.kpiDimension.compareTo(other.kpiDimension);
        return tmp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StatsCommonDimension that = (StatsCommonDimension) o;

        if (dateDimension != null ? !dateDimension.equals(that.dateDimension) : that.dateDimension != null)
            return false;
        if (platformDimension != null ? !platformDimension.equals(that.platformDimension) : that.platformDimension != null)
            return false;
        return kpiDimension != null ? kpiDimension.equals(that.kpiDimension) : that.kpiDimension == null;
    }

    @Override
    public int hashCode() {
        int result = dateDimension != null ? dateDimension.hashCode() : 0;
        result = 31 * result + (platformDimension != null ? platformDimension.hashCode() : 0);
        result = 31 * result + (kpiDimension != null ? kpiDimension.hashCode() : 0);
        return result;
    }

    public DateDimension getDateDimension() {
        return dateDimension;
    }

    public void setDateDimension(DateDimension dateDimension) {
        this.dateDimension = dateDimension;
    }

    public PlatformDimension getPlatformDimension() {
        return platformDimension;
    }

    public void setPlatformDimension(PlatformDimension platformDimension) {
        this.platformDimension = platformDimension;
    }

    public KpiDimension getKpiDimension() {
        return kpiDimension;
    }

    public void setKpiDimension(KpiDimension kpiDimension) {
        this.kpiDimension = kpiDimension;
    }
}
