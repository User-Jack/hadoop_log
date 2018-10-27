package com.qianfeng.anlystic.modle.dim;

import com.qianfeng.anlystic.modle.dim.base.BaseDimension;
import com.qianfeng.anlystic.modle.dim.base.BrowserDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 可用于用户模块、浏览器模块map阶段的输出的key
 */
public class StatsUserDimension extends StatsDimension{

    private StatsCommonDimension statsCommonDimension = new StatsCommonDimension();
    private BrowserDimension browserDimension = new BrowserDimension();

    public StatsUserDimension(){

    }

    public StatsUserDimension(StatsCommonDimension statsCommonDimension, BrowserDimension browserDimension) {
        this.statsCommonDimension = statsCommonDimension;
        this.browserDimension = browserDimension;
    }

    /**
     * 克隆一个当前对象
     * @param dimension
     * @return
     */
    public static StatsUserDimension clone(StatsUserDimension dimension){
        BrowserDimension browserDimension = new BrowserDimension(dimension.browserDimension.getId(),
                dimension.browserDimension.getBrowserName(),dimension.browserDimension.getBrowserVersion());
        StatsCommonDimension statsCommonDimension = StatsCommonDimension.clone(dimension.statsCommonDimension);
        return new StatsUserDimension(statsCommonDimension,browserDimension);
    }


    @Override
    public void write(DataOutput out) throws IOException {
        this.browserDimension.write(out);
        this.statsCommonDimension.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.browserDimension.readFields(in);
        this.statsCommonDimension.readFields(in);
    }

    @Override
    public int compareTo(BaseDimension o) {
        if(this == o){
            return  0;
        }
        StatsUserDimension other = (StatsUserDimension) o;
        int tmp = this.statsCommonDimension.compareTo(other.statsCommonDimension);
        if(tmp != 0){
            return tmp;
        }
        tmp = this.browserDimension.compareTo(other.browserDimension);
        return tmp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StatsUserDimension that = (StatsUserDimension) o;

        if (statsCommonDimension != null ? !statsCommonDimension.equals(that.statsCommonDimension) : that.statsCommonDimension != null)
            return false;
        return browserDimension != null ? browserDimension.equals(that.browserDimension) : that.browserDimension == null;
    }

    @Override
    public int hashCode() {
        int result = statsCommonDimension != null ? statsCommonDimension.hashCode() : 0;
        result = 31 * result + (browserDimension != null ? browserDimension.hashCode() : 0);
        return result;
    }

    public StatsCommonDimension getStatsCommonDimension() {
        return statsCommonDimension;
    }

    public void setStatsCommonDimension(StatsCommonDimension statsCommonDimension) {
        this.statsCommonDimension = statsCommonDimension;
    }

    public BrowserDimension getBrowserDimension() {
        return browserDimension;
    }

    public void setBrowserDimension(BrowserDimension browserDimension) {
        this.browserDimension = browserDimension;
    }
}
