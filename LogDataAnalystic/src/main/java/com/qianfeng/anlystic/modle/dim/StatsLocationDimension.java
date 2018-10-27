package com.qianfeng.anlystic.modle.dim;

import com.qianfeng.anlystic.modle.dim.base.BaseDimension;
import com.qianfeng.anlystic.modle.dim.base.LocationDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Auther: lyd
 * @Date: 2018/7/12 11:32
 * @Description: 用于map阶段的输出key
 */
public class StatsLocationDimension extends StatsDimension{
    private StatsCommonDimension statsCommonDimension = new StatsCommonDimension();
    private LocationDimension locationDimension = new LocationDimension();

    public StatsLocationDimension(){
    }

    public StatsLocationDimension(StatsCommonDimension statsCommonDimension, LocationDimension locationDimension) {
        this.statsCommonDimension = statsCommonDimension;
        this.locationDimension = locationDimension;
    }

    /**
     * 克隆一个当前对象
     * @param dimension
     * @return
     */
    public static StatsLocationDimension clone(StatsLocationDimension dimension){
        LocationDimension locationDimension = LocationDimension.newInstance(
                dimension.locationDimension.getCountry(),
                dimension.locationDimension.getProvince(),
                dimension.locationDimension.getCity()
        );
        StatsCommonDimension statsCommonDimension = StatsCommonDimension.clone(dimension.statsCommonDimension);
        return new StatsLocationDimension(statsCommonDimension,locationDimension);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.locationDimension.write(out);
        this.statsCommonDimension.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.locationDimension.readFields(in);
        this.statsCommonDimension.readFields(in);
    }


    @Override
    public int compareTo(BaseDimension o) {
        if(this == o){
            return  0;
        }
        StatsLocationDimension other = (StatsLocationDimension) o;
        int tmp = this.statsCommonDimension.compareTo(other.statsCommonDimension);
        if(tmp != 0){
            return tmp;
        }
        tmp = this.locationDimension.compareTo(other.locationDimension);
        return tmp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StatsLocationDimension that = (StatsLocationDimension) o;

        if (statsCommonDimension != null ? !statsCommonDimension.equals(that.statsCommonDimension) : that.statsCommonDimension != null)
            return false;
        return locationDimension != null ? locationDimension.equals(that.locationDimension) : that.locationDimension == null;
    }

    @Override
    public int hashCode() {
        int result = statsCommonDimension != null ? statsCommonDimension.hashCode() : 0;
        result = 31 * result + (locationDimension != null ? locationDimension.hashCode() : 0);
        return result;
    }

    public StatsCommonDimension getStatsCommonDimension() {
        return statsCommonDimension;
    }

    public void setStatsCommonDimension(StatsCommonDimension statsCommonDimension) {
        this.statsCommonDimension = statsCommonDimension;
    }

    public LocationDimension getLocationDimension() {
        return locationDimension;
    }

    public void setLocationDimension(LocationDimension locationDimension) {
        this.locationDimension = locationDimension;
    }
}
