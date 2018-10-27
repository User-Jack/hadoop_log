package com.qianfeng.anlystic.modle.dim.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Auther: lyd
 * @Date: 2018/7/16 11:16
 * @Description:货币类型
 */
public class CurrencyTypeDimension extends BaseDimension{
    private int id;
    private String currencyName;

    public CurrencyTypeDimension(){

    }

    public CurrencyTypeDimension(String currencyName) {
        this.currencyName = currencyName;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.write(this.id);
        out.writeUTF(this.currencyName);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.currencyName = in.readUTF();
    }

    @Override
    public int compareTo(BaseDimension o) {
        if(this == o){
            return  0;
        }
        CurrencyTypeDimension other = (CurrencyTypeDimension) o;
        int tmp = this.id - other.id;
        if(tmp != 0){
            return tmp;
        }
        tmp = this.currencyName.compareTo(other.currencyName);
        return tmp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CurrencyTypeDimension that = (CurrencyTypeDimension) o;

        if (id != that.id) return false;
        return currencyName != null ? currencyName.equals(that.currencyName) : that.currencyName == null;
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + (currencyName != null ? currencyName.hashCode() : 0);
        return result;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCurrencyName() {
        return currencyName;
    }

    public void setCurrencyName(String currencyName) {
        this.currencyName = currencyName;
    }
}
