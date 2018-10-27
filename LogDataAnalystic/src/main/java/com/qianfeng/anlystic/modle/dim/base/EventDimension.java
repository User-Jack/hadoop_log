package com.qianfeng.anlystic.modle.dim.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Auther: lyd
 * @Date: 2018/7/13 09:16
 * @Description: 事件维度
 */
public class EventDimension extends BaseDimension{

    private int id;
    private String category;
    private String action;

    public EventDimension(){

    }

    public EventDimension(String category, String action) {
        this.category = category;
        this.action = action;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.id);
        out.writeUTF(this.category);
        out.writeUTF(this.action);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.category = in.readUTF();
        this.action = in.readUTF();
    }

    @Override
    public int compareTo(BaseDimension o) {
        if(this == o){
            return  0;
        }
        EventDimension other = (EventDimension) o;
        int tmp = this.id - other.id;
        if(tmp != 0){
            return tmp;
        }
        tmp = this.category.compareTo(other.category);
        if(tmp != 0){
            return tmp;
        }
        tmp = this.action.compareTo(other.action);
        return tmp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EventDimension that = (EventDimension) o;

        if (id != that.id) return false;
        if (category != null ? !category.equals(that.category) : that.category != null) return false;
        return action != null ? action.equals(that.action) : that.action == null;
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + (category != null ? category.hashCode() : 0);
        result = 31 * result + (action != null ? action.hashCode() : 0);
        return result;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }
}
