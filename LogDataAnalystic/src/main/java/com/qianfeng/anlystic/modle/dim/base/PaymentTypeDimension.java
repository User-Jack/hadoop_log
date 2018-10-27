package com.qianfeng.anlystic.modle.dim.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Auther: lyd
 * @Date: 2018/7/16 11:21
 * @Description:支付类型
 */
public class PaymentTypeDimension extends BaseDimension{
    private int id;
    private String paymentType;

    public PaymentTypeDimension(){

    }

    public PaymentTypeDimension(String paymentType) {
        this.paymentType = paymentType;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.id);
        out.writeUTF(this.paymentType);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.paymentType = in.readUTF();
    }

    @Override
    public int compareTo(BaseDimension o) {
        if(this == o){
            return  0;
        }
        PaymentTypeDimension other = (PaymentTypeDimension) o;
        int tmp = this.id - other.id;
        if(tmp != 0){
            return tmp;
        }
        tmp = this.paymentType.compareTo(other.paymentType);
        return tmp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PaymentTypeDimension that = (PaymentTypeDimension) o;

        if (id != that.id) return false;
        return paymentType != null ? paymentType.equals(that.paymentType) : that.paymentType == null;
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + (paymentType != null ? paymentType.hashCode() : 0);
        return result;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getPaymentType() {
        return paymentType;
    }

    public void setPaymentType(String paymentType) {
        this.paymentType = paymentType;
    }
}
