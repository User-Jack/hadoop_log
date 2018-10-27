package com.qianfeng.anlystic.hive;


import com.qianfeng.anlystic.modle.dim.base.CurrencyTypeDimension;
import com.qianfeng.anlystic.service.IDimensionConvert;
import com.qianfeng.anlystic.service.impl.IDimensionConvertImpl;
import com.qianfeng.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 获取货币类型维度的Id
 * Created by lyd on 2018/4/9.
 */
public class CurrencyTypeDimensionUdf extends UDF{

    public IDimensionConvert converter =null;

    public CurrencyTypeDimensionUdf(){
        converter = new IDimensionConvertImpl();
    }

    /**
     *
     * @param name
     * @return
     */
    public int evaluate(String name){
        name = name == null || StringUtils.isEmpty(name.trim()) ? GlobalConstants.DEFAULT_VALUE :name.trim() ;
        CurrencyTypeDimension currencyTypeDimension = new CurrencyTypeDimension(name);
        try {
            return converter.getDimensionIdByValue(currencyTypeDimension);
        } catch (Exception e) {
            e.printStackTrace();
        }
        throw new RuntimeException("获取货币类型维度的Id异常");
    }

    public static void main(String[] args) {
        System.out.println(new CurrencyTypeDimensionUdf().evaluate(null));
    }
}
