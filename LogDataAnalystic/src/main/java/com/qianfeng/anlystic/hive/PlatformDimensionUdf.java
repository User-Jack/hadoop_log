package com.qianfeng.anlystic.hive;

import com.qianfeng.anlystic.modle.dim.base.PlatformDimension;
import com.qianfeng.anlystic.service.IDimensionConvert;
import com.qianfeng.anlystic.service.impl.IDimensionConvertImpl;
import com.qianfeng.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;


/**
 *
 */
public class PlatformDimensionUdf extends UDF {
    public IDimensionConvert converter =null;

    public PlatformDimensionUdf() {
        this.converter =new IDimensionConvertImpl();
    }

    /**
     * 根据平台名称获取对应的平台维度Id
     * @param platformName
     * @return
     */
    public IntWritable evaluate(String  platformName){

        if (StringUtils.isEmpty(platformName.toString())){
            platformName= GlobalConstants.DEFAULT_VALUE;
        }


        //获取平台维度
        PlatformDimension platformDimension =new PlatformDimension(platformName);
        try {
            return new IntWritable(converter.getDimensionIdByValue(platformDimension));
        } catch (Exception e) {
            throw new RuntimeException("获取平台维度ID异常"+e);
        }
    }

    public static void main(String[] args) {
        System.out.println(new PlatformDimensionUdf().evaluate("website"));
    }
}

