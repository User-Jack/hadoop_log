package com.qianfeng.anlystic.service;

import com.qianfeng.anlystic.modle.dim.base.BaseDimension;

import java.io.IOException;

/**
 * 操作维度表的接口
 */
public interface IDimensionConvert {
    /**
     * 更加传入的维度对象获取对应的维度的Id
     * @param dimension
     * @return
     * @throws IOException
     */
    int getDimensionIdByValue(BaseDimension dimension) throws IOException;
}
