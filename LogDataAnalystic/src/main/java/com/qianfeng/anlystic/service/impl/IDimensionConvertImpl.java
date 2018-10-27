package com.qianfeng.anlystic.service.impl;

import com.qianfeng.anlystic.modle.dim.base.*;
import com.qianfeng.anlystic.service.IDimensionConvert;
import com.qianfeng.util.JdbcUtil;

import java.io.IOException;
import java.sql.*;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 操作维度表的接口的具体实现
 */
public class IDimensionConvertImpl implements IDimensionConvert{

    //定义一个用于做缓存map  维度的唯一标识:维度Id
    private Map<String,Integer> cache = new LinkedHashMap<String,Integer>(){

        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Integer> eldest) {
            return this.size() > 5000;
        }
    };

    /**
     *获取维度Id的具体的实现
     * @param dimension
     * @return
     * @throws IOException
     */
    @Override
    public int getDimensionIdByValue(BaseDimension dimension) throws IOException {
        Connection conn = null;
        try {
            //判断cache中是否有该维度
            String cacheKey = buildCache(dimension);
            if(this.cache.containsKey(cacheKey)){
                return this.cache.get(cacheKey).intValue();
            }
            //代码走到这儿，证明缓存中没有该维度的id.所以需要去数据库中查询
            conn = JdbcUtil.getConnection();
            //构建查询或者插入维度表的sql语句
            /**
             * 1、先查询维度表，是否有维度所对应的维度Id，有查询返回；没有先插入再返回
             */
            String[] sqls = null;
            if(dimension instanceof BrowserDimension){
                sqls = this.buildBrowserSqls(dimension);
            } else if(dimension instanceof DateDimension){
                sqls = this.buildDateSqls(dimension);
            } else if(dimension instanceof PlatformDimension){
                sqls = this.buildPlatformSqls(dimension);
            } else if(dimension instanceof KpiDimension){
                sqls = this.buildKpiSqls(dimension);
            } else if(dimension instanceof LocationDimension){
                sqls = this.buildLocalSqls(dimension);
            } else if(dimension instanceof EventDimension){
                sqls = this.buildEventSqls(dimension);
            } else if(dimension instanceof CurrencyTypeDimension){
                sqls = this.buildCurrencySqls(dimension);
            } else if(dimension instanceof PaymentTypeDimension){
                sqls = this.buildPaymentSqls(dimension);
            } else {
                throw new RuntimeException("dimension维度暂不支持.");
            }


            int id = -1;
            //执行sql
            synchronized (this){
                id = this.executSql(conn,cacheKey,sqls,dimension);
            }
            this.cache.put(cacheKey,id); //将获取出来的维度id的值存储到缓存中
            return id;
        } catch (Exception e){
            throw  new RuntimeException("获取维度Id方法失败",e);
        } finally {
            JdbcUtil.close(conn,null,null);
        }
    }


    /**
     * 构建kpi的sql
     * @param dimension
     * @return
     */
    private String[] buildKpiSqls(BaseDimension dimension) {
        String query = "select id from `dimension_kpi` where `kpi_name` = ?";
        String insert = "insert into `dimension_kpi`(`kpi_name`) values(?)";
        return new String[]{query,insert};
    }

    private String[] buildPlatformSqls(BaseDimension dimension) {
        String query = "select id from `dimension_platform` where `platform_name` = ?";
        String insert = "insert into `dimension_platform`(`platform_name`) values(?)";
        return new String[]{query,insert};
    }

    private String[] buildDateSqls(BaseDimension dimension) {
        String query = "select id from `dimension_date` where `year` = ? and `season` = ? and `month` = ?" +
                " and `week` = ? and `day` = ? and `calendar` = ? and `type` = ?";
        String insert = "insert into `dimension_date`(`year` ,`season` ,`month` ,`week` ,`day` ,`calendar` ,`type`) " +
                "values(?,?,?,?,?,?,?)";
        return new String[]{query,insert};
    }

    private String[] buildBrowserSqls(BaseDimension dimension) {
        String query = "select id from `dimension_browser` where `browser_name` = ? and `browser_version` = ?";
        String insert = "insert into `dimension_browser`(`browser_name`,`browser_version`) values(?,?)";
        return new String[]{query,insert};
    }

    private String[] buildLocalSqls(BaseDimension dimension) {
        String query = "select id from `dimension_location` where `country` = ? and `province` = ? and `city` = ?";
        String insert = "insert into `dimension_location`(`country`,`province`,`city`) values(?,?,?)";
        return new String[]{query,insert};
    }

    private String[] buildEventSqls(BaseDimension dimension) {
        String query = "select id from `dimension_event` where `category` = ? and  `action` = ?";
        String insert = "insert into `dimension_event`(`category`, `action`) values(?,?)";
        return new String[]{query,insert};
    }

    private String[] buildPaymentSqls(BaseDimension dimension) {
        String query = "select id from `dimension_payment_type` where `payment_type` = ?";
        String insert = "insert into `dimension_payment_type`(`payment_type`) values(?)";
        return new String[]{query,insert};
    }

    private String[] buildCurrencySqls(BaseDimension dimension) {
        String query = "select id from `dimension_currency_type` where `currency_name` = ?";
        String insert = "insert into `dimension_currency_type`(`currency_name`) values(?)";
        return new String[]{query,insert};
    }


    /**
     * 构建每一个dimension的key
     * @param dimension
     * @return
     */
    private String buildCache(BaseDimension dimension) {
        StringBuilder sb = new StringBuilder();
        if(dimension instanceof BrowserDimension){
            sb.append("browser_");
            BrowserDimension browser = (BrowserDimension) dimension;
            sb.append(browser.getBrowserName()).append(browser.getBrowserVersion());
        } else if(dimension instanceof DateDimension){
            sb.append("date_");
            DateDimension date = (DateDimension) dimension;
            sb.append(date.getYear()).
                    append(date.getSeason())
            .append(date.getMonth())
            .append(date.getWeek())
            .append(date.getDay())
            .append(date.getType())
            ;
        } else if(dimension instanceof PlatformDimension){
            sb.append("platform_");
            PlatformDimension platform = (PlatformDimension) dimension;
            sb.append(platform.getPlatformName());
        } else if(dimension instanceof KpiDimension){
            sb.append("kpi_");
            KpiDimension kpi = (KpiDimension) dimension;
            sb.append(kpi.getKpiName());
        } else if(dimension instanceof LocationDimension){
            sb.append("local_");
            LocationDimension local = (LocationDimension) dimension;
            sb.append(local.getCountry());
            sb.append(local.getProvince());
            sb.append(local.getCity());
        }  else if(dimension instanceof EventDimension){
            sb.append("event_");
            EventDimension event = (EventDimension) dimension;
            sb.append(event.getCategory());
            sb.append(event.getAction());
        } else if(dimension instanceof PaymentTypeDimension){
            sb.append("payment_");
            PaymentTypeDimension payment = (PaymentTypeDimension) dimension;
            sb.append(payment.getPaymentType());
        } else if(dimension instanceof CurrencyTypeDimension){
            sb.append("currency_");
            CurrencyTypeDimension currency = (CurrencyTypeDimension) dimension;
            sb.append(currency.getCurrencyName());
        } else {
            throw new RuntimeException("dimension维度暂不支持."+dimension.getClass());
        }

        return sb == null ? null : sb.toString();
    }


    /**
     * 执行sql
     * @param conn
     * @param cacheKey
     * @param sqls
     * @param dimension
     * @return
     */
    private int executSql(Connection conn, String cacheKey, String[] sqls, BaseDimension dimension) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try{
            //获取ps
            //先查询是否有维度Id，没有则插入
            ps = conn.prepareStatement(sqls[0]);
            this.setArgs(dimension,ps);
            //执行
            rs = ps.executeQuery();
            if(rs.next()){
                return rs.getInt(1);
            }
            //代码走到这儿，没有查询出对应的维度Id
            ps = conn.prepareStatement(sqls[1], Statement.RETURN_GENERATED_KEYS);
            this.setArgs(dimension,ps);
            ps.executeUpdate(); //返回影响的函数
            rs = ps.getGeneratedKeys();
            if(rs.next()){
                return rs.getInt(1);
            }
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            JdbcUtil.close(null,rs,ps);
        }
        throw  new RuntimeException("该diemnsion不能获取到维度Id.dimension:"+dimension.getClass());
    }

    /**
     * 设置参数
     * @param dimension
     * @param ps
     */
    private void setArgs(BaseDimension dimension, PreparedStatement ps) {
        try{
            int i = 0;
            if(dimension instanceof BrowserDimension){
                BrowserDimension browser = (BrowserDimension) dimension;
                ps.setString(++i,browser.getBrowserName());
                ps.setString(++i,browser.getBrowserVersion());
            } else if(dimension instanceof DateDimension){
                DateDimension date = (DateDimension) dimension;
                ps.setInt(++i,date.getYear());
                ps.setInt(++i,date.getSeason());
                ps.setInt(++i,date.getMonth());
                ps.setInt(++i,date.getWeek());
                ps.setInt(++i,date.getDay());
                ps.setDate(++i,new Date(date.getCalendar().getTime())); //设置时间
                ps.setString(++i,date.getType());
            } else if(dimension instanceof PlatformDimension){
                PlatformDimension platform = (PlatformDimension) dimension;
                ps.setString(++i,platform.getPlatformName());
            } else if(dimension instanceof KpiDimension){
                KpiDimension kpi = (KpiDimension) dimension;
                ps.setString(++i,kpi.getKpiName());
            } else if(dimension instanceof LocationDimension){
                LocationDimension local = (LocationDimension) dimension;
                ps.setString(++i,local.getCountry());
                ps.setString(++i,local.getProvince());
                ps.setString(++i,local.getCity());
            } else if(dimension instanceof EventDimension){
                EventDimension event = (EventDimension) dimension;
                ps.setString(++i,event.getCategory());
                ps.setString(++i,event.getAction());
            } else if(dimension instanceof PaymentTypeDimension){
                PaymentTypeDimension payment = (PaymentTypeDimension) dimension;
                ps.setString(++i,payment.getPaymentType());
            } else if(dimension instanceof CurrencyTypeDimension){
                CurrencyTypeDimension currency = (CurrencyTypeDimension) dimension;
                ps.setString(++i,currency.getCurrencyName());
            }  else {
                throw new RuntimeException("dimension维度暂不支持.");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}