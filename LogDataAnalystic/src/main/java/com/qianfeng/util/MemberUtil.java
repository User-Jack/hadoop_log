package com.qianfeng.util;

import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @Auther: lyd
 * @Date: 2018/7/11 09:13
 * @Description: 新增会员的工具类
 */
public class MemberUtil {
    //用于做缓存，缓存memberId:是否是新的
    public static Map<String,Boolean> cache = new LinkedHashMap<String,Boolean>(){
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
            return this.size() > 5000;
        }
    };

    /**
     * 判断memberId是否有效
     * @param memberId
     * @return
     */
    public static boolean isValidateMemberId(String memberId){

        return false;
    }


    /**
     * 判断是否为新增的会员，如果返回true，则是新增会员。反之不是
     * @param memberId
     * @param conn
     * @return
     */
    public static boolean isNewMember(String memberId, Connection conn){
        Boolean isNewMember = null;
       if(StringUtils.isNotEmpty(memberId)){
           //从缓存中去取对应的memberId
           isNewMember = cache.get(memberId);
           //判断缓存中是否存在
           if(isNewMember == null){
               //查询历史数据
               /**
                * 1、先查询缓存，缓存有则直接返回，没有就查询数据库
                * 数据库有为不是新会员，没有则是新会员
                */
               PreparedStatement ps = null;
               ResultSet rs = null;
               try{
                   ps = conn.prepareStatement("select `member_id` from `member_info` where `member_id` = ?");
                   ps.setString(1,memberId);
                   //执行
                   rs = ps.executeQuery();
                   if(rs.next()){
                       //不是一个新增会员
                       isNewMember = Boolean.valueOf(false);
                   } else {
                       //是一个新增会员
                       isNewMember = Boolean.valueOf(true);
                   }
                   //将其查询的结果存储到cache中
                   cache.put(memberId,isNewMember);
               } catch (Exception e){
                   e.printStackTrace();
               } finally {
                   JdbcUtil.close(null,rs,ps);
               }
           }
       }
        return isNewMember == null ? false : isNewMember.booleanValue() ;
    }

    //可以自己写一个工具方法来清除某一天或者是一个时段的历史数据

}
