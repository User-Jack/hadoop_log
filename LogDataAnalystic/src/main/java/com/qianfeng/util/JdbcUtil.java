package com.qianfeng.util;

import com.qianfeng.common.GlobalConstants;

import java.sql.*;

/**
 * 获取数据库连接和关闭连接
 */
public class JdbcUtil {

    /**
     * 获取连接
     * @return
     */
    public static Connection getConnection(){
        Connection conn = null;
        //先加载驱动
        try {
            Class.forName(GlobalConstants.DRIVER);
            //开始获取
            conn = DriverManager.getConnection(GlobalConstants.URL,GlobalConstants.USERNAME,
                    GlobalConstants.PASSWORD);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * 关闭
     * @param conn
     * @param rs
     * @param ps
     */
    public static void close(Connection conn, ResultSet rs, PreparedStatement ps){
        if(conn != null){
            try {
                conn.close();
            } catch (SQLException e) {
               //do nothing
            }
        }

        if(ps != null){
            try {
                ps.close();
            } catch (SQLException e) {
                //do nothing
            }
        }

        if(rs != null){
            try {
                rs.close();
            } catch (SQLException e) {
                //do nothing
            }
        }
    }
}
