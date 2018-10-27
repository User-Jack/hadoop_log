package com.qianfeng.common;

/**
 * 公用常量类
 */
public class EventLogConstants {

    /**
     * 事件枚举
     */
    public static enum EventEnum{
        LANUCH(1,"lanuch event","e_l"),
        PAGEVIEW(2,"page view event","e_pv"),
        CHARGEREQUEST(3,"charge request event","e_crt"),
        CHARGESUCCESS(4,"charge success evnet","e_cs"),
        CHARGEREFUND(5,"charge refund event","e_cr"),
        EVENT(6,"event","e_e");

        public final int id;
        public final String name;
        public final String alias;

        EventEnum(int id, String name, String alias) {
            this.id = id;
            this.name = name;
            this.alias = alias;
        }

        /**
         * 根据别名获取枚举
         * @param alias
         * @return
         */
        public static EventEnum valueOfAlias(String alias){
            for (EventEnum event:values()){
                if(event.alias.equals(alias)){
                    return event;
                }
            }
            return null;
//            throw  new RuntimeException("该alias没有对应的枚举.alias:"+alias);
        }
    }

    /**
     * hbase表相关
     */
    public static final String EVENT_LOG_HBASE_NAME = "event_logs";

    public static final String EVENT_LOG_FAMILY_NAME = "info";

    /**
     * 日志收集字段常量
     */

    public static final String LOG_SEPARTOR = "\\^A";

    public static final String LOG_COLUMN_NAME_IP = "ip";

    public static final String LOG_COLUMN_NAME_SERVER_TIME = "s_time";

    public static final String LOG_COLUMN_NAME_VERSION = "ver";

    public static final String LOG_COLUMN_NAME_UUID = "u_ud";

    public static final String LOG_COLUMN_NAME_MEMBER_ID = "u_mid";

    public static final String LOG_COLUMN_NAME_SESSION_ID = "u_sd";

    public static final String LOG_COLUMN_NAME_CLIENT_TIME = "c_time";

    public static final String LOG_COLUMN_NAME_LANGUAGE = "l";

    public static final String LOG_COLUMN_NAME_USERAGENT = "b_iev";

    public static final String LOG_COLUMN_NAME_RESOLUTION = "b_rst";

    public static final String LOG_COLUMN_NAME_CURRENT_URL = "p_url";

    public static final String LOG_COLUMN_NAME_PREFER_URL = "p_ref";

    public static final String LOG_COLUMN_NAME_TITLE = "tt";

    public static final String LOG_COLUMN_NAME_PLATFORM = "pl";


    /**
     * 订单相关列
     */
    public static final String LOG_COLUMN_NAME_ORDER_ID = "oid";

    public static final String LOG_COLUMN_NAME_ORDER_NAME = "on";

    public static final String LOG_COLUMN_NAME_ORDER_CURRENCY_AMOUT = "cua";

    public static final String LOG_COLUMN_NAME_ORDER_CURRENCY_TYPE = "cut";

    public static final String LOG_COLUMN_NAME_ORDER_PAYMENT_TYPE = "pt";


    /**
     * 事件相关
     */
    public static final String LOG_COLUMN_NAME_EVENT_NAME = "en";

    public static final String LOG_COLUMN_NAME_EVENT_CATEGORY = "ca";

    public static final String LOG_COLUMN_NAME_EVENT_ACTION = "ac";

    public static final String LOG_COLUMN_NAME_EVENT_START = "kv_";

    public static final String LOG_COLUMN_NAME_EVENT_DURATION = "du";


    /**
     * useragent解析字段
     */
    public static final String LOG_COLUMN_NAME_BROWSER_NAME = "browser_name";

    public static final String LOG_COLUMN_NAME_BROWSER_VERSION = "browser_version";

    public static final String LOG_COLUMN_NAME_OS_NAME = "os_name";

    public static final String LOG_COLUMN_NAME_OS_VERSION = "os_version";

    /**
     * IP解析出来的字段
     */
    public static final String LOG_COLUMN_NAME_COUNTRY = "country";

    public static final String LOG_COLUMN_NAME_PROVINCE = "province";

    public static final String LOG_COLUMN_NAME_CITY = "city";

}
