package com.qianfeng.common;

/**
 * kpi的枚举
 */
public enum  KpiType {

    NEW_INSTALL_USER("new_install_user"),
    BROWSER_NEW_INSTALL_USER("browser_new_install_user"),
    ACTIVE_USER("active_user"),
    BROWSER_ACTIVE_USER("browser_active_user"),
    ACTIVE_MEMBER("active_member"),
    BROWSER_ACTIVE_MEMBER("browser_active_member"),
    NEW_MEMBER("new_member"),
    BROWSER_NEW_MEMBER("browser_new_member"),
    MEMBER_INFO("member_info"),
    SESSION("session"),
    BROWSER_SESSION("browser_session"),
    HOURLY_ACTIVE_USER("hourly_active_user"),
    HOURLY_SESSION("hourly_session"),
    PAGE_VIEW("pageview"),
    LOCATION("location"),
    ;

    public final String kpiName;

    KpiType(String kpiName) {
        this.kpiName = kpiName;
    }

    /**
     * 根据kpiName来放回kpiType的枚举
     * @param name
     * @return
     */
    public static KpiType valueOfName(String name){
        for (KpiType kpi : values()){
            if(name.equals(kpi.kpiName)){
                return kpi;
            }
        }
        return null;
    }

}
