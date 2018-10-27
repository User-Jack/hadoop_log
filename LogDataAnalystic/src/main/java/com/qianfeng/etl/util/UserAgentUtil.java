package com.qianfeng.etl.util;

import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;
import cz.mallat.uasparser.UserAgentInfo;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * 浏览器代理对象userAgent的解析
 */
public class UserAgentUtil {
    private static final Logger logger = Logger.getLogger(UASparser.class);
    public static UASparser uaSparser = null;

    /**
     * 静态代码块获取
     */
    static {
        try {
            uaSparser = new UASparser(OnlineUpdater.getVendoredInputStream());
        } catch (IOException e) {
            logger.warn("获取uaspaser对象异常.",e);
        }
    }

    public static UserAgentInfo parserUserAgent(String userAgent){
       //用于返回
        UserAgentInfo ui = null;
        //判断userAgent是否为空
        if(StringUtils.isEmpty(userAgent)){
            return ui;
        }

        try {
            //正常解析
           cz.mallat.uasparser.UserAgentInfo info = uaSparser.parse(userAgent);
            if(info != null){
                //添加
                ui = new UserAgentInfo();
                ui.setBrowserName(info.getUaFamily());
                ui.setBrowserVersion(info.getBrowserVersionInfo());
                ui.setOsName(info.getOsFamily());
                ui.setOsVersion(info.getOsName());
            }
        } catch (IOException e) {
            logger.warn("uaspaser解析异常.",e);
        }

        return ui;
    }

    /**
     * 用于封装解析后的属性
     */
    public static class UserAgentInfo{
       private String browserName;
       private String browserVersion;
       private String osName;
       private String osVersion;

        public UserAgentInfo() {
        }

        public UserAgentInfo(String browserName, String browserVersion, String osName, String osVersion) {
            this.browserName = browserName;
            this.browserVersion = browserVersion;
            this.osName = osName;
            this.osVersion = osVersion;
        }

        public String getBrowserName() {
            return browserName;
        }

        public void setBrowserName(String browserName) {
            this.browserName = browserName;
        }

        public String getBrowserVersion() {
            return browserVersion;
        }

        public void setBrowserVersion(String browserVersion) {
            this.browserVersion = browserVersion;
        }

        public String getOsName() {
            return osName;
        }

        public void setOsName(String osName) {
            this.osName = osName;
        }

        public String getOsVersion() {
            return osVersion;
        }

        public void setOsVersion(String osVersion) {
            this.osVersion = osVersion;
        }

        @Override
        public String toString() {
            return "UserAgentInfo{" +
                    "browserName='" + browserName + '\'' +
                    ", browserVersion='" + browserVersion + '\'' +
                    ", osName='" + osName + '\'' +
                    ", osVersion='" + osVersion + '\'' +
                    '}';
        }
    }
}
