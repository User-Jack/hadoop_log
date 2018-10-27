import com.qianfeng.etl.util.IpParserUtil;
import com.qianfeng.etl.util.ip.IPSeeker;

import java.util.List;

public class IpUtilTest {
    public static void main(String[] args) {
//        System.out.println(IPSeeker.getInstance().getCountry("218.26.184.114"));
//        System.out.println(IPSeeker.getInstance().getCountry("192.168.216.111"));

//        System.out.println(new IpParserUtil().parserIp("123.150.187.130"));

        /*List<String> ips =  IPSeeker.getInstance().getAllIp();
        for (String ip:ips){
            System.out.println("ip="+ip+"   "+new IpParserUtil().parserIp(ip));

        }*/

        List<String> ips =  IPSeeker.getInstance().getAllIp();
        for (String ip:ips){
            try {
                System.out.println("ip="+ip+"   "+new IpParserUtil().parserIp1("http://ip.taobao.com/service/getIpInfo.php?ip="+ip,"UTF-8"));
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }
}
