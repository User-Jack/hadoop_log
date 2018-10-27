import com.qianfeng.anlystic.modle.dim.base.BrowserDimension;
import com.qianfeng.anlystic.modle.dim.base.EventDimension;
import com.qianfeng.anlystic.modle.dim.base.LocationDimension;
import com.qianfeng.anlystic.modle.dim.base.PlatformDimension;
import com.qianfeng.anlystic.service.IDimensionConvert;
import com.qianfeng.anlystic.service.impl.IDimensionConvertImpl;

public class DimensionTest {
    public static void main(String[] args)  throws Exception{
        IDimensionConvert iDimensionConvert = new IDimensionConvertImpl();
//        System.out.println(iDimensionConvert.getDimensionIdByValue(new PlatformDimension("all")));
//        System.out.println(iDimensionConvert.getDimensionIdByValue(new BrowserDimension("Chrome","46.0.2490.71")));

//        System.out.println(iDimensionConvert.getDimensionIdByValue(new LocationDimension("中国","河南","郑州")));
        System.out.println(iDimensionConvert.getDimensionIdByValue(new EventDimension("aaa","cc")));


    }
}