package boluo.basics;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Note17_Test {

    @Test
    public void func1() {
        String str = "在每年8.31日前结清下一期管理费，甲方提供正规增值税专用发票，后续新增设备管理费不满一个月按一个月计算，超过一个月不满一年按实际月份计算。（台数按实际投放台数登记）\n" +
                "浙大新宇x校区大合同，总履约保证金10000元。保洁、水电费规定查看服务协议。\n" +
                "2020.2.4留学生门店拆出。\n" +
                "2021年3月调增洗烘设备3+2=5台，管理费按1400元/台/年核算，调增流程：XMTZ20218888。\n" +
                "2021年6月调增洗衣机1台，管理费按1400元/台/年核算，调增流程：XMTZ2021068888。\n" +
                "截止2020-08-31，每年费用50400.00元。（洗烘设备数量24台+12台=36台）\n" +
                "截止2021-02-28，每年费用54600.00元。（洗烘设备数量26台+13台=39台）\n" +
                "截止2021-05-31，每年费用61600.00元。（洗烘设备数量29台+15台=44台）";

        Pattern pattern = Pattern.compile("截止\\d{4}-\\d{2}-\\d{2}，每年费用([\\d\\.]+)元.*");
        Matcher matcher = pattern.matcher(str);
        List<String> result = new ArrayList<>();

        // 切割成   ***截止**, 截止**, 截止**, "" 四段
        // TODO 把切割后的字符串放入result
        for (int position = 0; ; ) {
            if (matcher.find()) {
                result.add(str.substring(position, matcher.end()));
                position = matcher.end();
            } else {
                result.add(str.substring(position));
                break;
            }
        }

        result = result.stream().map(i -> i.replace("\n", "")).collect(Collectors.toList());
        Assert.assertFalse(result.isEmpty());
        Assert.assertEquals(result.size(), 4);
        Assert.assertTrue(result.get(0).endsWith("截止2020-08-31，每年费用50400.00元。（洗烘设备数量24台+12台=36台）"));
        Assert.assertEquals(result.get(1), "截止2021-02-28，每年费用54600.00元。（洗烘设备数量26台+13台=39台）");
        Assert.assertEquals(result.get(2), "截止2021-05-31，每年费用61600.00元。（洗烘设备数量29台+15台=44台）");
        Assert.assertEquals(result.get(3), "");
    }

}
