package boluo.basics;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import static org.junit.Assert.assertEquals;

public class Note08_Regex {

    @Test
    public void func1() {
        // 正则匹配
        // 1.按行对数据分割
        String str1 = "0" + "\n\n\n" + "123\n" + "456\n" + "\n" + "789";
        List<String> strList1 = Arrays.asList(str1.split("[\\r\\n]+"));
        assertEquals(4, strList1.size());


        // 2.去除和替换
        String str2 = "3.02主营业务成本-管理费";
        // ^匹配输入字符串的开始位置, 但是在方括号中使用, 表示匹配除了^后面的字符, 以下示例中, 表示除了'-'以外的所有字符
        String subject = str2.replaceAll("^\\d+\\.\\d+([^\\-]+)\\-(.*)", "$1.$2");
        System.out.println(subject);


        // 3.日期格式匹配
        String str3 = "2020-08-01至2021-07-31优惠34000.00元。（2020年疫情减免居间费用）";
        // {n} 限定n次, {n,} 至少n次, {n,m} 在n-m次中间
        Pattern pattern = Pattern.compile("(\\d{4}-\\d{2}-\\d{2})至(\\d{4}-\\d{2}-\\d{2})优惠([\\d\\.]+)元.*");
        Matcher matcher = pattern.matcher(str3);
        if (matcher.matches()) System.out.println("str match");

        // String.format()
        String str = String.format("%03d", 10);
        System.out.println(str);
    }

    @Test
	public void func2() {
    	String str = "part1" +
				"截止2020-08-31，每年费用50400.00元。（洗烘设备数量24台+12台=36台）\n" +
				"part2" +
				"截止2021-02-28，每年费用54600.00元。（洗烘设备数量26台+13台=39台）\n" +
				"part3" +
				"截止2021-05-31，每年费用61600.00元。（洗烘设备数量29台+15台=44台）";

		Pattern pattern = Pattern.compile("截止\\d{4}-\\d{2}-\\d{2}，每年费用([\\d\\.]+)元.*");
		Matcher matcher = pattern.matcher(str);
		for (int i = 0; i < 10; i++) {
			if (matcher.find()) {
				System.out.println(matcher.start());
			}
		}

	}
}
