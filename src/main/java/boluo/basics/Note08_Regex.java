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
    public void func10() {
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
}
