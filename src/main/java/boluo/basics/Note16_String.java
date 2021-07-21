package boluo.basics;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Note16_String {

	@Test
	public void func1() {
		// 分割字符串且保留切割符
		String str = "管理费标准：1000元/台/年；管理费半年度结算，水电费季度结算，到达每个周期后2周内甲方开具增值税专用发票，乙方在收到发票3个工作日内支付到甲方账户。\n" +
				"注：原合同约定紫金港投放18+16=34台；2019年12月11日签订补充协议，约定于2019年12月1日起增加12+8=20台，累计投放54台。\n" +
				"2021.2.4进行留学生门店拆出创建新项目。\n" +
				"门店编号：428  费用：7000.00\n" +
				"门店编号：429  费用：7000.00\n" +
				"门店编号：430  费用：4000.00\n" +
				"门店编号：431  费用：4000.00\n" +
				"门店编号：432  费用：4000.00\n" +
				"门店编号：433  费用：4000.00\n" +
				"门店编号：434  费用：4000.00\n" +
				"截止2019-11-30，每年费用34000.00元。（洗烘设备数量18台+16台=34台）\n" +
				"门店编号：428  费用：7000.00\n" +
				"门店编号：429  费用：7000.00\n" +
				"门店编号：430  费用：4000.00\n" +
				"门店编号：431  费用：4000.00\n" +
				"门店编号：432  费用：4000.00\n" +
				"门店编号：433  费用：4000.00\n" +
				"门店编号：434  费用：4000.00\n" +
				"门店编号：897  费用：20000.00\n" +
				"截止2021-01-30，每年费用54000.00元。（洗烘设备数量18台+16台=34台）\n" +
				"门店编号：428  费用：7000.00\n" +
				"门店编号：429  费用：7000.00\n" +
				"门店编号：430  费用：4000.00\n" +
				"门店编号：431  费用：4000.00\n" +
				"门店编号：432  费用：4000.00\n" +
				"门店编号：433  费用：4000.00\n" +
				"门店编号：434  费用：4000.00\n" +
				"门店编号：897  费用：20000.00\n";

		String pattern = "截止\\d{4}-\\d{2}-\\d{2}，每年费用([\\d\\.]+)元.*";

		//1. 定义匹配模式
		Pattern p = Pattern.compile(pattern);
		Matcher m = p.matcher(str);

		//2. 拆分句子[拆分后的句子符号也没了]
		String[] words = p.split(str);

		//3. 保留原来的分隔符
		if (words.length > 0) {
			int count = 0;
			while (count < words.length) {
				if (m.find()) {
					words[count] += m.group();
				}
				count++;
			}
		}
		Arrays.stream(words).forEach(System.out::println);
	}

	@Test
	public void func2() {

		// 按照偏移量切分字符串	切割成   ***截止**, 截止**, 截止**, "" 四段
		String str = "在每年8.31日前结清下一期管理费，甲方提供正规增值税专用发票，后续新增设备管理费不满一个月按一个月计算，超过一个月不满一年按实际月份计算。（台数按实际投放台数登记）\n" +
				"浙大新宇x校区大合同，总履约保证金10000元。保洁、水电费规定查看服务协议。\n" +
				"2020.2.4留学生门店拆出。\n" +
				"2021年3月调增洗烘设备3+2=5台，管理费按1400元/台/年核算，调增流程：XMTZ20218888。\n" +
				"2021年6月调增洗衣机1台，管理费按1400元/台/年核算，调增流程：XMTZ2021068888。\n" +
				"截止2020-08-31，每年费用50400.00元。（洗烘设备数量24台+12台=36台）\n" +
				"截止2021-02-28，每年费用54600.00元。（洗烘设备数量26台+13台=39台）\n" +
				"截止2021-05-31，每年费用61600.00元。（洗烘设备数量29台+15台=44台）";

		List<String> result = Lists.newArrayList();
		Pattern pattern = Pattern.compile("截止\\d{4}-\\d{2}-\\d{2}，每年费用([\\d\\.]+)元.*");
		Matcher matcher = pattern.matcher(str);

		for (int position = 0; ; ) {
			if (matcher.find()) {
				result.add(str.substring(position, matcher.end()));
				position = matcher.end();
			} else {
				result.add(str.substring(position));
				break;
			}
		}
		System.out.println(result);
	}

}
