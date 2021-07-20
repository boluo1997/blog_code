package boluo.basics;

import org.junit.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Note16_String {

	@Test
	public void func1() {
		// 分割字符串且保留切割符
		String str = "aaa";
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
		System.out.println(words);
	}
}
