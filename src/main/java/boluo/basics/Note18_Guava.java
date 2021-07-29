package boluo.basics;

import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import org.junit.Test;

import java.util.List;

public class Note18_Guava {

	// 连接器
	private static final Joiner joiner = Joiner.on(",").skipNulls();

	// 分割器
	private static final Splitter splitter = Splitter.on(",").trimResults().omitEmptyStrings();

	// 字符串匹配器
	private static final CharMatcher charMatcherDigit = CharMatcher.digit();
	private static final CharMatcher charMatcherAny = CharMatcher.any();

	@Test
	public void func1() {
		// 把集合, 数组中的元素join在一起
		String join = joiner.join(Lists.newArrayList("a", null, "b"));
		System.out.println("join = " + join);

		for (String tmp : splitter.split("a,  ,b,,")) {
			System.out.println("|" + tmp + "|");
		}
	}

	@Test
	public void func2() {
		// 只保留匹配的字符, 其他移除
		System.out.println(charMatcherDigit.retainFrom("qwertyuiopasdfghjkl"));

		// 移除匹配的字符
		System.out.println(charMatcherDigit.removeFrom("yeah I love u 1314"));

	}

	@Test
	public void func3() {
		// guava对jdk原生类型操作的扩展
		List<Integer> list = Ints.asList(1, 3, 5, 7, 9);

		System.out.println(Ints.join(",", 1, 3, 1, 4));

		// 原生类型数组快速合并
		int[] newIntArray = Ints.concat(new int[]{1, 2}, new int[]{2, 3, 4});
		System.out.println(newIntArray.length);

		// 最大/最小
		System.out.println(Ints.max(newIntArray) + "," + Ints.min(newIntArray));

		// 是否包含
		System.out.println(Ints.contains(newIntArray, 6));

		// 集合到数组的转换
		int[] toArray = Ints.toArray(list);

	}

}





