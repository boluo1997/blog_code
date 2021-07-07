import boluo.basics.Note02_Enum;
import boluo.basics.Note03_ThreadPoolManager;
import com.google.common.collect.Lists;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class boluoTest {

	private static final Logger logger = LoggerFactory.getLogger(boluoTest.class);

	@Test
	public void func1() {
		int areaType = 3;
		if (Note02_Enum.PREFECTURE.eq(areaType)) {

			logger.info(Note02_Enum.PREFECTURE.getName());
			Note03_ThreadPoolManager.run(() -> {
				// do something
			});
		}
	}

	@Test
	public void func2() {

		Integer in1 = 10;
		Integer in2 = 10;
		Integer in3 = 200;
		Integer in4 = 200;

		System.out.println(in1 == in2);
		System.out.println(in3 == in4);
		System.out.println(in3.equals(in4));
	}

	@Test
	public void func3() {
		int a = 7;
		int b = 8;
		int c = 9;

		// a & 1 = 1, 则a为奇数
		System.out.println(a & 1);
		System.out.println(b & 1);
		System.out.println(c & 1);

	}

	@Test
	public void func4() {
		// 字符串排序问题
		List<Integer> intList = Lists.newArrayList();
		intList.add(25);
		intList.add(3);
		intList.add(12);

		intList.sort((m, n) -> m - n);
		System.out.println(intList);

		List<String> stringList = Lists.newArrayList();
		stringList.add("25");
		stringList.add("D");
		stringList.add("3");

		stringList.sort((m, n) -> m.compareTo(n));
		System.out.println(stringList);
	}

	@Test
	public void func5() {
		// \u000d System.out.println("注释中的代码也会执行！！！");
	}


}
