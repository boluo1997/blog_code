package boluo.basics;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Note20_HashMap {

	@Test
	public void func1() {
		// HashMap初始化
		HashMap<String, String> map = new HashMap<>();
		map.put("name", "dingc");
		map.put("phone", "18831086282");
	}

	@Test
	public void func2() {
		// 第一个{}是定义了一个匿名内部类, 第二个{}是一个实例初始化块
		HashMap<String, String> map = new HashMap<String, String>() {{
			put("name", "dingc");
			put("phone", "18831086282");
		}};
		System.out.println(map);
	}

	@Test
	public void func3() {
		List<String> names = new ArrayList<String>() {{
			for (int i = 0; i < 10; i++) {
				add("A" + i);
			}
		}};
		System.out.println(names);
	}

}



