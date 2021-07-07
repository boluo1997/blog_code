package boluo.basics;

import boluo.algorithm.BubbleSort;
import boluo.work.FromQingliu2;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.Test;

import java.lang.reflect.Method;

public class Note15_Reflex {
	// 反射

	static class A {
		public void hello() {
			System.out.println("!!!");
		}
	}

//	public static void main(String[] args) throws Exception {
//		Class<?> clz = Class.forName("A");
//		Object obj = clz.newInstance();
//		Method m = clz.getDeclaredMethod("hello", null);
//		m.invoke(obj);
//	}


	@Test
	public void func1() throws Exception {

		int[] nums = {59, 27, 63, 45, 99, 1, 72, 13};

		Method bubbleSort = BubbleSort.class.getDeclaredMethod("bubbleSort", int[].class);
		// 解除私有限定
		bubbleSort.setAccessible(true);
		bubbleSort.invoke(null, nums);
		for (int i : nums) {
			System.out.println(i);
		}

	}
}
