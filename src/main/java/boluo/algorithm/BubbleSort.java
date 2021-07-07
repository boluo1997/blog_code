package boluo.algorithm;

public class BubbleSort {

	public static void main(String[] args) {
		int nums[] = new int[]{2, 8, 99, 15, 73, 65, 42, 37, 65};
		bubbleSort(nums);
		for (int i : nums) {
			System.out.println(i);
		}
	}

	public static void bubbleSort(int nums[]) {
		for (int i = 0; i < nums.length - 1; i++) {
			// 外层循环控制的是比较的轮数, 当外层循环走过一次之后, 最大的数会沉底
			// 所以内层循环剩下就不用再比较最后一个数
			// 以此类推, 内层循环每次少走nums.length -1 - i次

			for (int j = 0; j < nums.length - 1 - i; j++) {
				if (nums[j] > nums[j + 1]) {
					int temp = nums[j];
					nums[j] = nums[j + 1];
					nums[j + 1] = temp;
				}
			}
		}
	}
}
