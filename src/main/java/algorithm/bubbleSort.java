package algorithm;

public class bubbleSort {

    public static void main(String[] args) {
        int nums[] = new int[]{2, 8, 99, 15, 73, 65, 42, 37, 65};
        bubbleSort(nums);
        for (int i : nums) {
            System.out.println(i);
        }
    }

    public static void bubbleSort(int nums[]) {
        for (int i = 0; i < nums.length - 1; i++) {        //这层循环控制的是比较的轮数
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
