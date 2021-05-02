package boluo.algorithm;

public class SelectSort {

    public static void main(String[] args) {
        int nums[] = new int[]{2, 8, 99, 15, 73, 65, 42, 37, 65};
        selectSort(nums);
        for (int i : nums) {
            System.out.println(i);
        }
    }

    public static void selectSort(int nums[]) {

        for (int i = 0; i < nums.length - 1; i++) {
            for (int j = i + 1; j < nums.length; j++) {
                if (nums[i] > nums[j]) {
                    int temp = nums[i];
                    nums[i] = nums[j];
                    nums[j] = temp;
                }
            }
        }
    }
}
