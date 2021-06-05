package boluo.algorithm;

/**
 * 动态规划
 * 找出最长的递增子序列
 * [1, 5, 2, 4, 3]
 */
public class dynamicProgramming {
    public static void main(String[] args) {
        int[] nums = new int[]{1, 5, 2, 4, 3, 5};
        int maxLength = selectMaxSequence(nums, 0);
        System.out.println(maxLength);
    }

    // 返回从数组第i个数字开始最长的子序列长度 (i从0开始)
    public static int selectMaxSequence(int[] nums, int i) {
        if (i == nums.length - 1) return 1;
        int maxLength = 1;
        for (int j = i + 1; j < nums.length; j++) {
            if (nums[j] > nums[i]) {
                maxLength = Math.max(maxLength, selectMaxSequence(nums, j) + 1);
            }
        }
        return maxLength;
    }
}


