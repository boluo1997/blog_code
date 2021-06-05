package boluo.algorithm;

import java.util.Hashtable;

/**
 * 动态规划
 * 找出最长的递增子序列
 * [1, 5, 2, 4, 3]
 */
public class dynamicProgramming {

    // 用一个哈希表记录下从i开始的最长子序列长度
    static Hashtable<Integer, Integer> memo = new Hashtable<>();

    public static void main(String[] args) {
        int[] nums = new int[]{1, 5, 2, 4, 3};
        int maxLength = selectMaxSequence(nums, 0);
        System.out.println(maxLength);
    }

    // 返回从数组第i个数字开始最长的子序列长度 (i从0开始)
    public static int selectMaxSequence(int[] nums, int i) {
        if (i == nums.length - 1) return 1;
        if (memo.keySet().contains(i)) return memo.get(i);
        int maxLength = 1;
        for (int j = i + 1; j < nums.length; j++) {
            if (nums[j] > nums[i]) {
                maxLength = Math.max(maxLength, selectMaxSequence(nums, j) + 1);
            }
        }
        memo.put(i, maxLength);
        return maxLength;
    }
}


