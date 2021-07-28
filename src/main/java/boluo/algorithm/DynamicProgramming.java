package boluo.algorithm;

import java.util.Hashtable;

/**
 * 动态规划
 */
public class DynamicProgramming {

    // 用一个哈希表记录下从i开始的最长子序列长度
    static Hashtable<Integer, Integer> memo = new Hashtable<>();

    public static void main(String[] args) {

        int[] nums = new int[]{1, 5, 2, 4, 3};
        int maxLength = selectMaxSequence(nums, 0);
        System.out.println(maxLength);

        int[][] arr = {{7}, {3, 8}, {8, 1, 0}, {2, 7, 4, 4}, {4, 5, 2, 6, 5}};
        System.out.println(getMaxPath(arr));
    }

    /**
     * 1.找出最长的递增子序列
     * 返回从数组第i个数字开始最长的子序列长度 (i从0开始)
     */
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

    /**
     * 2.求最大的数字之和
     * 给出如下三角形
     * 7
     * 3 8
     * 8 1 0
     * 2 7 4 4
     * 4 5 2 6 5
     * 从三角形中寻找一条从顶部到达底部的路径, 使得路径上经过的数字之和最大, 路径上的每一步都只能往左下或者右下走
     * 求出最大的数字之和, 样例答案为30 (7->3->8->7->5)
     */
    // 定义一个状态f(i,j): 表示在第i层第j个数字的时候, 满足条件最大的路径和为多少
    // 即: f(i,j) = arr[i][j] + max(f(i+1,j), f(i+1,j+1));
    public static int maxSumPath(int i, int j, int[][] arr) {
        if (i == arr.length - 1) return arr[i][j];
        return arr[i][j] + Math.max(maxSumPath(i + 1, j, arr), maxSumPath(i + 1, j + 1, arr));
    }

    public static int getMaxPath(int[][] arr) {
        int[][] dp = new int[arr.length][arr.length];
        for (int i = 0; i < arr.length; i++) {
            dp[arr.length - 1][i] = arr[arr.length - 1][i];
        }
        for (int i = arr.length - 2; i >= 0; i--) {
            for (int j = 0; j <= i; j++) {
                dp[i][j] = arr[i][j] + Math.max(dp[i + 1][j], dp[i + 1][j + 1]);
            }
        }
        return dp[0][0];
    }
}


