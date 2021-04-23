package algorithm;

import java.util.Arrays;
import java.util.Comparator;

/**
 * 贪心算法
 * 贪心算法 ( 贪心思想 ) 采用贪心的策略, 保证每次操作都是局部最优的,
 * 从而使最后得到的结果是全局最优的.
 */
public class greedyAlgorithm {
    public static void main(String[] args) {

    }

    /**
     * 1.### 分配问题
     * 有一群孩子和一堆饼干, 每个孩子有一个饥饿度, 每个饼干都有一个大小.
     * 每个孩子只能吃最多一个饼干, 且只有饼干的大小大于孩子的饥饿度时, 这个孩子才能吃饱.
     * 求最多有多少个孩子可以吃饱
     */

    /**
     *
     * @param children
     * @param cookies
     * @return
     *
     * 因为饥饿度最小的孩子最容易吃饱, 所以我们先考虑这个孩子. 为了尽量使得剩下的饼干可以满足饥饿度更大的孩子.
     * 我们应该把 >= 这个孩子饥饿度的、且大小最小的饼干给这个孩子. 满足了这个孩子之后, 我们采取同样的策略,
     * 考虑剩下孩子里饥饿度最小的孩子, 直到没有满足条件的饼干存在.
     *
     * 简而言之, 这里的贪心策略是: 给剩余孩子里最小饥饿度的孩子分配最小的能饱腹的饼干.
     * 具体实现, 因为我们要获得大小关系, 所以先把孩子和饼干排序. 再从饥饿度最小的孩子和大小最小的饼干出发, 计算有多少对能满足的条件
     */
    public int solution1(int[] children, int[] cookies){
        Arrays.sort(children);
        Arrays.sort(cookies);

        //能吃饱的孩子的数量
        int child = 0;
        int cookie = 0;

        //这里把child和cookie当作指针
        while(child < children.length && cookie < cookies.length){
            if(children[child] <= cookies[cookie++]){
                child++;
            }
        }

        return child;
    }

    /**
     * 2.### 区间问题
     * 给定多个区间, 让这些区间互不重叠所需要移除区间的最少个数. 起止相连不算重叠.
     *
     * 输入是一个数组, 数组由多个长度固定为2的数组组成, 表示区间的开始和结尾. 输出一个整数, 表示需要移除的区间数量
     * @input   [[1,2], [2,4], [1,3]]
     * @output  1
     * * 在这个样例中, 我们可以移除区间 [1,3], 使得剩余的区间 [[1,2], [2,4]]互不重叠
     */

    /**
     * 在选择要保留的区间时, 区间的结尾十分重要: 选择的区间结尾越小, 余留给其他区间的空间就越大, 就越能够保留更多的区间
     * 因此, 我们采取的贪心策略为, 优先保留结尾小且不相交的区间. 具体实现方法为, 先把区间按照结尾的大小进行增序排序,
     * 每次选择结尾最小且和前一个选择的区间不重叠的区间. 在样例中, 排序后的数组为[[1,2],[1,3],[2,4]].
     * 按照我们的贪心策略, 首先初始化为区间[1,2], 由于[1,3]与[1,2]相交, 我们跳过该区间, [2,4]与[1,2]不相交, 我们将其保留.
     * 因此最终保留的区间为[[1,2],[2,4]]
     */

    public static int solution2(int[][] inputs) {

        //按照区间结尾来排序
        Arrays.sort(inputs, new Comparator<int[]>() {
            @Override
            public int compare(int[] o1, int[] o2) {
                return o1[1] = o2[1];
            }
        });

        //满足条件的区间个数
        int count = 0;
        int pre = inputs[0][1];
        for (int i = 1; i < inputs.length; i++) {
            if (inputs[i][0] >= pre) {
                //可以选择的
                count++;
                pre = inputs[i][1];
            }
        }
        return inputs.length - count;
    }

    /**
     * 3.买卖股票的最佳时机
     *
     * 给定一个数组, 它的第i个元素是一支给定股票第i天的价格
     * 设计一个算法来计算你所能获取的最大利润, 你可以尽可能地完成更多的交易(多次买卖一支股票)
     * 注意: 你不能同时参与多笔交易 (你必须在再次购买前出售掉之前的股票)
     * @input   [7,1,5,3,6,4]
     * @output  7
     *
     * 解释:
     * 在第2天股票价格 = 1的时候买入, 在第3天股票价格 = 5的时候卖出, 这笔交易所能获得利润4
     * 在第4天股票价格 = 3的时候买入, 在第5天股票价格 = 6的时候卖出, 这笔交易所能获得利润3
     *
     * 这道题的贪心思想在于: 如果今天买明天卖可以赚钱, 那就买入
     */
    public static int solution3(int nums[]){
        int profit = 0;
        for(int i = 0; i < nums.length - 1; i++){
            if(nums[i] < nums[i+1]){
                profit += nums[i+1] - nums[i];
            }
        }
        return profit;
    }
}











