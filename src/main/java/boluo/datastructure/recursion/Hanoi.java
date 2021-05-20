package boluo.datastructure.recursion;

public class Hanoi {
    public static void main(String[] args) {
        hanoi(3, "A盘", "B盘", "C盘");
    }

    public static void hanoi(int n, String s, String m, String e) {
        if (n == 1) {
            // 将编号为1的盘子从头柱移动到尾柱
            move(s, n, e);
        } else {
            // 将头柱s上的编号为1至n-1的盘子移动到中柱m, 尾柱e做辅助
            hanoi(n - 1, s, e, m);
            // 将编号为n的盘子从头柱s移动到尾柱e
            move(s, n, e);
            // 将中柱上的编号为1至n-1的盘子移动到尾柱e, 头柱s做辅助
            hanoi(n - 1, m, s, e);
        }
    }

    public static void move(String str1, int n, String str2) {
        System.out.println("将编号为" + n + "的盘子从" + str1 + "移动到" + str2);
    }
}
