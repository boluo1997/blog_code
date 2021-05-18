package boluo.basics;

import org.junit.Test;

public class Note10_BitOperation {

    @Test
    // 1.位运算实现乘除法
    public void func1() {
        // 将a向右移一位, 相当于将a除以2¹; a向左移n位, 相当于将a乘以2的n次幂
        int a = 4;
        System.out.println(a >> 2);
        System.out.println(a << 2);

    }

    @Test
    // 2.位运算交换两数
    public void func2() {
        // swap1(3, 4);

        // 第一步: a ^= b ---> a = (a^b);
        // 第二步: b ^= a ---> b = b^(a^b) ---> b = (b^b)^a = a
        // 第三步: a ^= b ---> a = (a^b)^a = (a^a)^b = b
        swap2(3, 4);
    }

    @Test
    // 3.位运算判断奇偶
    public void func3() {
        int a = 7;
        int b = 8;
        int c = 9;

        // a & 1 = 1, 则a为奇数
        System.out.println(a & 1);
        System.out.println(b & 1);
        System.out.println(c & 1);
    }

    @Test
    // 4.位运算交换符号
    public void func4() {
        // 将正数变成负数, 负数变成正数
        int a = 3;
        int b = -5;
        System.out.println(~a + 1);
        System.out.println(~b + 1);
    }

    @Test
    // 5.位运算求绝对值
    public void func5() {
        // 整数的绝对值是其本身, 负数的绝对值正好可以对其进行取反加一求得, 即我们首先判断其符号位
        // 整数右移 31 位得到 0, 负数右移 31 位得到 -1, 即 0xffffffff, 然后根据符号进行相应的操作
        int a = -7;
        int a_ = a >> 31;
        System.out.println(a_ == 0 ? a : ~a + 1);

        // 优化, 可以将 i == 0 的条件判断语句去掉。
        // 我们都知道符号位 i 只有两种情况, 即 i = 0 为正，i = -1 为负。
        // 对于任何数与 0 异或都会保持不变, 与 -1 即 0xffffffff 进行异或就相当于对此数进行取反,
        // 因此可以将上面三目元算符转换为((a^i)-i), 即整数时 a 与 0 异或得到本身, 再减去 0,
        // 负数时与 0xffffffff 异或将 a 进行取反, 然后在加上 1, 即减去 i(i =-1)
        int b = -9;
        int b1 = b >> 31;
        System.out.println((b ^ b1) - b1);
    }



    private void swap1(int a, int b) {
        a = a + b;
        b = a - b;
        a = a - b;
        System.out.println("a = " + a);
        System.out.println("b = " + b);
    }

    private void swap2(int a, int b) {
        a ^= b;
        b ^= a;
        a ^= b;
        System.out.println("a = " + a);
        System.out.println("b = " + b);
    }
}
