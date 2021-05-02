package boluo.datastructure.stack;

import java.util.Scanner;

public class StackTest1 {
    public static void main(String[] args) {
        conversion();
    }

    /**
     * 十进制转二进制
     * 用初始十进制数除以2, 把余数记下来, 若商不为0, 用商除以2, 直到商为0
     */
    public static void conversion() {
        Sqstacktp stack = new Sqstacktp();
        Scanner scanner = new Scanner(System.in);
        int n = scanner.nextInt();
        while (n != 0) {
            int result = n % 2;
            stack.push(n % 2);
            n = n/2;
        }

        System.out.println("the result is: ");
        while (!stack.isEmpty()) {
            System.out.print(stack.pop());
        }
        System.out.println();
    }

}
