package boluo.basics;

import org.junit.Test;

public class Note09_Lambda {

    /**
     * lambda语法： () -> {}  方法参数 -> 方法实现内容
     */
    @Test
    public void func1() {

        // 使用匿名内部类来创建线程
        new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("这是用匿名内部类创建的线程...");
            }
        }).start();

        // 使用lambda表达式创建线程
        new Thread(() -> System.out.println("这是用Lambda表达式创建的线程...")).start();
    }

    // 我们使用lambda的时候, 并不关心接口名, 方法名, 参数名
    // 只关注他的参数类型, 参数个数, 返回值


}
