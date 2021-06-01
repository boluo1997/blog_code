package boluo.basics;

import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;

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

    @Test
    public void func2(){
        // Consumer 一个入参, 无返回值
        Consumer<String> consumer = s -> System.out.println(s);
        consumer.accept("boluo");

        // Supplier 无入参, 有返回值
        Supplier supplier = () -> "boluo";
        String s = (String) supplier.get();
        System.out.println(s);

        // Function 一个入参, 一个返回值

        // Predicate 一个入参, 返回Boolean


    }

    @Test
    public void func3() {

//		如果声明 int value = 0 再加上以下代码后会编译错误
//    	Executors.newSingleThreadExecutor().submit(new Runnable() {
//			@Override
//			public void run() {
//				value++;
//			}
//		});

		AtomicInteger value = new AtomicInteger();
		IntStream.range(0, 10).forEach(i -> value.getAndIncrement());

    }


}
