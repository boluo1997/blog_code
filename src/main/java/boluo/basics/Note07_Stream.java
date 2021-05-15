package boluo.basics;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Stream
 */
public class Note07_Stream {

    @Test
    public void func1() {
        // List转Stream
        List<String> list = Lists.newArrayList();
        list.add("dingc");
        Stream stream = list.stream();
        stream.forEach(System.out::println);
    }

    @Test
    public void func2() {
        // 数组转Stream
        String[] arr = new String[]{"boluo", "dingc", "qidai"};
        Stream stream = Arrays.stream(arr);
        stream.forEach(System.out::println);
    }

    @Test
    public void func3() {
        // Map转Stream
        // Map不是一个序列, 不是集合, 没办法直接转成stream(), 但entrySet()是Set, 可以转
        Map<String, Integer> map = Maps.newHashMap();
        map.put("boluo", 20);
        Stream stream1 = map.keySet().stream();
        Stream stream2 = map.values().stream();
        Stream stream3 = map.entrySet().stream();
        stream1.forEach(System.out::println);
        stream2.forEach(System.out::println);
        stream3.forEach(System.out::println);
    }

    @Test
    public void func4() {
        // 直接创建Stream
        Stream stream1 = Stream.of("boluo", 20);
        stream1.forEach(System.out::println);

        // Stream提供了iterate来生成一个无限序列, 一个基于初始值的无限序列, 可以用lambda设置序列的生成规则, 比如每次增加2
        Stream.iterate(0, n -> n + 2).limit(10).forEach(System.out::println);

        // 斐波那契数列
        Stream.iterate(new int[]{0, 1}, t -> new int[]{t[1], t[0] + t[1]})
                .limit(20)
                .map(t -> t[0])
                .forEach(System.out::println);

        // Stream还提供了另一个generate方法来生成序列。接收一个用户指定的生成序列函数IntSupplier.
        IntSupplier fib = new IntSupplier() {
            private int previous = 0;
            private int current = 1;

            @Override
            public int getAsInt() {
                int oldPrevious = this.previous;
                int nextValue = this.previous + this.current;
                this.previous = this.current;
                this.current = nextValue;
                return oldPrevious;
            }
        };
        IntStream.generate(fib).limit(10).forEach(System.out::println);

        String str = Stream.generate(() -> "?").limit(5).collect(Collectors.joining(","));
        System.out.println(str);
    }

}






