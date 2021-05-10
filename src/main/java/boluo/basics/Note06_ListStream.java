package boluo.basics;

import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * list-stream操作
 */
public class Note06_ListStream {

    public static void main(String[] args) {

        List<User> list = Lists.newArrayList();
        // list.parallelStream() 并行流方法, 能够让数据集执行并行操作
        List<Integer> newList = list.stream().map(User::getAge).sorted().limit(10).collect(Collectors.toList());

    }

    public static List<User> func1(List<User> list) {
        list = list.stream()
                .filter(user -> {
                    return user.getAge() == 20;
                }).collect(Collectors.toList());
        return list;
    }

    /**
     * sorted() / sorted((T,T) -> int)
     * 如果流中的元素的类实现了 Comparable接口, 即有自己的排序规则, 那么可以直接调用sorted方法对元素进行排序
     * 反之, 需要调用sorted((T,T) -> int)实现 Comparator接口
     *
     * @param list
     * @return
     */
    public static List<User> func2(List<User> list) {
        // 根据年龄大小来比较
        list = list.stream()
                .sorted(Comparator.comparingInt(User::getAge))
                .collect(Collectors.toList());

        list = list.stream()
                .sorted((p1, p2) -> p2.getAge() - p1.getAge())
                .collect(Collectors.toList());

        return list;
    }

    public void func3() {

        // map 适用于一对一
        List<User> list = Lists.newArrayList();
        List<String> names = list.stream()
                // .map(user -> user.getName())
                .map(User::getName)
                .collect(Collectors.toList());

        // flatMap 适用于一对多
        // 由多个Integer类型组成的集合而成一个大集合
        List<List<Integer>> list1 = Arrays.asList(Arrays.asList(1), Arrays.asList(2, 3), Arrays.asList(4, 5, 6));

        // 由flatMap方法将list集合中的每一个元素变成流, 从而组成一个大的流, 并由collect方法连接起来
        List<Integer> result = list1.stream()
                // .flatMap(i -> i.stream())
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    private static class User {
        String name;
        int age;

        public User(String name, int age) {
            this.name = name;
            this.age = age;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        @Override
        public String toString() {
            return "User{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    '}';
        }
    }
}
