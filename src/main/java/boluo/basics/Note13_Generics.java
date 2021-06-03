package boluo.basics;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.List;

// 泛型
// ?: 不确定的Java类型
// T(type): 表示具体的一个Java类型
// K V: 代表键值对
// E: 代表元素
public class Note13_Generics<T> {
    private T t;

    public void set(T t) {
        this.t = t;
    }

    public T get() {
        return t;
    }

    // 不指定类型
    public void noSpecifyType() {
        Note13_Generics generics = new Note13_Generics();
        generics.set("test");

        // 接收时需要强转
        String result = (String) generics.get();
        System.out.println(result);
    }

    // 指定类型
    public void specifyType() {
        Note13_Generics<String> generics1 = new Note13_Generics<>();
        Note13_Generics<Integer> generics2 = new Note13_Generics<>();

        generics1.set("test");
        generics2.set(29511);

        // 不需要强制类型转换, 可在编译时检查类型安全, 可以用在类, 方法, 接口上
        String result1 = generics1.get();
        Integer result2 = generics2.get();
        System.out.println(result1 + ", " + result2);
    }

}
