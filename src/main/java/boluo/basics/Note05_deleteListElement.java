package boluo.basics;

import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;

/**
 * 删除列表中所有偶数的元素
 */
public class Note05_deleteListElement {

    public static void main(String[] args) {
        List<Integer> list = Lists.newArrayList();
        for (int i = 0; i < 100; i++) {
            list.add(i);
        }
        // func1(list);
        // func2(list);
        func3(list);
        System.out.println(list);
    }

    /**
     * 新建一个集合删除
     */
    private static void func1(List<Integer> list) {
        List<Integer> removeList = Lists.newArrayList();
        for (Integer i : list) {
            if (i % 2 == 0) {
                removeList.add(i);
            }
        }
        list.removeAll(removeList);
    }

    /**
     * 使用下标删除
     */
    private static void func2(List<Integer> list) {
        for (int i = 0; i < list.size(); i++) {
            if (list.get(i) % 2 == 0) {
                list.remove(i--);
            }
        }
    }

    /**
     * 使用迭代器删除
     */
    private static void func3(List<Integer> list) {
        Iterator<Integer> it = list.iterator();
        while (it.hasNext()) {
            if (it.next() % 2 == 0) {
                it.remove();
            }
        }
    }
}
