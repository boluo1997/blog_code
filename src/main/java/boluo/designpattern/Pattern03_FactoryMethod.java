package boluo.designpattern;

/**
 * 工厂方法模式
 */
public class Pattern03_FactoryMethod {

    interface Iterator {
        // 产品接口
    }

    class ListItr implements Iterator {
        // 产品A
    }

    class Itr implements Iterator {
        // 产品B
    }

    interface List {
        // 工厂接口
        Iterator iterator();
    }

    class ArrayList implements List {
        // 子工厂, 具体实例化
        @Override
        public Iterator iterator() {
            return new Itr();
        }
    }

    class LinkedList implements List {
        // 子工厂, 具体实例化
        @Override
        public Iterator iterator() {
            return new ListItr();
        }
    }

    List list = new ArrayList();
    Iterator itr = list.iterator();
    // itr...
}
