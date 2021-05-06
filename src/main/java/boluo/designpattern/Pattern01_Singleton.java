package boluo.designpattern;

/**
 * 单例模式:
 * 确保一个类只有一个实例, 而且自行实例化并向整个系统提供这个实例
 */
public class Pattern01_Singleton {

    private static Pattern01_Singleton singleton;

    private Pattern01_Singleton() {
        
    }

    public static Pattern01_Singleton getInstance() {
        return singleton;
    }
}

/**
 * 饿汉式
 * 在类加载的时候就进行实例化
 */
class HungrySingleton {

    private static HungrySingleton singleton = new HungrySingleton();

    private HungrySingleton() {
    }

    public static HungrySingleton getInstance() {
        return singleton;
    }
}

/**
 * 懒汉式
 * 在类加载的时候不进行实例化, 在第一次使用的时候进行实例化
 */
class LazySingleton {
    // 线程不安全
    private static LazySingleton singleton;

    private LazySingleton() {
    }

    public static LazySingleton getInstance() {
        if (singleton == null) {
            singleton = new LazySingleton();
        }
        return singleton;
    }
}

/**
 * 双重检查锁
 * 加锁, 防止对象被多次实例化
 * 以上代码中, 我们同步处理的处理范围是整个方法, 但是我们的实例化仅仅发生在第一次,
 * 一旦被实例化, 下次判断便不可能为空, 就不会再实例化, 但是我们还会执行同步块,
 * 为了提高效率, 我们可以减小同步的范围, 即在第一次判断为空时, 我们再加锁
 */
class DoubleCheckSingleton {
    private volatile static DoubleCheckSingleton singleton;

    private DoubleCheckSingleton() {
    }

    public static DoubleCheckSingleton getInstance() {
        if (singleton == null) {
            synchronized (DoubleCheckSingleton.class) {
                if (singleton == null) {
                    singleton = new DoubleCheckSingleton();
                }
            }
        }
        return singleton;
    }
}






