package boluo.designpattern;

import org.junit.Test;

/**
 * 简单工厂模式
 * 不够灵活, 每次新增一个产品, 都要
 */
public class Pattern02_SimpleFactory {

    public static Product createProduct(String str) {
        if (str.equals("Iphone")) {
            return new Iphone();
        } else {
            return new Samsung();
        }
    }

    abstract static class Product {

    }

    static class Iphone extends Product {

    }

    static class Samsung extends Product {

    }

    @Test
    public void func1() {
        Product p = Pattern02_SimpleFactory.createProduct("Iphone");
        System.out.println(p);
    }

}

