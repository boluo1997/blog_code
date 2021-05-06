package boluo.designpattern;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * 反射实现工厂模式
 */
public class Pattern02_SimpleFactoryReflection {

    private static final Map<EnumProductType, Class> activityIdMap = Maps.newHashMap();

    public static void addProductKey(EnumProductType EnumProduct, Class product) {
        activityIdMap.put(EnumProduct, product);
    }

    public static activityOne product(EnumProductType type) throws IllegalAccessException, InstantiationException {
        Class productClass = activityIdMap.get(type);
        return (activityOne) productClass.newInstance();
    }

    public static void main(String[] args) throws InstantiationException, IllegalAccessException {
        addProductKey(EnumProductType.activityOne, activityOne.class);
        activityOne product = product(EnumProductType.activityOne);
        System.out.println(product.toString());
    }

    public static class Product {
        private String name;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    public static class activityOne extends Product {
        private String store;

        @Override
        public String toString() {
            return "activityOne{" +
                    "store='" + store + '\'' +
                    '}';
        }
    }

    public static class activityTwo extends Product {
        private String store;
    }

    public enum EnumProductType {
        activityOne, activityTwo;
    }

}
