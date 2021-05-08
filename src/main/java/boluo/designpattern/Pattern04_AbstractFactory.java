package boluo.designpattern;

public class Pattern04_AbstractFactory {

    // 工厂接口
    interface Factory {
        Phone createPhone(String str);
        Mask createMask(String str);
    }

    class SuperFactory implements Factory {

        @Override
        public Phone createPhone(String str) {
            return new IPhone();
        }

        @Override
        public Mask createMask(String str) {
            return new N95();
        }
    }

    // 产品A
    interface Phone {
    }

    class IPhone implements Phone {
    }

    // 产品B
    interface Mask {
    }

    class N95 implements Mask {
    }



    public void func1(String[] args) {
        SuperFactory factory = new SuperFactory();
        Phone phone = factory.createPhone("iPhone");
    }
}
