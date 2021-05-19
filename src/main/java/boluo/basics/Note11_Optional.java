package boluo.basics;

import java.util.Optional;
import java.util.function.Predicate;

public class Note11_Optional {
    public static void main(String[] args) {
        Optional<Foo> optional = getFoo();
        optional.ifPresent(foo -> {
            System.out.println("有值..." + foo.getId());
        });

        // 设置默认值 orElse
        Integer fooId = getId();
        Integer id = Optional.ofNullable(fooId).orElse(4);
        System.out.println(id);

        // orElseGet
        Integer fooId2 = null;
        Integer id2 = Optional.ofNullable(fooId2).orElseGet(() -> 1);
        System.out.println(id2);

        String name = null;
        String name2 = Optional.ofNullable(name).orElse(getDefaultValue());
        String name3 = Optional.ofNullable(name).orElseGet(Note11_Optional::getDefaultValue);

        Predicate<String> len6 = pwd -> pwd.length() > 6;
        Predicate<String> len10 = pwd -> pwd.length() < 10;

        String password = "295111";
        Optional<String> opt = Optional.ofNullable(password);
        boolean br = opt.filter(pwd -> pwd.length() > 6).isPresent();
    }

    // 在foo为null的时候, 不调用
    public static Optional<Foo> getFoo() {
        boolean hasId = false;
        if (hasId) return Optional.of(new Foo(3, "password"));
        return Optional.empty();
    }

    public static Integer getId() {
        return null;
    }

    public static String getDefaultValue() {
        System.out.println("getDefaultValue...");
        return "boluo";
    }



    static class Foo {

        Integer id;

        String password;

        public Foo() {

        }

        public Foo(int id, String password) {
            this.id = id;
            this.password = password;
        }

        public Integer getId() {
            return id;
        }

        public String getPassword() {
            return password;
        }

    }
}
