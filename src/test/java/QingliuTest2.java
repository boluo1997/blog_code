import boluo.work.FromQingliu2;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Objects;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assertions.assertThat;

public class QingliuTest2 {

    private final ObjectMapper mapper = new ObjectMapper();

    // 替换测试
    @Test
    public void replaceTest1() throws Throwable {
        Method replace = FromQingliu2.class.getDeclaredMethod("replace", ArrayNode.class, ArrayNode.class);
        replace.setAccessible(true);
        String randomKey = "key" + Math.round(Math.random() * 100);
        String title = "title" + Math.round(Math.random() * 100);
        ArrayNode result;
        ObjectNode add = mapper.createObjectNode()
                .put("_op", "add");

        ObjectNode value1 = kv(0, title, 8, "20");
        ObjectNode patch1 = kv(0, title, 8, "30")
                .put(randomKey, randomKey);
        ObjectNode result1 = kv(0, title, 8, "30")
                .put(randomKey, randomKey);

        ArrayNode before = array(value1).deepCopy();
        Assert.assertEquals(array(result1), replace.invoke(null, before, array(patch1)));
        Assert.assertEquals(before, array(value1));

        ObjectNode value2 = kv(2, title, 4, "20");
        ObjectNode patch2 = kvNull(2, title, 4);
        Assert.assertEquals(array(), invoke(replace, array(value2), array(patch2)));

        ObjectNode patch3 = kvWithKV(3, title, 4, add, "20");
        Assert.assertEquals(array(kv(3, title, 4, "20")), invoke(replace, array(), array(patch3)));

        ObjectNode value4 = kv(3, title, 4, "20");
        ObjectNode patch4 = kvWithKV(3, title, 4, add, "30");
        Assert.assertEquals(array(kv(3, title, 4, "20", "30")), invoke(replace, array(value4), array(patch4)));
    }

    // 删除测试
    @Test
    public void replaceTest2() throws Throwable {
        Method replace = FromQingliu2.class.getDeclaredMethod("replace", ArrayNode.class, ArrayNode.class);
        replace.setAccessible(true);
        String title = "title";
        ArrayNode result;

        ///////////////////////

        ObjectNode value2 = kvT(0, title, 2,
                kv(1, title, 8, "v1"), kv(2, title, 8, "v2"),
                kv(1, title, 8, "x1"), kv(2, title, 8, "x2"));
        ObjectNode patch2 = kvT(0, title, 1,
                kv(1, title, 8, "v1m"),
                kv(1, title, 8, "x1m"));
        ObjectNode result2 = kvT(0, title, 2,
                kv(1, title, 8, "v1m"), kv(2, title, 8, "v2"),
                kv(1, title, 8, "x1m"), kv(2, title, 8, "x2"));
        Assert.assertEquals(array(result2), replace.invoke(null, array(value2), array(patch2)));

        ///////////////////////

        ObjectNode value3 = kvT(0, title, 2,
                kv(1, title, 8, "v1"), kv(2, title, 8, "v2"),
                kv(1, title, 8, "x1"), kv(2, title, 8, "x2"));
        ObjectNode patch3 = kvT(0, title, 2,
                kv(1, title, 8, "v1m"), null,
                kvNull(1, title, 8), kvNull(2, title, 8));
        ObjectNode result3 = kvT(0, title, 2,
                kv(1, title, 8, "v1m"), kv(2, title, 8, "v2"));
        Assert.assertEquals(array(result3), replace.invoke(null, array(value3), array(patch3)));

        ///////////////////////
        // 增加行

        ObjectNode value4 = kvT(4, title, 2,
                kv(1, title, 8, "v1"), kv(2, title, 8, "v2"),
                kv(1, title, 8, "x1"), kv(2, title, 8, "x2"));
        ObjectNode patch4 = kvT(4, title, 2,
                kv(1, title, 8, "v1m"), null,
                kv(1, title, 8, "x1m"), null,
                kv(1, title, 8, "y1"), kv(2, title, 8, "y2"),
                kv(1, title, 8, "z1"), kv(2, title, 8, "z2"));
        ObjectNode result4 = kvT(4, title, 2,
                kv(1, title, 8, "v1m"), kv(2, title, 8, "v2"),
                kv(1, title, 8, "x1m"), kv(2, title, 8, "x2"),
                kv(1, title, 8, "y1"), kv(2, title, 8, "y2"),
                kv(1, title, 8, "z1"), kv(2, title, 8, "z2"));

        // 修改1行
        ObjectNode value5 = kvT(5, title, 2,
                kv(1, title, 8, "a1"), kv(2, title, 8, "a2"),
                kv(1, title, 8, "b1"), kv(2, title, 8, "b2"));
        ObjectNode patch5 = kvT(5, title, 2,
                null, kv(2, title, 8, "a2m"));
        ObjectNode result5 = kvT(5, title, 2,
                kv(1, title, 8, "a1"), kv(2, title, 8, "a2m"),
                kv(1, title, 8, "b1"), kv(2, title, 8, "b2"));

        result = invoke(replace, array(value4, value5), array(patch4, patch5));
        Assert.assertEquals(array(result4, result5), result);

        ///////////////////////

        ObjectNode value6 = kvT(6, title, 2,
                kv(1, title, 8, "a1"), kv(2, title, 8, "a2"),
                kv(1, title, 8, "b1"), kv(2, title, 8, "b2"));
        ObjectNode patch6 = kvT(6, title, 2,
                kvNull(3, title, 8));
        ObjectNode result6 = kvT(6, title, 2,
                kv(1, title, 8, "a1"), kv(2, title, 8, "a2"),
                kv(1, title, 8, "b1"), kv(2, title, 8, "b2"));

        result = invoke(replace, array(value6), array(patch6));
        Assert.assertEquals(array(result6), result);

        ///////////////////////

        ObjectNode value7 = kvT(7, title, 2);
        ObjectNode patch7 = kvT(7, title, 2,
                kv(1, title, 8, "a1"));

        result = invoke(replace, array(value7), array(patch7));
        Assert.assertEquals(
                array(kvT(7, title, 2,
                        kv(1, title, 8, "a1"))),
                result);
    }

    @Test
    public void compareTest1() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method assertCompare = FromQingliu2.class.getDeclaredMethod("assertCompare", ArrayNode.class, ArrayNode.class);
        assertCompare.setAccessible(true);
        String title = "title";

        ObjectNode value1 = kv(0, title, 8, "20");

        ObjectNode value2 = mapper.createObjectNode()
                .put("queId", "0")
                .put("queTitle", title);
        value2.withArray("values")
                .addObject()
                .put("tableValue", "20")
                .put("value", "20");

        assertThatNoException().isThrownBy(() -> {
            assertCompare.invoke(null, array(value1), array(value2));
        });
        assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> {
            invoke(assertCompare, array(value1), array());
        });
        assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> {
            invoke(assertCompare, array(), array(value1));
        });

        ObjectNode value3 = mapper.createObjectNode()
                .put("queId", "3");
        value3.withArray("values")
                .addObject()
                .put("value", "20");
        assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> {
            invoke(assertCompare, array(value1), array(value3));
        });

        ObjectNode valueL4 = kv(4, title, 8, "20", "30");
        ObjectNode valueR4 = kv(4, title, 8, "20");
        assertThatNoException().isThrownBy(() -> {
            assertCompare.invoke(null, array(valueL4), array(valueL4));
        });
        assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> {
            invoke(assertCompare, array(valueL4), array(valueR4));
        });
        assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> {
            invoke(assertCompare, array(valueR4), array(valueL4));
        });

        ///////////////////////

        ObjectNode value5 = mapper.createObjectNode()
                .put("queId", "4")
                .put("queTitle", "")
                .put("queType", 8);
        assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> {
            invoke(assertCompare, array(value5), array(value5));
        });
        ObjectNode value6 = mapper.createObjectNode()
                .put("queId", "4")
                .putNull("queTitle")
                .put("queType", 8);
        assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> {
            invoke(assertCompare, array(value6), array(value6));
        });
        ObjectNode value7 = mapper.createObjectNode()
                .put("queId", "4")
                .put("queTitle", 1)
                .put("queType", 8);
        assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> {
            invoke(assertCompare, array(value7), array(value7));
        });

        ObjectNode valueL8 = kv(4, "1", 8, "20");
        ObjectNode valueR8 = kv(4, "2", 8, "20");
        assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> {
            invoke(assertCompare, array(valueL8), array(valueR8));
        });
    }

    @Test
    public void compareTest2() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method assertCompare = FromQingliu2.class.getDeclaredMethod("assertCompare", ArrayNode.class, ArrayNode.class);
        assertCompare.setAccessible(true);
        String title = "title";

        ObjectNode value1 = kv(0, title, 8, "20");

        ///////////////////////
        // 比较table

        ObjectNode value4 = value1.deepCopy();
        value4.withArray("tableValues")
                .add(kv(186919, title, 4, "186919"))
                .add(kv(186920, title, 4, "186920"));
        ObjectNode value5 = value1.deepCopy();
        value5.withArray("tableValues")
                .add(kv(186920, title, 4, "186920"))
                .add(kv(186919, title, 4, "186919"));
        assertCompare.invoke(null, array(value4), array(value4));
        assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> {
            invoke(assertCompare, array(value1), array(value4));
        });
        assertThatNoException().isThrownBy(() -> {
            invoke(assertCompare, array(value4), array(value5));
        });
        assertThatNoException().isThrownBy(() -> {
            invoke(assertCompare, array(value5), array(value4));
        });
        ObjectNode value6 = value1.deepCopy();
        value6.withArray("tableValues")
                .add(kv(186920, title, 4, "186920"))
                .add(kv(186918, title, 4, "186918"));
        assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> {
            invoke(assertCompare, array(value4), array(value6));
        });
        assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> {
            invoke(assertCompare, array(value6), array(value4));
        });

        ObjectNode valueL7 = value4.deepCopy();
        ObjectNode valueR7 = kvT(0, title, 3,
                kv(186920, title, 4, "186920aaa"),
                kv(186919, title, 4, "186919"),
                kv(186918, title, 4, "186918"));
        assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> {
            invoke(assertCompare, array(valueL7), array(valueR7));
        });
        assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> {
            invoke(assertCompare, array(valueR7), array(valueL7));
        });

        ///////////////////////

        ObjectNode valueL8 = kvT(0, title, 2,
                kv(1, title, 8, "v1"), kv(2, title, 8, "v2"),
                kv(1, title, 8, "x1"), kv(2, title, 8, "x2"));
        ObjectNode valueR8 = kvT(0, title, 2,
                kv(1, title, 8, "v1"), kv(2, title, 8, "v2"),
                kv(1, title, 8, "x1"), kv(2, title, 8, "x2"));
        assertThatNoException().isThrownBy(() -> {
            invoke(assertCompare, array(valueL8), array(valueR8));
        });
        assertThatNoException().isThrownBy(() -> {
            invoke(assertCompare, array(valueR8), array(valueL8));
        });

        ///////////////////////

        ObjectNode valueL9 = kvT(0, title, 2,
                kv(1, title, 8, "v1"), kv(2, title, 8));
        ObjectNode valueR9 = kvT(0, title, 2,
                kv(1, title, 8, "v1"));
        assertThatNoException().isThrownBy(() -> {
            invoke(assertCompare, array(valueL9), array(valueR9));
        });
        assertThatNoException().isThrownBy(() -> {
            invoke(assertCompare, array(valueR9), array(valueL9));
        });

        ///////////////////////

        ObjectNode valueL10 = kvT(0, title, 2,
                kv(1, title, 8, "v1a", "v1b"), kv(2, title, 8));
        ObjectNode valueR10 = kvT(0, title, 2,
                kv(1, title, 8, "v1a"));
        assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> {
            invoke(assertCompare, array(valueL10), array(valueR10));
        });
        assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> {
            invoke(assertCompare, array(valueR10), array(valueL10));
        });
    }

    @Test
    public void getPatchAnswerTest1() throws Throwable {
        Method getPatchAnswer = FromQingliu2.class.getDeclaredMethod("getPatchAnswer", ArrayNode.class, String.class, String.class);
        getPatchAnswer.setAccessible(true);
        String title = "title";
        ArrayNode result;
        ObjectNode kv1 = mapper.createObjectNode()
                .put("k1", "v1");
        ObjectNode kv2 = mapper.createObjectNode()
                .put("k2", "v2");
        ObjectNode add = mapper.createObjectNode()
                .put("_op", "add");

        ObjectNode value1 = mapper.createObjectNode()
                .put("queId", 1)
                .put("queType", 8);
        value1.set("b", kv(1, title, 8, "b1"));
        value1.set("a", kv(1, title, 8, "a1"));

        ObjectNode value2 = mapper.createObjectNode()
                .put("queId", 2)
                .put("queType", 8);
        value2.set("b", kv(2, title, 8, "b2"));
        value2.putNull("a");

        ObjectNode value3 = mapper.createObjectNode()
                .put("queId", 3)
                .put("queType", 8);
        value3.putNull("b");
        value3.set("a", kv(3, title, 8, "a3"));

        ObjectNode value4 = mapper.createObjectNode()
                .put("queId", 4)
                .put("queType", 8);
        value4.set("b", kv(4, title, 8, "b1"));
        value4.set("a", kv(4, title, 8));

        result = invoke(getPatchAnswer, array(value1, value2, value3, value4), "b", "b");
        assertThat(result).containsExactlyInAnyOrder(
                kv(1, title, 8, "b1"),
                kv(2, title, 8, "b2"),
                kv(4, title, 8, "b1")
        );

        result = invoke(getPatchAnswer, array(value1, value2, value3, value4), "b", "a");
        assertThat(result).containsExactlyInAnyOrder(
                kv(1, title, 8, "a1"),
                kvNull(2, title, 8),
                kvWithKV(3, title, 8, add, "a3")
        );

        ///////////////////////

        ObjectNode value5 = mapper.createObjectNode()
                .put("queId", 5)
                .put("queType", 8);
        value5.set("b", kvWithKV(5, title, 8, kv1, "b1"));
        value5.set("a", kvWithKV(5, title, 8, kv2, "b1m"));
        result = invoke(getPatchAnswer, array(value5), "b", "b");
        assertThat(result).containsExactly(
                kvWithKV(5, title, 8, kv1, "b1")
        );
        result = invoke(getPatchAnswer, array(value5), "b", "a");
        assertThat(result).containsExactly(
                kvWithKV(5, title, 8, kv2, "b1m")
        );

    }

    @Test
    public void getPatchAnswerTest2() throws Throwable {
        Method getPatchAnswer = FromQingliu2.class.getDeclaredMethod("getPatchAnswer", ArrayNode.class, String.class, String.class);
        getPatchAnswer.setAccessible(true);
        String title = "title";
        ArrayNode result;
        ObjectNode kv1 = mapper.createObjectNode()
                .put("k1", "v1");
        ObjectNode kv2 = mapper.createObjectNode()
                .put("k1", "v1");

        ObjectNode valuet4 = mapper.createObjectNode()
                .put("queId", 4)
                .put("queType", 18);
        valuet4.set("b", kvT(4, title, 2,
                kvWithKV(41, title, 8, kv1, "a41"), kv(42, title, 8),
                kv(41, title, 8, "b41"), kv(42, title, 8, "b42"),
                kv(41, title, 8, "c41"), kv(42, title, 8)
        ));
        valuet4.set("a", kvT(4, title, 2,
                kvWithKV(41, title, 8, kv2, "a41"), kv(42, title, 8, "a42"),
                kv(41, title, 8, "b41"), kv(42, title, 8, "b42")
        ));

        ObjectNode valuet5 = mapper.createObjectNode()
                .put("queId", 5)
                .put("queType", 18);
        valuet5.putNull("b");
        valuet5.set("a", kvT(5, title, 2,
                kv(41, title, 8, "a41"), kv(42, title, 8, "a42"),
                kv(41, title, 8, "a41"), kv(42, title, 8)
        ));

        ObjectNode valuet6 = mapper.createObjectNode()
                .put("queId", 6)
                .put("queType", 18);
        valuet6.set("b", kvT(6, title, 2,
                kv(41, title, 8, "a41"), kv(42, title, 8, "a42"),
                kv(41, title, 8, "a41"), kv(42, title, 8)
        ));
        valuet6.set("a", kvT(6, title, 2,
                kv(41, title, 8, "a41"), kv(42, title, 8, "a42"),
                kv(41, title, 8, "b41"), kv(42, title, 8),
                kv(41, title, 8, "c41"), kv(42, title, 8, "c42"),
                kv(41, title, 8, "d41"), kv(42, title, 8, "d42")
        ));

        ObjectNode valuet7 = mapper.createObjectNode()
                .put("queId", 7)
                .put("queType", 18);
        valuet7.set("b", kvT(7, title, 2,
                null, null,
                kv(41, title, 8, "a41"), kv(42, title, 8)
        ));
        valuet7.set("a", kvT(7, title, 2,
                kv(41, title, 8, "a41"), kv(42, title, 8),
                kv(41, title, 8, "b41"), kv(42, title, 8)
        ));

        ObjectNode valuet8 = mapper.createObjectNode()
                .put("queId", 8)
                .put("queType", 18);
        valuet8.set("b", kvT(8, title, 2,
                kv(41, title, 8, "a41"), kv(42, title, 8, "a42")
        ));
        valuet8.set("a", kvT(8, title, 2,
                kv(41, title, 8, "a41m"), kv(42, title, 8)
        ));

        result = invoke(getPatchAnswer, array(valuet4, valuet5, valuet6, valuet7, valuet8), "b", "b");
        assertThat(result).containsOnlyOnce(kvT(4, title, 2,
                kvWithKV(41, title, 8, kv1, "a41"), null,
                kv(41, title, 8, "b41"), kv(42, title, 8, "b42"),
                kv(41, title, 8, "c41"), null
        ));
        assertThat(result).containsOnlyOnce(kvT(7, title, 2,
                null, null,
                kv(41, title, 8, "a41")
        ));
        assertThat(result).noneMatch(i -> i.at("/queId").asInt() == 5);

        result = invoke(getPatchAnswer, array(valuet4, valuet5, valuet6, valuet7, valuet8), "b", "a");
        assertThat(result).containsOnlyOnce(kvT(4, title, 2,
                kvWithKV(41, title, 8, kv2, "a41"), kv(42, title, 8, "a42"),
                kv(41, title, 8, "b41"), kv(42, title, 8, "b42"),
                kvNull(41, title, 8), kvNull(42, title, 8)
        ));
        assertThat(result).containsOnlyOnce(kvT(5, title, 2,
                kv(41, title, 8, "a41"), kv(42, title, 8, "a42"),
                kv(41, title, 8, "a41"), null
        ));
        assertThat(result).containsOnlyOnce(kvT(6, title, 2,
                kv(41, title, 8, "a41"), kv(42, title, 8, "a42"),
                kv(41, title, 8, "b41"), null,
                kv(41, title, 8, "c41"), kv(42, title, 8, "c42"),
                kv(41, title, 8, "d41"), kv(42, title, 8, "d42")
        ));
        // 与顺序有关
        assertThat(result).containsOnlyOnce(kvT(7, title, 2,
                kv(41, title, 8, "a41"), null,
                kv(41, title, 8, "b41"), null
        ));
		/*ObjectNode expect7 = kvT(7, title, 2,
				kv(41, title, 8, "a41"), null,
				kv(41, title, 8, "b41"), null
		);
		assertThat(result).filteredOn(i -> i.at("/queId").asInt() == 7)
				.hasSize(1)
				.singleElement()
				.matches(i -> {
					assertThat(i.at("/tableValues")).containsExactlyInAnyOrderElementsOf(expect7.at("/tableValues"));
					return true;
				});*/
        assertThat(result).containsOnlyOnce(kvT(8, title, 2,
                kv(41, title, 8, "a41m"), kv(42, title, 8)
        ));

        ///////////////////////

        ObjectNode valuet9 = mapper.createObjectNode()
                .put("queId", 9)
                .put("queType", 18);
        valuet9.set("b", kvT(9, title, 2,
                kv(41, title, 8), kv(42, title, 8, "a42"),
                null, null,
                kv(41, title, 8), kv(42, title, 8, "c42"),
                null, null,
                kv(41, title, 8), kv(42, title, 8, "e42")
        ));
        valuet9.set("a", kvT(9, title, 2,
                kv(41, title, 8, "a41m"), kv(42, title, 8)
        ));
        result = invoke(getPatchAnswer, array(valuet9), "b", "b");
        assertThat(result).containsExactlyInAnyOrder(kvT(9, title, 1,
                kv(42, title, 8, "a42"),
                null,
                kv(42, title, 8, "c42"),
                null,
                kv(42, title, 8, "e42")
        ));

        ///////////////////////

        ObjectNode valuet10 = mapper.createObjectNode()
                .put("queId", 10)
                .put("queType", 18);
        valuet10.set("b", kvT(10, title, 2,
                kv(41, title, 8), kv(42, title, 8, "a42")
        ));
        valuet10.putNull("a");
        result = invoke(getPatchAnswer, array(valuet10), "b", "a");
        assertThat(result).hasSize(0);

        ///////////////////////

        ObjectNode valuet11 = mapper.createObjectNode()
                .put("queId", 11)
                .put("queType", 18);
        valuet11.set("b", kvT(11, title, 2,
                kv(41, title, 8, "a41")
        ));
        valuet11.set("a", kvT(11, title, 2,
                kv(41, title, 8, "a41"), kv(42, title, 8, "a42")
        ));
        result = invoke(getPatchAnswer, array(valuet11), "b", "a");
        assertThat(result).containsExactlyInAnyOrder(kvT(11, title, 2,
                kv(41, title, 8, "a41"), kv(42, title, 8, "a42")
        ));
    }

    private ArrayNode array(ObjectNode... obj) {
        ArrayNode answer = mapper.createArrayNode();
        answer.addAll(Arrays.asList(obj));
        return answer;
    }

    private ObjectNode kv(int key, String title, int type, Object... value) {
        ObjectNode result = mapper.createObjectNode()
                .put("queId", key)
                .put("queTitle", title)
                .put("queType", type);
        ArrayNode values = result.withArray("values");
        for (Object v : value) {
            values.addObject()
                    .set("value", mapper.valueToTree(v));
        }
        return result;
    }

    private ObjectNode kvWithKV(int key, String title, int type, ObjectNode kv, Object... value) {
        ObjectNode result = mapper.createObjectNode()
                .put("queId", key)
                .put("queTitle", title)
                .put("queType", type);
        result.setAll(kv);
        ArrayNode values = result.withArray("values");
        for (Object v : value) {
            values.addObject()
                    .set("value", mapper.valueToTree(v));
        }
        return result;
    }

    private ObjectNode kvNull(int key, String title, int type) {
        ObjectNode result = mapper.createObjectNode()
                .put("queId", key)
                .put("queTitle", title)
                .put("queType", type);
        result.withArray("values")
                .addObject()
                .putNull("value");
        return result;
    }

    private ObjectNode kvT(int key, String title, int col, ObjectNode... value) {
        ObjectNode result = mapper.createObjectNode()
                .put("queId", key)
                .put("queTitle", title)
                .put("queType", 18);
        ArrayNode tableValues = result.withArray("tableValues");
        ArrayNode row = mapper.createArrayNode();
        int count = -1;
        for (ObjectNode v : value) {
            count += 1;
            if (Objects.isNull(row) || count % col == 0) {
                row = tableValues.addArray();
            }
            if (Objects.isNull(v)) {
                continue;
            }
            ObjectNode t = v.deepCopy();
            for (JsonNode i : t.at("/values")) {
                if (i.isObject()) {
                    ((ObjectNode) i).put("ordinal", count / col);
                }
            }
            row.add(t);
        }
        // 删除空行
        for (int i = 0; i < tableValues.size(); ++i) {
            if (tableValues.get(i).size() == 0) {
                tableValues.remove(i--);
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private <T> T invoke(Method method, Object... args) throws Throwable {
        try {
            return (T) method.invoke(null, args);
        } catch (InvocationTargetException ex) {
            throw ex.getTargetException();
        } catch (Exception e) {
            throw e;
        }
    }
}
