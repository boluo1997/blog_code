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
    public void replaceTest1() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method replace = FromQingliu2.class.getDeclaredMethod("replace", ArrayNode.class, ArrayNode.class);
        replace.setAccessible(true);
        String randomKey = "key" + Math.round(Math.random() * 100);

        ObjectNode value1 = kv(0, 8, "20");
        ObjectNode patch1 = kv(0, 8, "30")
                .put(randomKey, randomKey);
        ObjectNode result1 = kv(0, 8, "30")
                .put(randomKey, randomKey);

        ArrayNode before = array(value1).deepCopy();
        Assert.assertEquals(array(result1), replace.invoke(null, before, array(patch1)));
        Assert.assertEquals(before, array(value1));

        ///////////////////////

        ObjectNode value2 = kvT(0, 2,
                kv(1, 8, "v1"), kv(2, 8, "v2"),
                kv(1, 8, "x1"), kv(2, 8, "x2"));
        ObjectNode patch2 = kvT(0, 1,
                kv(1, 8, "v1m"),
                kv(1, 8, "x1m"));
        ObjectNode result2 = kvT(0, 2,
                kv(1, 8, "v1m"), kv(2, 8, "v2"),
                kv(1, 8, "x1m"), kv(2, 8, "x2"));
        Assert.assertEquals(array(result1, result2), replace.invoke(null, array(value1, value2), array(patch1, patch2)));

        ///////////////////////

        ObjectNode value3 = kvT(0, 2,
                kv(1, 8, "v1"), kv(2, 8, "v2"),
                kv(1, 8, "x1"), kv(2, 8, "x2"));
        ObjectNode patch3 = kvT(0, 2,
                kv(1, 8, "v1m"), null,
                kvNull(1, 8), kvNull(2, 8));
        ObjectNode result3 = kvT(0, 2,
                kv(1, 8, "v1m"), kv(2, 8, "v2"));
        Assert.assertEquals(array(result3), replace.invoke(null, array(value3), array(patch3)));

        ///////////////////////
        // 增加行

        ObjectNode value4 = kvT(0, 2,
                kv(1, 8, "v1"), kv(2, 8, "v2"),
                kv(1, 8, "x1"), kv(2, 8, "x2"));
        ObjectNode patch4 = kvT(0, 2,
                kv(1, 8, "v1m"), null,
                kv(1, 8, "x1m"), null,
                kv(1, 8, "y1"), kv(2, 8, "y2"),
                kv(1, 8, "z1"), kv(2, 8, "z2"));
        ObjectNode result4 = kvT(0, 2,
                kv(1, 8, "v1m"), kv(2, 8, "v2"),
                kv(1, 8, "x1m"), kv(2, 8, "x2"),
                kv(1, 8, "y1"), kv(2, 8, "y2"),
                kv(1, 8, "z1"), kv(2, 8, "z2"));
        Assert.assertEquals(array(result4), replace.invoke(null, array(value4), array(patch4)));
    }

    // 删除测试
    @Test
    public void compareTest2() throws Throwable {
        Method replace = FromQingliu2.class.getDeclaredMethod("replace", ArrayNode.class, ArrayNode.class);
        replace.setAccessible(true);

        ObjectNode value1 = kv(1, 4, "20");

        // 删除
        ObjectNode patch2 = kvNull(1, 4);

        Assert.assertEquals(array(), invoke(replace, array(value1), array(patch2)));
    }

    @Test
    public void compareTest1() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method assertCompare = FromQingliu2.class.getDeclaredMethod("assertCompare", ArrayNode.class, ArrayNode.class);
        assertCompare.setAccessible(true);

        ObjectNode value1 = mapper.createObjectNode()
                .put("queId", 0)
                .put("queTitle", "编号")
                .put("queType", "8");
        value1.withArray("values")
                .addObject()
                .put("value", "20");

        ObjectNode value2 = mapper.createObjectNode()
                .put("queId", "0");
        value2.withArray("values")
                .addObject()
                .put("tableValue", "20")
                .put("value", "20");


        assertCompare.invoke(null, array(value1), array(value2));
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

        ///////////////////////
        // 比较table

        ObjectNode value4 = value1.deepCopy();
        value4.withArray("tableValues")
                .add(kv(186919, 4, "186919"))
                .add(kv(186920, 4, "186920"));
        ObjectNode value5 = value1.deepCopy();
        value5.withArray("tableValues")
                .add(kv(186920, 4, "186920"))
                .add(kv(186919, 4, "186919"));
        assertCompare.invoke(null, array(value4), array(value4));
        assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> {
            invoke(assertCompare, array(value1), array(value4));
        });
        assertThatNoException().isThrownBy(() -> {
            invoke(assertCompare, array(value4), array(value5));
        });
        ObjectNode value6 = value1.deepCopy();
        value6.withArray("tableValues")
                .add(kv(186920, 4, "186920"))
                .add(kv(186918, 4, "186918"));
        assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> {
            invoke(assertCompare, array(value4), array(value6));
        });

        ObjectNode valueL7 = value4.deepCopy();
        ObjectNode valueR7 = kvT(0, 3,
                kv(186920, 4, "186920aaa"),
                kv(186919, 4, "186919"),
                kv(186918, 4, "186918"));
        assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> {
            invoke(assertCompare, array(valueL7), array(valueR7));
        });

        ///////////////////////

        ObjectNode valueL8 = kvT(0, 2,
                kv(1, 8, "v1"), kv(2, 8, "v2"),
                kv(1, 8, "x1"), kv(2, 8, "x2"));
        ObjectNode valueR8 = kvT(0, 2,
                kv(1, 8, "v1"), kv(2, 8, "v2"),
                kv(1, 8, "x1"), kv(2, 8, "x2"));
        assertThatNoException().isThrownBy(() -> {
            invoke(assertCompare, array(valueL8), array(valueR8));
        });
    }

    @Test
    public void getPatchAnswerTest1() throws Throwable {
        Method getPatchAnswer = FromQingliu2.class.getDeclaredMethod("getPatchAnswer", ArrayNode.class, String.class, String.class);
        getPatchAnswer.setAccessible(true);
        ArrayNode result;

        ObjectNode value1 = mapper.createObjectNode()
                .put("queId", 1)
                .put("queType", 8);
        value1.set("b", kv(1, 8, "b1"));
        value1.set("a", kv(1, 8, "a1"));

        ObjectNode value2 = mapper.createObjectNode()
                .put("queId", 2)
                .put("queType", 8);
        value2.set("b", kv(2, 8, "b2"));
        value2.putNull("a");

        ObjectNode value3 = mapper.createObjectNode()
                .put("queId", 3)
                .put("queType", 8);
        value3.putNull("b");
        value3.set("a", kv(3, 8, "a3"));

        ObjectNode value4 = mapper.createObjectNode()
                .put("queId", 4)
                .put("queType", 8);
        value4.set("b", kv(4, 8, "b1"));
        value4.set("a", kv(4, 8));

        result = invoke(getPatchAnswer, array(value1, value2, value3, value4), "b", "b");
        assertThat(result.get(0)).isEqualTo(kv(1, 8, "b1"));
        assertThat(result.get(1)).isEqualTo(kv(2, 8, "b2"));
        assertThat(result).noneMatch(i -> i.at("/queId").asInt() == 3);

        result = invoke(getPatchAnswer, array(value1, value2, value3, value4), "b", "a");
        assertThat(result.get(0)).isEqualTo(kv(1, 8, "a1"));
        assertThat(result.get(1)).isEqualTo(kvNull(2, 8));
        assertThat(result.get(2)).isEqualTo(kv(3, 8, "a3"));
        assertThat(result).noneMatch(i -> i.at("/queId").asInt() == 4);

        ///////////////////////

        ObjectNode valuet4 = mapper.createObjectNode()
                .put("queId", 4)
                .put("queType", 18);
        valuet4.set("b", kvT(4, 2,
                kv(41, 8, "a41"), kv(42, 8),
                kv(41, 8, "b41"), kv(42, 8, "b42"),
                kv(41, 8, "c41"), kv(42, 8)
        ));
        valuet4.set("a", kvT(4, 2,
                kv(41, 8, "a41"), kv(42, 8, "a42"),
                kv(41, 8, "b41"), kv(42, 8, "b42")
        ));

        ObjectNode valuet5 = mapper.createObjectNode()
                .put("queId", 5)
                .put("queType", 18);
        valuet5.putNull("b");
        valuet5.set("a", kvT(5, 2,
                kv(41, 8, "a41"), kv(42, 8, "a42"),
                kv(41, 8, "a41"), kv(42, 8)
        ));

        ObjectNode valuet6 = mapper.createObjectNode()
                .put("queId", 6)
                .put("queType", 18);
        valuet6.set("b", kvT(6, 2,
                kv(41, 8, "a41"), kv(42, 8, "a42"),
                kv(41, 8, "a41"), kv(42, 8)
        ));
        valuet6.set("a", kvT(6, 2,
                kv(41, 8, "a41"), kv(42, 8, "a42"),
                kv(41, 8, "b41"), kv(42, 8),
                kv(41, 8, "c41"), kv(42, 8, "c42"),
                kv(41, 8, "d41"), kv(42, 8, "d42")
        ));

        ObjectNode valuet7 = mapper.createObjectNode()
                .put("queId", 7)
                .put("queType", 18);
        valuet7.set("b", kvT(7, 2,
                null, null,
                kv(41, 8, "a41"), kv(42, 8)
        ));
        valuet7.set("a", kvT(7, 2,
                kv(41, 8, "a41"), kv(42, 8),
                kv(41, 8, "b41"), kv(42, 8)
        ));

        result = invoke(getPatchAnswer, array(valuet4, valuet5, valuet6, valuet7), "b", "b");
        assertThat(result).containsOnlyOnce(kvT(4, 2,
                kv(41, 8, "a41"), null,
                kv(41, 8, "b41"), kv(42, 8, "b42"),
                kv(41, 8, "c41"), null
        ));
        assertThat(result).containsOnlyOnce(kvT(7, 2,
                null, null,
                kv(41, 8, "a41")
        ));
        assertThat(result).noneMatch(i -> i.at("/queId").asInt() == 5);

        result = invoke(getPatchAnswer, array(valuet4, valuet5, valuet6, valuet7), "b", "a");
        assertThat(result).containsOnlyOnce(kvT(4, 2,
                kv(41, 8, "a41"), kv(42, 8, "a42"),
                kv(41, 8, "b41"), kv(42, 8, "b42"),
                kvNull(41, 8), kvNull(42, 8)
        ));
        assertThat(result).containsOnlyOnce(kvT(5, 2,
                kv(41, 8, "a41"), kv(42, 8, "a42"),
                kv(41, 8, "a41"), kv(42, 8)
        ));
        assertThat(result).containsOnlyOnce(kvT(6, 2,
                kv(41, 8, "a41"), kv(42, 8, "a42"),
                kv(41, 8, "b41"), null,
                kv(41, 8, "c41"), kv(42, 8, "c42"),
                kv(41, 8, "d41"), kv(42, 8, "d42")
        ));
        assertThat(result).containsOnlyOnce(kvT(7, 2,
                kv(41, 8, "a41"), null,
                kv(41, 8, "b41"), null
        ));
    }

    private ArrayNode array(ObjectNode... obj) {
        ArrayNode answer = mapper.createArrayNode();
        answer.addAll(Arrays.asList(obj));
        return answer;
    }

    private ObjectNode kv(int key, int type, Object... value) {
        ObjectNode result = mapper.createObjectNode()
                .put("queId", key)
                .put("queType", type);
        ArrayNode values = result.withArray("values");
        for (Object v : value) {
            values.addObject()
                    .set("value", mapper.valueToTree(v));
        }
        return result;
    }

    private ObjectNode kvNull(int key, int type) {
        ObjectNode result = mapper.createObjectNode()
                .put("queId", key)
                .put("queType", type);
        result.withArray("values")
                .addObject()
                .putNull("value");
        return result;
    }

    private ObjectNode kvT(int key, int col, ObjectNode... value) {
        ObjectNode result = mapper.createObjectNode()
                .put("queId", key)
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
