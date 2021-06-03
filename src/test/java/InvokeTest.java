import boluo.work.Qingliu;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableSet;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public class InvokeTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final SparkSession spark = SparkSession.builder().master("local[*]")
            .config("spark.driver.host", "localhost")
            .getOrCreate();

    @Test
    public void publicTest() {
        Method[] declaredMethods = Qingliu.class.getDeclaredMethods();
        Set<String> publicMethod = Arrays.stream(declaredMethods)
                .filter(i -> Modifier.isPublic(i.getModifiers()))
                .map(Method::getName)
                .collect(Collectors.toSet());

        Assert.assertEquals(ImmutableSet.of("replace"), publicMethod);
    }

}
