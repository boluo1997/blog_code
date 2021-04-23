import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableList;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.StringType$;
import org.junit.Assert;
import org.junit.Test;
import scala.Tuple2;
import scala.Tuple3;
import work.Func;
import work.My;

import java.io.IOException;

public class FuncTest {

    private static final ObjectMapper mapper = new ObjectMapper();
    private final SparkSession spark = SparkSession.builder().master("local[*]")
            .config("spark.driver.host", "localhost")
            .getOrCreate();
    private final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

    @Test
    public void patchFilterTest() throws IOException {
        ArrayNode patch = mapper.createArrayNode();
        patch.addObject()
                .put("op", "replace")
                .put("path", "/child")
                .with("value")
                .put("foo1", "bar1")
                .put("foo2", "bar2")
                .with("foo3")
                .put("foo31", "bar31")
                .put("foo32", "bar32");

        patch.addObject()
                .put("op", "replace")
                .put("path", "/child2")
                .with("value")
                .put("foo1", "bar1")
                .put("foo2", "bar2")
                .with("foo3")
                .put("foo31", "bar31")
                .put("foo32", "bar32");

        patch.addObject()
                .put("op", "replace")
                .put("path", "")
                .with("value")
                .put("foo1", "bar1")
                .put("foo2", "bar2")
                .with("foo3")
                .put("foo31", "bar31")
                .put("foo32", "bar32");

        Dataset<String> ds = spark.createDataset(ImmutableList.of(patch.toString()), Encoders.STRING());

        JsonNode result = mapper.readTree(ds.select(Func.patchFilter("value", "/child/aaa", "/childfoo1"))
                .first().getString(0));
        Assert.assertEquals(result.size(), 0);
    }

    @Test
    public void fengtanTest3() {
        Dataset<Tuple3<String, String, Long>> ds = spark.createDataset(ImmutableList.of(
                Tuple3.apply("2020-04-01 07:00", null, null),
                Tuple3.apply("2020-04-01 08:00", "主营业务收入.收入.加液", 2L),
                Tuple3.apply("2020-04-02 08:00", "主营业务收入.收入.加液", 2L),	// [4.1, 1]
                Tuple3.apply("2020-04-03 08:00", "主营业务收入.收入.X", 4L),		// [4.2, 1]
                Tuple3.apply("2020-04-05 16:00", "主营业务收入.收入.X", 4L),		// [4.3, 1],[4.4, 1]
                Tuple3.apply("2020-04-06 08:00", "主营业务收入.收入.加液", 4L),	// [4.5, 1]
                Tuple3.apply("2020-04-07 08:00", "主营业务收入.收入.加液", 2L),	// [4.6, 1]
                Tuple3.apply("2020-04-09 07:00", null, null)					// [4.7, 1],[4.8, 1]
        ), Encoders.tuple(Encoders.STRING(), Encoders.STRING(), Encoders.LONG()));
        Dataset<Row> df = ds
                .withColumn("ex", My.分摊分成("timestamp(_1)", "_3", "_2",
                        "if(_2=null,null,9)", "null", "null", "date('2020-04-09')")
                        .over(Window.orderBy("_1")))
                .selectExpr("ex");
        df.show(false);
    }

}
