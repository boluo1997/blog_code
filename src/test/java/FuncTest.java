import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableList;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Test;
import scala.Tuple2;
import scala.Tuple3;
import boluo.work.Func;
import boluo.work.My;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import javax.ws.rs.HttpMethod;

import static boluo.work.My.*;

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
        JsonNode result = mapper.readTree(ds.select(patchFilter("value", "/child/foo1"))
                .first().getString(0));
        assertEquals(result.size(), 1);
        assertEquals(result.at("/0/value").size(), 1);
        assertEquals(result.at("/0/value/foo1"), TextNode.valueOf("bar1"));

        result = mapper.readTree(ds.select(patchFilter("value", "/child/foo3", "/child/foo3/foo31"))
                .first().getString(0));
        assertEquals(result.size(), 1);
        assertEquals(result.at("/0/value").size(), 1);
        assertEquals(result.at("/0/value/foo3").size(), 1);
        assertEquals(result.at("/0/value/foo3/foo31"), TextNode.valueOf("bar31"));

        result = mapper.readTree(ds.select(patchFilter("value", "/child/foo1", "/child/foo2"))
                .first().getString(0));
        assertEquals(result.size(), 1);
        assertEquals(result.at("/0/value").size(), 2);
        assertEquals(result.at("/0/value/foo1"), TextNode.valueOf("bar1"));
        assertEquals(result.at("/0/value/foo2"), TextNode.valueOf("bar2"));

        result = mapper.readTree(ds.select(patchFilter("value", "/foo1", "/foo2"))
                .first().getString(0));
        assertEquals(result.size(), 1);
        assertEquals(result.at("/0/value").size(), 2);
        assertEquals(result.at("/0/value/foo1"), TextNode.valueOf("bar1"));
        assertEquals(result.at("/0/value/foo2"), TextNode.valueOf("bar2"));

        result = mapper.readTree(ds.select(patchFilter("value", "/child1"))
                .first().getString(0));
        assertEquals(result.size(), 0);

        result = mapper.readTree(ds.select(patchFilter("value", "/child/abs", "/childfoo1"))
                .first().getString(0));
        assertEquals(result.size(), 0);
    }

    @Test
    public void patchFilterTest2() throws IOException {
        ArrayNode patch = mapper.createArrayNode();
        patch.addObject()
                .put("op", "remove")
                .put("path", "/name");
        patch.addObject()
                .put("op", "remove")
                .put("path", "/code");
        patch.addObject()
                .put("op", "remove")
                .put("path", "/foo/bar");
        patch.addObject()
                .put("op", "remove")
                .put("path", "/foo/bar1");

        Dataset<String> ds = spark.createDataset(ImmutableList.of(patch.toString()), Encoders.STRING());
        JsonNode result = mapper.readTree(ds.select(patchFilter("value", "/foo"))
                .first().getString(0));
        assertEquals(result.size(), 2);

        result = mapper.readTree(ds.select(patchFilter("value", "/name/first"))
                .first().getString(0));
        assertEquals(result.size(), 1);
        assertEquals(result.at("/0/path"), TextNode.valueOf("/name"));
    }

    @Test
    public void patchFilterTest3() throws IOException {
        ArrayNode patch = mapper.createArrayNode();
        patch.addObject()
                .put("op", "add")
                .put("path", "")
                .with("value")
                .put("foo1", "bar1")
                .put("foo2", "bar2")
                .with("foo3")
                .put("foo31", "bar31")
                .put("foo32", "bar32");

        Dataset<String> ds = spark.createDataset(ImmutableList.of(patch.toString()), Encoders.STRING());
        JsonNode result = mapper.readTree(ds.select(patchFilter("value", "/foo1"))
                .first().getString(0));
        assertEquals(result.size(), 1);
        assertEquals(result.at("/0/value").size(), 1);
        assertEquals(result.at("/0/op"), TextNode.valueOf("add"));
        assertEquals(result.at("/0/value/foo1"), TextNode.valueOf("bar1"));

        result = mapper.readTree(ds.select(patchFilter("value", "/foo3", "/foo3/foo31"))
                .first().getString(0));
        assertEquals(result.size(), 1);
        assertEquals(result.at("/0/value").size(), 1);
        assertEquals(result.at("/0/value/foo3").size(), 1);
        assertEquals(result.at("/0/value/foo3/foo31"), TextNode.valueOf("bar31"));

        result = mapper.readTree(ds.select(patchFilter("value", "/foo1", "/foo2"))
                .first().getString(0));
        assertEquals(result.size(), 1);
        assertEquals(result.at("/0/value").size(), 2);
        assertEquals(result.at("/0/value/foo1"), TextNode.valueOf("bar1"));
        assertEquals(result.at("/0/value/foo2"), TextNode.valueOf("bar2"));
    }

    @Test
    public void mergeTest1() {
        Dataset<Row> ds = spark.createDataset(ImmutableList.of(
                Tuple2.apply("a1", 1),
                Tuple2.apply("a2", null),
                Tuple2.apply(null, 2)
        ), Encoders.tuple(Encoders.STRING(), Encoders.INT())).toDF()
                .selectExpr("struct(_1,_2) a")
                .withColumn("id", monotonically_increasing_id());

        ds.show(false);
        List<Row> result = ds
                .select(merge("a", ds.select("a.*").schema()).over(Window.orderBy("id")).as("a"))
                .selectExpr("a.*")
                .collectAsList();
        System.out.println(result);
        Assert.assertEquals("a2", result.get(1).apply(0));
        Assert.assertEquals(1, result.get(1).apply(1));
        Assert.assertEquals("a2", result.get(2).apply(0));
        Assert.assertEquals(2, result.get(2).apply(1));
    }

    @Test
    public void fengtanErrorTest3() {
        Dataset<Row> src = spark.createDataset(ImmutableList.of(
                RowFactory.create("2019-12-30 08:00", "主营业务收入.收入.加液", 2L, null, null, null, null),
                RowFactory.create("2020-01-01 08:00", "主营业务收入.收入.加液", 2L, null, null, null, null)
        ), RowEncoder.apply(new StructType()
                .add("ts", "string")
                .add("科目", "string")
                .add("金额", "long")
                .add("固定", "long")
                .add("比例1", "double")
                .add("比例2", "double")
                .add("结束日", "string")));

        Dataset<Row> df = src
                .union(spark.sql("select '2019-12-30 07:00', null, null, 4, 0.4, 0.8, '2020-04-09 07:00'"))
                .union(spark.sql("select '2020-04-09 07:00', null, null, null, null, null, '2020-04-09 07:00'"))
                .withColumn("ex", My.分摊分成(
                        "timestamp(ts)", "`金额`", "`科目`",
                        "`固定`", "`比例1`", "`比例2`", "timestamp(`结束日`)")
                        .over(Window.orderBy("ts")))
                .selectExpr("explode(ex) ex")
                .selectExpr("ex.*");
        assertEquals(101, df.count());


        df = src
                .union(spark.sql("select '2019-12-30 07:00', null, null, 4, 0.4, 0.8, '2020-04-09 07:00'"))
                .union(spark.sql("select '2019-12-30 07:00', null, null, 4, 0.4, 0.8, '2020-04-09 07:00'"))
                .union(spark.sql("select '2020-04-09 07:00', null, null, null, null, null, '2020-04-09 07:00'"))
                .withColumn("ex", My.分摊分成(
                        "timestamp(ts)", "`金额`", "`科目`",
                        "`固定`", "`比例1`", "`比例2`", "timestamp(`结束日`)")
                        .over(Window.orderBy("ts")))
                .selectExpr("explode(ex) ex")
                .selectExpr("ex.*");
        assertEquals(202, df.count());
    }


    @Test
    public void patchFilterTest4() throws IOException {
        ArrayNode patch = mapper.createArrayNode();
        patch.addObject()
                .put("op", "replace")
                .put("path", "/foo/name")
                .put("value", "world");
        patch.addObject()
                .put("op", "replace")
                .put("path", "/foo/last")
                .put("value", "hello");
        patch.addObject()
                .put("op", "replace")
                .put("path", "/foo1/last")
                .put("value", "hello");

        Dataset<String> ds = spark.createDataset(ImmutableList.of(patch.toString()), Encoders.STRING());
        JsonNode result = mapper.readTree(ds.select(patchFilter("value", "/foo/name"))
                .first().getString(0));
        assertEquals(result.size(), 1);
        assertEquals(result.at("/0/value"), TextNode.valueOf("world"));

        result = mapper.readTree(ds.select(patchFilter("value", "/foo"))
                .first().getString(0));
        assertEquals(result.size(), 2);
    }


    @Test
    public void fengtanTest3() {
        Dataset<Row> src = spark.createDataset(ImmutableList.of(
                RowFactory.create("2020-04-01 08:00", "主营业务收入.收入.加液", 2L, null, null, null, null),
                RowFactory.create("2020-04-02 08:00", "主营业务收入.收入.加液", 2L, null, null, null, null),
                RowFactory.create("2020-04-03 08:00", "主营业务收入.收入.X", 4L, null, null, null, null),
                RowFactory.create("2020-04-05 16:00", "主营业务收入.收入.X", 4L, null, null, null, null),
                RowFactory.create("2020-04-06 08:00", "主营业务收入.收入.加液", 4L, null, null, null, null),
                RowFactory.create("2020-04-07 08:00", "主营业务收入.收入.加液", 2L, null, null, null, null)
        ), RowEncoder.apply(new StructType()
                .add("ts", "string")
                .add("科目", "string")
                .add("金额", "long")
                .add("固定", "long")
                .add("比例1", "double")
                .add("比例2", "double")
                .add("结束日", "string")));

        Dataset<Row> df = src
                .union(spark.sql("select '2020-04-01 07:00', null, null, 10L, null, null, '2020-04-09 07:00'"))
                .union(spark.sql("select '2020-04-01 07:00', null, null, null, 0.4, 0.8, '2020-04-09 07:00'"))
                .union(spark.sql("select '2020-04-09 07:00', null, null, null, null, null, '2020-04-09 07:00'"))
                .withColumn("ex", My.分摊分成(
                        "timestamp(ts)", "`金额`", "`科目`",
                        "`固定`", "`比例1`", "`比例2`", "timestamp(`结束日`)")
                        .over(Window.orderBy("ts")))
                .selectExpr("explode(ex) ex")
                .selectExpr("ex.*");
        assertEquals(16, df.count());

        df = src
                .union(spark.sql("select '2020-04-01 07:00', null, null, 10L, null, null, '2020-04-09 07:00'"))
                .union(spark.sql("select '2020-04-09 07:00', null, null, null, null, null, '2020-04-09 07:00'"))
                .withColumn("ex", My.分摊分成(
                        "timestamp(ts)", "`金额`", "`科目`",
                        "`固定`", "`比例1`", "`比例2`", "timestamp(`结束日`)")
                        .over(Window.orderBy("ts")))
                .selectExpr("explode(ex) ex")
                .selectExpr("ex.*");
        assertEquals(8, df.count());
        assertEquals(10L, df.agg(sum("amount")).first().getLong(0));
        assertEquals(2L, df.where("date='2020-04-02'").first().getLong(1));
        assertEquals(2L, df.where("date='2020-04-06'").first().getLong(1));

        df = src
                .union(spark.sql("select '2020-04-01 07:00', null, null, null, 0.4, 0.8, '2020-04-09 07:00'"))
                .union(spark.sql("select '2020-04-09 07:00', null, null, null, null, null, '2020-04-09 07:00'"))
                .withColumn("ex", My.分摊分成(
                        "timestamp(ts)", "`金额`", "`科目`",
                        "`固定`", "`比例1`", "`比例2`", "timestamp(`结束日`)")
                        .over(Window.orderBy("ts")))
                .selectExpr("explode(ex) ex")
                .selectExpr("ex.*");
        assertEquals(8, df.count());
        assertEquals(11L, df.agg(sum("amount")).first().getLong(0));
        assertEquals(2L, df.where("date='2020-04-01'").first().getLong(1));
        assertEquals(1L, df.where("date='2020-04-02'").first().getLong(1));
        assertEquals(0L, df.where("date='2020-04-04'").first().getLong(1));
        assertEquals(4L, df.where("date='2020-04-06'").first().getLong(1));
        assertEquals(1L, df.where("date='2020-04-07'").first().getLong(1));

        df = src
                .union(spark.sql("select '2020-04-01 07:00', null, null, 4, 0.4, 0.8, '2020-04-09 07:00'"))
                .union(spark.sql("select '2020-04-09 07:00', null, null, null, null, null, '2020-04-09 07:00'"))
                .withColumn("ex", My.分摊分成(
                        "timestamp(ts)", "`金额`", "`科目`",
                        "`固定`", "`比例1`", "`比例2`", "timestamp(`结束日`)")
                        .over(Window.orderBy("ts")))
                .selectExpr("explode(ex) ex")
                .selectExpr("ex.*");
        assertEquals(8, df.count());
        assertEquals(11L, df.agg(sum("amount")).first().getLong(0));
        assertEquals(1L, df.where("date='2020-04-01'").first().getLong(1));
        assertEquals(4L, df.where("date='2020-04-03'").first().getLong(1));
        assertEquals(0L, df.where("date='2020-04-04'").first().getLong(1));
        assertEquals(4L, df.where("date='2020-04-06'").first().getLong(1));
        assertEquals(0L, df.where("date='2020-04-08'").first().getLong(1));
    }

    @Test
    public void fentanTest4() {
        Dataset<Tuple3<String, String, Long>> ds = spark.createDataset(ImmutableList.of(
                Tuple3.apply("2020-04-01 07:00", null, null),
                Tuple3.apply("2020-04-01 08:00", "主营业务收入.收入.加液", 2L),
                Tuple3.apply("2020-04-02 08:00", "主营业务收入.收入.加液", 2L),    // [4.1, 1]
                Tuple3.apply("2020-04-03 08:00", "主营业务收入.收入.X", 4L),        // [4.2, 2]
                Tuple3.apply("2020-04-05 16:00", "主营业务收入.收入.X", 4L),        // [4.3, 1],[4.4, 1]
                Tuple3.apply("2020-04-06 08:00", "主营业务收入.收入.加液", 4L),    // [4.5, 1]
                Tuple3.apply("2020-04-07 08:00", "主营业务收入.收入.加液", 2L),    // [4.6, 2]
                Tuple3.apply("2020-04-09 07:00", null, null)                    // [4.7, 1],[4.8, 1]
        ), Encoders.tuple(Encoders.STRING(), Encoders.STRING(), Encoders.LONG()));
        Dataset<Row> df = ds
                .withColumn("ex", My.阶梯分成("timestamp(_1)", "_3", "_2",
                        "if(isnull(_2),array(struct(0.5d k,0l a),struct(0.6d k,4l a),struct(0.7d k,6l a)),null)",
                        "if(isnull(_2),0.8,null)")
                        .over(Window.orderBy("_1")))
                .selectExpr("explode(ex) ex")
                .selectExpr("ex.*");
        assertEquals(8, df.count());
        assertEquals(13L, df.agg(sum("amount")).first().getLong(0));
        assertEquals(2L, df.where("date='2020-04-01'").first().getLong(1));
        assertEquals(1L, df.where("date='2020-04-02'").first().getLong(1));
        assertEquals(0L, df.where("date='2020-04-04'").first().getLong(1));
        assertEquals(3L, df.where("date='2020-04-06'").first().getLong(1));
        assertEquals(2L, df.where("date='2020-04-07'").first().getLong(1));
    }

    @Test
    public void avgAmountTest() {
        Dataset<Row> src = spark.createDataset(ImmutableList.of(
                RowFactory.create("2020-04-02 07:00", "主营业务收入.收入.加液", 2L),
                RowFactory.create("2020-04-02 08:00", "主营业务收入.收入.加液", 2L),
                RowFactory.create("2020-04-03 08:00", "主营业务收入.收入.X", 4L),
                RowFactory.create("2020-04-05 16:00", "主营业务收入.收入.X", 4L),
                RowFactory.create("2020-04-06 08:00", "主营业务收入.收入.加液", 4L),
                RowFactory.create("2020-04-06 09:00", "主营业务收入.收入.加液", 2L)
        ), RowEncoder.apply(new StructType()
                .add("ts", "string")
                .add("科目", "string")
                .add("金额", "long")));
        Dataset<Row> df;

        df = src
                .union(spark.sql("select '2020-04-09 07:00',null,null"))
                .withColumn("ex", My.avgAmount(
                        "timestamp(ts)", "`金额`", "`科目`",
                        "10", "null", "null",
                        "timestamp('2020-04-01 07:00')",
                        "timestamp('2020-04-09 07:00')").over(Window.orderBy("ts")))
                .selectExpr("explode(ex) ex")
                .selectExpr("ex.*");
        assertEquals(8, df.count());
        assertEquals(10L, df.agg(sum("amount")).first().getLong(0));
        assertEquals(2L, df.where("date='2020-04-02'").first().getLong(1));
        assertEquals(2L, df.where("date='2020-04-06'").first().getLong(1));

        df = src
                .union(spark.sql("select '2020-04-09 07:00',null,null"))
                .withColumn("ex", My.avgAmount(
                        "timestamp(ts)", "`金额`", "`科目`",
                        "null", "array(struct(0.4d k,0l a))", "0.8",
                        "timestamp('2020-04-01 07:00')",
                        "timestamp('2020-04-09 07:00')").over(Window.orderBy("ts")))
                .selectExpr("explode(ex) ex")
                .selectExpr("ex.*");
        assertEquals(4, df.count());
        assertEquals(11L, df.agg(sum("amount")).first().getLong(0));
        assertEquals(3L, df.where("date='2020-04-02'").first().getLong(1));
        assertEquals(2L, df.where("date='2020-04-03'").first().getLong(1));
        assertEquals(1L, df.where("date='2020-04-05'").first().getLong(1));
        assertEquals(5L, df.where("date='2020-04-06'").first().getLong(1));

        // 开始时间正好一样
        df = src
                .union(spark.sql("select '2020-04-09 07:00',null,null"))
                .withColumn("ex", My.avgAmount(
                        "timestamp(ts)", "`金额`", "`科目`",
                        "null", "array(struct(0.4d k,0l a))", "0.8",
                        "timestamp('2020-04-02 07:00')",
                        "timestamp('2020-04-09 07:00')").over(Window.orderBy("ts")))
                .selectExpr("explode(ex) ex")
                .selectExpr("ex.*");
        assertEquals(4, df.count());
        assertEquals(11L, df.agg(sum("amount")).first().getLong(0));
        assertEquals(3L, df.where("date='2020-04-02'").first().getLong(1));
        assertEquals(2L, df.where("date='2020-04-03'").first().getLong(1));
        assertEquals(1L, df.where("date='2020-04-05'").first().getLong(1));
        assertEquals(5L, df.where("date='2020-04-06'").first().getLong(1));

        df = src
                .union(spark.sql("select '2020-04-09 07:00',null,null"))
                .withColumn("ex", My.avgAmount(
                        "timestamp(ts)", "`金额`", "`科目`",
                        "4", "array(struct(0.4d k,0l a))", "0.8",
                        "timestamp('2020-04-01 07:00')",
                        "timestamp('2020-04-09 07:00')").over(Window.orderBy("ts")))
                .selectExpr("explode(ex) ex")
                .selectExpr("ex.*");
        assertEquals(4, df.count());
        assertEquals(11L, df.agg(sum("amount")).first().getLong(0));
        assertEquals(1L, df.where("date='2020-04-01'").first().getLong(1));
        assertEquals(4L, df.where("date='2020-04-03'").first().getLong(1));
        assertEquals(1L, df.where("date='2020-04-05'").first().getLong(1));
        assertEquals(5L, df.where("date='2020-04-06'").first().getLong(1));

        // 阶梯分成
        df = src
                .union(spark.sql("select '2020-04-09 07:00',null,null"))
                .withColumn("ex", My.avgAmount(
                        "timestamp(ts)", "`金额`", "`科目`",
                        "null",
                        "array(struct(0.5d k,0l a),struct(0.6d k,4l a),struct(0.7d k,6l a))",
                        "0.8",
                        "timestamp('2020-04-01 07:00')",
                        "timestamp('2020-04-09 07:00')").over(Window.orderBy("ts")))
                .selectExpr("explode(ex) ex")
                .selectExpr("ex.*");
        assertEquals(4, df.count());
        assertEquals(13L, df.agg(sum("amount")).first().getLong(0));
        assertEquals(3L, df.where("date='2020-04-02'").first().getLong(1));
        assertEquals(2L, df.where("date='2020-04-03'").first().getLong(1));
        assertEquals(3L, df.where("date='2020-04-05'").first().getLong(1));
        assertEquals(5L, df.where("date='2020-04-06'").first().getLong(1));
    }

    @Test
    public void 分摊策略Test() {
        DataType t1 = new StructType()
                .add("策略号", "string")
                .add("收款方", "string")
                .add("账户", "string")
                .add("科目", "string")
                .add("固定金额", "long")
                .add("非加液阶梯比例", "array<struct<k:double,a:long>>")
                .add("加液比例", "double")
                .add("开始时间", "timestamp");

        DataType t2 = new StructType()
                .add("编号", "string")
                .add("名称", StringType$.MODULE$)
                .add("省", StringType$.MODULE$)
                .add("市", StringType$.MODULE$)
                .add("区", StringType$.MODULE$)
                .add("地址", StringType$.MODULE$)
                .add("合同截止日期", "timestamp");

        Dataset<Row> src = spark.createDataset(ImmutableList.of(
                RowFactory.create(Timestamp.valueOf("2030-01-01 00:00:00"),
                        RowFactory.create("1", null, null, "主营业务成本.装修费", null, null, null, Timestamp.valueOf("2019-01-01 00:00:00")),
                        RowFactory.create(null, null, null, null, null, null, Timestamp.valueOf("2020-01-01 00:00:00"))),
                RowFactory.create(Timestamp.valueOf("2030-01-01 00:00:00"),
                        RowFactory.create("2", null, null, "主营业务成本.装修费", null, null, null, Timestamp.valueOf("2019-01-01 00:00:00")),
                        RowFactory.create(null, null, null, null, null, null, Timestamp.valueOf("2030-01-01 00:00:00"))),
                RowFactory.create(Timestamp.valueOf("2030-01-01 00:00:00"),
                        RowFactory.create("3", null, null, "主营业务成本.装修费（空间设计费）", null, null, null, Timestamp.valueOf("2019-01-01 00:00:00")),
                        RowFactory.create(null, null, null, null, null, null, Timestamp.valueOf("2023-01-01 00:00:00")))
        ), RowEncoder.apply(new StructType()
                .add("ts", "timestamp")
                .add("share", t1)
                .add("biz", t2)));
        Dataset<Row> df;

        df = src
                .withColumn("ex", explode(My.分摊策略("ts", "share", "biz")))
                .selectExpr("share.`策略号` no", "ex.*");

        assertEquals(12 * 3, df.where("no='1'").count());
        assertEquals(12 * 5, df.where("no='2'").count());
        assertEquals(12 * 4, df.where("no='3'").count());
    }

    @Test
    // 调用接口测试
    public void apiTest1() {

        JsonNode jsonNode = apiRequest(HttpMethod.POST,
                "http://localhost:8848/article/item/1", "", "", null);
        System.out.println(jsonNode);
        // 打印结果: {"status":200,"msg":"OK","data":null}
    }

    @Test
    public void apiTest2() {
        ObjectNode requestNode = mapper.createObjectNode();
        requestNode.put("page", "1");
        requestNode.put("rows", "5");
        JsonNode jsonNode = apiRequest(HttpMethod.POST,
                "http://localhost:10002/animal/pageManage", "", "", requestNode);
        System.out.println(jsonNode);

        // 打印结果:
        //{
        //    "total": 26,
        //    "rows": [
        //        {
        //            "animalId": 1,
        //            "animalName": "小白",
        //            "animalImage": "http://image.boluo.com/upload/d/5/f/3/3/a/e/0/0d65bfaa-e161-4c2b-83fc-abc546e302e1.jpg",
        //            "animalType": "狗",
        //            "animalInfo": "九个月大！性别：母，超级黏人，已经做了绝育，等待你来领养哦！",
        //            "giveName": "救助站",
        //            "giveTitle": "救助站小白等待你领养哦！",
        //            "givePhone": "18831086282",
        //            "giveAddress": "唐山市流浪动物救助中心"
        //        },
        //        {
        //
        //        }
        //    ]
        //}
    }

    @Test
    public void 按策略分摊Test() {
        Dataset<Row> src = spark.createDataset(ImmutableList.of(
                RowFactory.create("2020-04-02 07:00", "主营业务收入.收入.加液", 2L),
                RowFactory.create("2020-04-02 08:00", "主营业务收入.收入.加液", 3L),
                RowFactory.create("2020-04-03 08:00", "主营业务收入.收入.X", 17L),
                RowFactory.create("2020-04-05 16:00", null, 23L),
                RowFactory.create("2020-04-06 08:00", "主营业务收入.收入.加液", 7L),
                RowFactory.create("2020-04-06 09:00", "主营业务收入.收入.加液", 13L)
        ), RowEncoder.apply(new StructType()
                .add("ts", "string")
                .add("科目", "string")
                .add("金额", "long")));
        Dataset<Row> df;

        // 按天数
        df = src
                .union(spark.sql("select '2020-04-09 07:00',null,null"))
                .withColumn("s", expr("sum(`金额`) over ()"))
                .withColumn("ex", Func.按策略分摊1(
                        "datediff(ts,'2020-04-01')",
                        "`金额`",
                        "datediff('2020-04-09','2020-04-01')",
                        "s",
                        "'天数'",
                        "array(struct(0.0d k,11l a))"
                ).over(Window.orderBy("ts", "科目")))
                .selectExpr("explode(ex) ex")
                .selectExpr("ex.*")
                .cache();
        assertEquals(8, df.count());
        assertEquals(11L, df.agg(last("amount")).first().getLong(0));

        // 按金额分
        df = src
                .union(spark.sql("select '2020-04-09 07:00',null,null"))
                .withColumn("s", expr("sum(`金额`) over ()"))
                .withColumn("ex", Func.按策略分摊1(
                        "datediff(ts,'2020-04-01')",
                        "`金额`",
                        "datediff('2020-04-09','2020-04-01')",
                        "s",
                        "'金额'",
                        "array(struct(0.0d k,11l a))"
                ).over(Window.orderBy("ts", "科目")))
                .selectExpr("explode(ex) ex")
                .selectExpr("ex.*")
                .cache();
        assertEquals(8, df.count());
        assertEquals(0, df.where("n=0").first().getLong(1));
        assertEquals(1, df.where("n=1").first().getLong(1));
        assertEquals(4, df.where("n=2").first().getLong(1));
        assertEquals(4, df.where("n=3").first().getLong(1));
        assertEquals(8, df.where("n=4").first().getLong(1));
        assertEquals(11, df.where("n=5").first().getLong(1));
        assertEquals(11, df.where("n=6").first().getLong(1));
        assertEquals(11, df.where("n=7").first().getLong(1));

        // 按金额分
        df = src
                .union(spark.sql("select '2020-04-09 07:00',null,null"))
                .withColumn("s", expr("sum(`金额`) over ()"))
                .withColumn("ex", Func.按策略分摊1(
                        "datediff(ts,'2020-04-01')",
                        "`金额`",
                        "datediff('2020-04-09','2020-04-01')",
                        "s",
                        "'金额'",
                        "array(struct(0.0d k,-11l a))"
                ).over(Window.orderBy("ts", "科目")))
                .selectExpr("explode(ex) ex")
                .selectExpr("ex.*")
                .cache();
        assertEquals(8, df.count());
        assertEquals(0, df.where("n=0").first().getLong(1));
        assertEquals(-1, df.where("n=1").first().getLong(1));
        assertEquals(-4, df.where("n=2").first().getLong(1));
        assertEquals(-4, df.where("n=3").first().getLong(1));
        assertEquals(-8, df.where("n=4").first().getLong(1));
        assertEquals(-11, df.where("n=5").first().getLong(1));
        assertEquals(-11, df.where("n=6").first().getLong(1));
        assertEquals(-11, df.where("n=7").first().getLong(1));

        // 按比例
        df = src
                .union(spark.sql("select '2020-04-09 07:00',null,null"))
                .withColumn("s", expr("sum(`金额`) over ()"))
                .withColumn("ex", Func.按策略分摊1(
                        "datediff(ts,'2020-04-01')",
                        "`金额`",
                        "datediff('2020-04-09','2020-04-01')",
                        "s",
                        "'金额'",
                        "array(struct(-0.4d k,0l a))"
                ).over(Window.orderBy("ts", "科目")))
                .selectExpr("explode(ex) ex")
                .selectExpr("ex.*")
                .cache();
        assertEquals(8, df.count());
        assertEquals(0, df.where("n=0").first().getLong(1));
        assertEquals(-2, df.where("n=1").first().getLong(1));
        assertEquals(-9, df.where("n=2").first().getLong(1));
        assertEquals(-9, df.where("n=3").first().getLong(1));
        assertEquals(-18, df.where("n=4").first().getLong(1));
        assertEquals(-26, df.where("n=5").first().getLong(1));
        assertEquals(-26, df.where("n=6").first().getLong(1));
        assertEquals(-26, df.where("n=7").first().getLong(1));

        // 按阶梯比例
        df = src
                .union(spark.sql("select '2020-04-09 07:00',null,null"))
                .withColumn("s", expr("sum(`金额`) over ()"))
                .withColumn("ex", Func.按策略分摊1(
                        "datediff(ts,'2020-04-01')",
                        "`金额`",
                        "datediff('2020-04-09','2020-04-01')",
                        "s",
                        "'金额'",
                        "array(struct(-0.4d k,0l a),struct(-0.5d k,20l a),struct(-0.6d k,45l a))"
                ).over(Window.orderBy("ts", "科目")))
                .selectExpr("explode(ex) ex")
                .selectExpr("ex.*")
                .cache();
        assertEquals(8, df.count());
        assertEquals(0, df.where("n=0").first().getLong(1));
        assertEquals(-2, df.where("n=1").first().getLong(1));
        assertEquals(-9, df.where("n=2").first().getLong(1));
        assertEquals(-9, df.where("n=3").first().getLong(1));
        assertEquals(-21, df.where("n=4").first().getLong(1));
        assertEquals(-33, df.where("n=5").first().getLong(1));
        assertEquals(-33, df.where("n=6").first().getLong(1));
        assertEquals(-33, df.where("n=7").first().getLong(1));
    }

}
