import boluo.work.Qingliu;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class QingliuTest {

	private static ObjectMapper mapper = new ObjectMapper();

	static SparkSession spark = SparkSession
			.builder()
			.master("local[*]")
			.appName("Simple Application")
			.getOrCreate();

	@Test
	public void publicTest() {
		Method[] declaredMethods = Qingliu.class.getDeclaredMethods();
		Set<String> publicMethod = Arrays.stream(declaredMethods).filter(i -> Modifier.isPublic(i.getModifiers()))
				.map(Method::getName)
				.collect(Collectors.toSet());
		Assert.assertEquals(ImmutableSet.of("replace"), publicMethod);
	}

	@Test
	public void replaceTest() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {

		String appid = "1670d156";
		String token = "638fb362-7ee9-4d85-9359-e92f5c7b353e";
		Method getQingliuData = Qingliu.class.getDeclaredMethod("getQingliuData", String.class, String.class);
		getQingliuData.setAccessible(true);
		Method compare = Qingliu.class.getDeclaredMethod("compare", ArrayNode.class, ArrayNode.class);
		compare.setAccessible(true);
		Method getKey = Qingliu.class.getDeclaredMethod("getKey", ArrayNode.class, String.class);
		getKey.setAccessible(true);

		List<JsonNode> actual;
		Dataset<Row> src;
		JsonNode actualRow;
		long updated;

		src = spark.createDataset(ImmutableList.of(
		), RowEncoder.apply(new StructType()
				.add("测试字段", "string")
				.add("编号", "string")
				.add("名称", "string")));
		updated = Qingliu.replace(src, String.format("qingliu://%s?token=%s", appid, token), "编号");
		actual = (List<JsonNode>) getQingliuData.invoke(null, appid, token);
		Assert.assertEquals(0, actual.size());

		src = spark.createDataset(ImmutableList.of(
				RowFactory.create("测试字段1", "编号1", "名称1", 1L),
				RowFactory.create("测试字段2", "编号2", "名称2", 2L),
				RowFactory.create("测试字段3", "编号3", "名称3", 3L)
		), RowEncoder.apply(new StructType()
				.add("测试字段", "string")
				.add("编号", "string")
				.add("名称", "string")
				.add("数字", "long")));
		updated = Qingliu.replace(src, String.format("qingliu://%s?token=%s", appid, token), "编号");
		Assert.assertEquals(3, updated);
		actual = (List<JsonNode>) getQingliuData.invoke(null, appid, token);
		Assert.assertEquals(3, actual.size());
		actualRow = findAnswer(getKey, actual, "编号", "编号1");
		Assert.assertEquals(1L, getKey.invoke(null, actualRow.at("/answers"), "数字"));

		src = spark.createDataset(ImmutableList.of(
				RowFactory.create("测试字段1", "编号1", "名称1", 1L),
				RowFactory.create("测试字段2", "编号2", "名称2", 2L),
				RowFactory.create("测试字段3", "编号3", "名称3", 3L)
		), RowEncoder.apply(new StructType()
				.add("测试字段", "string")
				.add("编号", "string")
				.add("名称", "string")
				.add("数字", "long")));
		updated = Qingliu.replace(src, String.format("qingliu://%s?token=%s", appid, token), "编号");
		Assert.assertEquals(0, updated);
		actual = (List<JsonNode>) getQingliuData.invoke(null, appid, token);
		Assert.assertEquals(3, actual.size());

		src = spark.createDataset(ImmutableList.of(
				RowFactory.create("测试字段1", "编号1"),
				RowFactory.create("测试字段2", "编号2"),
				RowFactory.create("测试字段3", "编号3")
		), RowEncoder.apply(new StructType()
				.add("测试字段", "string")
				.add("编号", "string")));
		updated = Qingliu.replace(src, String.format("qingliu://%s?token=%s", appid, token), "编号");
		Assert.assertEquals(0, updated);
		actual = (List<JsonNode>) getQingliuData.invoke(null, appid, token);
		Assert.assertEquals(3, actual.size());
		actualRow = findAnswer(getKey, actual, "编号", "编号1");
		Assert.assertEquals("名称1", getKey.invoke(null, actualRow.at("/answers"), "名称"));

		src = spark.createDataset(ImmutableList.of(
				RowFactory.create("测试字段2", "编号2"),
				RowFactory.create("测试字段3", "编号3"),
				RowFactory.create("测试字段1", "编号4")
		), RowEncoder.apply(new StructType()
				.add("测试字段", "string")
				.add("编号", "string")));
		updated = Qingliu.replace(src, String.format("qingliu://%s?token=%s", appid, token), "编号");
		Assert.assertEquals(2, updated);
		actual = (List<JsonNode>) getQingliuData.invoke(null, appid, token);
		Assert.assertEquals(3, actualRow.size());
		actualRow = findAnswer(getKey, actual, "编号", "编号2");
		Assert.assertEquals("名称2", getKey.invoke(null, actualRow.at("/answers"), "名称"));

	}

	@Test
	public void getKeyTest() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		Method getKey = Qingliu.class.getDeclaredMethod("getKey", ArrayNode.class, String.class);
		getKey.setAccessible(true);

		ArrayNode answer = mapper.createArrayNode();
		answer.addObject()
				.put("queTitle", "编号")
				.withArray("values")
				.addObject()
				.put("value", "20");

		Assert.assertEquals("20", getKey.invoke(null, answer, "编号"));
	}

	@Test
	public void compareTest() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		Method compare = Qingliu.class.getDeclaredMethod("compare", ArrayNode.class, ArrayNode.class);
		compare.setAccessible(true);

		ArrayNode answer1 = mapper.createArrayNode();
		answer1.addObject()
				.put("queId", 0)
				.put("queTitle", "编号")
				.put("queType", "8")
				.withArray("values")
				.addObject()
				.put("value", "20");

		ArrayNode answer2 = mapper.createArrayNode();
		answer2.addObject()
				.put("queId", "0")
				.withArray("values")
				.addObject()
				.put("tableValue", "20")
				.put("value", "20");

		ArrayNode answer3 = mapper.createArrayNode();
		answer3.addObject()
				.put("queId", "3")
				.withArray("values")
				.addObject()
				.put("value", "20");

		Assert.assertEquals(true, compare.invoke(null, answer1, answer2));
		Assert.assertEquals(false, compare.invoke(null, answer1, answer3));

		ArrayNode answer4 = mapper.createArrayNode();
		answer4.addObject()
				.put("queId", "0")
				.withArray("values")
				.addObject()
				.put("value", 20);
		Assert.assertEquals(true, compare.invoke(null, answer1, answer4));
	}

	private static JsonNode findAnswer(Method getKey, List<JsonNode> actual, String key, String value) throws InvocationTargetException, IllegalAccessException {
		for (JsonNode i : actual) {
			if (Objects.equals(value, getKey.invoke(null, (ArrayNode) i.at("/answers"), key))) {
				return i;
			}
		}
		return null;
	}
}
