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
	@SuppressWarnings("unchecked")
	public void replaceTest() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		String appid = "167";
		String token = "638";
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
				.add("????????????", "string")
				.add("??????", "string")
				.add("??????", "string")));
		updated = Qingliu.replace(src, String.format("qingliu://%s?token=%s", appid, token), "??????");
		actual = (List<JsonNode>) getQingliuData.invoke(null, appid, token);
		Assert.assertEquals(0, actual.size());

		src = spark.createDataset(ImmutableList.of(
				RowFactory.create("????????????1", "??????1", "??????1", 1L),
				RowFactory.create("????????????2", "??????2", "??????2", 2L),
				RowFactory.create("????????????3", "??????3", "??????3", 3L)
		), RowEncoder.apply(new StructType()
				.add("????????????", "string")
				.add("??????", "string")
				.add("??????", "string")
				.add("??????", "long")));
		updated = Qingliu.replace(src, String.format("qingliu://%s?token=%s", appid, token), "??????");
		Assert.assertEquals(3, updated);
		actual = (List<JsonNode>) getQingliuData.invoke(null, appid, token);
		Assert.assertEquals(3, actual.size());
		actualRow = findAnswer(getKey, actual, "??????", "??????1");
		Assert.assertEquals(1L, getKey.invoke(null, actualRow.at("/answers"), "??????"));

		src = spark.createDataset(ImmutableList.of(
				RowFactory.create("????????????1", "??????1", "??????1", 1L),
				RowFactory.create("????????????2", "??????2", "??????2", 2L),
				RowFactory.create("????????????3", "??????3", "??????3", 3L)
		), RowEncoder.apply(new StructType()
				.add("????????????", "string")
				.add("??????", "string")
				.add("??????", "string")
				.add("??????", "long")));
		updated = Qingliu.replace(src, String.format("qingliu://%s?token=%s", appid, token), "??????");
		Assert.assertEquals(0, updated);
		actual = (List<JsonNode>) getQingliuData.invoke(null, appid, token);
		Assert.assertEquals(3, actual.size());

		src = spark.createDataset(ImmutableList.of(
				RowFactory.create("????????????1", "??????1", "??????1", 1L),
				RowFactory.create("????????????2", "??????2", "??????2", 2L),
				RowFactory.create("????????????3", "??????3", "??????3", 3L)
		), RowEncoder.apply(new StructType()
				.add("????????????", "string")
				.add("??????", "string")
				.add("??????", "string")
				.add("??????", "long")));
		updated = Qingliu.replace(src, String.format("qingliu://%s?token=%s", appid, token), "??????");
		Assert.assertEquals(0, updated);

		src = spark.createDataset(ImmutableList.of(
				RowFactory.create("????????????1", "??????1"),
				RowFactory.create("????????????2", "??????2"),
				RowFactory.create("????????????3", "??????3")
		), RowEncoder.apply(new StructType()
				.add("????????????", "string")
				.add("??????", "string")));
		updated = Qingliu.replace(src, String.format("qingliu://%s?token=%s", appid, token), "??????");
		Assert.assertEquals(0, updated);
		actual = (List<JsonNode>) getQingliuData.invoke(null, appid, token);
		Assert.assertEquals(3, actual.size());
		actualRow = findAnswer(getKey, actual, "??????", "??????1");
		Assert.assertEquals("??????1", getKey.invoke(null, actualRow.at("/answers"), "??????"));

		src = spark.createDataset(ImmutableList.of(
				RowFactory.create("????????????2", "??????2"),
				RowFactory.create("????????????3", "??????3"),
				RowFactory.create("????????????1", "??????4")
		), RowEncoder.apply(new StructType()
				.add("????????????", "string")
				.add("??????", "string")));
		updated = Qingliu.replace(src, String.format("qingliu://%s?token=%s", appid, token), "??????");
		Assert.assertEquals(2, updated);
		actual = (List<JsonNode>) getQingliuData.invoke(null, appid, token);
		Assert.assertEquals(3, actualRow.size());
		actualRow = findAnswer(getKey, actual, "??????", "??????2");
		Assert.assertEquals("??????2", getKey.invoke(null, actualRow.at("/answers"), "??????"));

	}

	@Test
	public void getKeyTest() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		Method getKey = Qingliu.class.getDeclaredMethod("getKey", ArrayNode.class, String.class);
		getKey.setAccessible(true);

		ArrayNode answer = mapper.createArrayNode();
		answer.addObject()
				.put("queTitle", "??????")
				.withArray("values")
				.addObject()
				.put("value", "20");
		answer.addObject()
				.put("queTitle", "??????1")
				.withArray("values")
				.addObject()
				.put("value", 20);
		answer.addObject()
				.put("queTitle", "??????2")
				.put("queType", "8")
				.withArray("values")
				.addObject()
				.put("value", 20);

		Assert.assertEquals("20", getKey.invoke(null, answer, "??????"));
		Assert.assertEquals(20L, getKey.invoke(null, answer, "??????1"));
		Assert.assertEquals(20L, getKey.invoke(null, answer, "??????2"));
	}

	@Test
	public void compareTest() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		Method compare = Qingliu.class.getDeclaredMethod("compare", ArrayNode.class, ArrayNode.class);
		compare.setAccessible(true);

		ArrayNode answer1 = mapper.createArrayNode();
		answer1.addObject()
				.put("queId", 0)
				.put("queTitle", "??????")
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
