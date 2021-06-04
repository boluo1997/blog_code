package boluo.work;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.CollectionAccumulator;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import javax.ws.rs.HttpMethod;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.URI;
import java.sql.Date;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Qingliu {

	private static final ObjectMapper mapper = new ObjectMapper();
	private static final Logger logger = LoggerFactory.getLogger(Qingliu.class);
	private static final Cache<String, JsonNode> requestCache = CacheBuilder.newBuilder().build();
	private static final CloseableHttpClient http = HttpClients.createDefault();

	static SparkSession spark = SparkSession
			.builder()
			.master("local[*]")
			.appName("Simple Application")
			.getOrCreate();

	public static long replace(Dataset<Row> ds, String uri, String key) {

		URI sUri = URI.create(uri);
		String appId = sUri.getHost();
		String token = sUri.getQuery();
		token = token.substring(6);

		StructType schema = ds.schema();

		// 查询申请人账户
		Optional<JsonNode> creator = user("小兰Robot", token);
		String userId = creator.map(i -> i.at("/userId").asText()).orElse(null);

		// 获取应用表单信息
		List<ObjectNode> form = form(appId, token, schema);
		List<String> formSer = form.stream().map(ObjectNode::toString).collect(Collectors.toList());

		// 分页查询应用数据
		List<ObjectNode> qingliuDataObjectNode = getQingliuData(appId, token);
		List<String> qingliuDataStr = qingliuDataObjectNode.stream().map(ObjectNode::toString).collect(Collectors.toList());
		Set<Object> keySet = Sets.newHashSet();
		for (ObjectNode jn : qingliuDataObjectNode) {
			Object jnKey = getKey(jn.withArray("answers"), key);
			keySet.add(Objects.requireNonNull(jnKey));
		}

		String finalToken = token;
		CollectionAccumulator<Object> removeQingliuAc = new CollectionAccumulator<>();
		LongAccumulator updateCount = new LongAccumulator();
		removeQingliuAc.register(spark.sparkContext(), Option.apply("removeQingliuData"), false);
		updateCount.register(spark.sparkContext(), Option.apply("updateCount"), false);

		ds.foreachPartition((rowIt) -> {
			while (rowIt.hasNext()) {
				Row row = rowIt.next();
				ObjectNode dsAnswer = mapper.createObjectNode();

				List<ObjectNode> qingliuData = stringToObjectNode(qingliuDataStr);
				Iterable<ObjectNode> form2 = stringToObjectNode(formSer);

				answers(form2, row)
						.forEach(dsAnswer.withArray("answers")::add);

				Object dsKey1 = Optional.ofNullable(row.getAs(key)).orElse("");
				if (keySet.contains(dsKey1)) {

					JsonNode answersJsonNode = getKeyNode(qingliuData, key, dsKey1);
					if (Objects.isNull(answersJsonNode)) throw new UnsupportedOperationException();

					ArrayNode answersArrayNode;
					ArrayNode dsArrayNode;
					if (answersJsonNode.at("/answers").isArray() && dsAnswer.at("/answers").isArray()) {
						answersArrayNode = (ArrayNode) answersJsonNode.at("/answers");
						dsArrayNode = (ArrayNode) dsAnswer.at("/answers");
					} else {
						throw new UnsupportedOperationException();
					}

					if (!compare(answersArrayNode, dsArrayNode)) {
						// 属性不相同, 更新单条数据信息
						String applyId = answersJsonNode.at("/applyId").asText();
						JsonNode req = apiRequest(HttpMethod.POST, String.format("https://api.ding.qingflow.com/apply/%s", applyId), finalToken, userId, dsAnswer);
						String requestId = req.at("/result/requestId").asText();
						Preconditions.checkArgument(!Strings.isNullOrEmpty(requestId), "requestId MISSING");
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						JsonNode updateCheck = apiRequest(HttpMethod.GET, String.format("https://api.ding.qingflow.com/operation/%s", requestId), finalToken, userId, null);
						Preconditions.checkArgument(updateCheck.at("/errorCode").asInt() == 0, "");
						updateCount.add(1L);
					}
					removeQingliuAc.add(dsKey1);
				} else {
					// 添加数据
					JsonNode req = apiRequest(HttpMethod.POST, String.format("https://api.ding.qingflow.com/app/%s/apply", appId), finalToken, userId, dsAnswer);
					String requestId = req.at("/result/requestId").textValue();
					Preconditions.checkArgument(!Strings.isNullOrEmpty(requestId), "MISSING requestId");
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					JsonNode addCheck = apiRequest(HttpMethod.GET, String.format("https://api.ding.qingflow.com/operation/%s", requestId), finalToken, userId, null);
					Preconditions.checkArgument(addCheck.at("/errorCode").asInt() == 0, "");
					removeQingliuAc.add(dsKey1);
					updateCount.add(1L);
				}
			}
		});

		// 先从qingliuData中把不需要删除的数据排除掉, 留下的都是需要删除的
		for (Object ac : removeQingliuAc.value()) {
			for (int j = 0; j < qingliuDataObjectNode.size(); j++) {
				if (Objects.equals(getKey(qingliuDataObjectNode.get(j).withArray("answers"), key), ac)) {
					qingliuDataObjectNode.remove(j--);
				}
			}
		}

		// 删除数据
		Set<String> deleteApplyIds = Sets.newTreeSet();
		for (JsonNode jn : qingliuDataObjectNode) {
			deleteApplyIds.add(jn.at("/applyId").asText());
		}

		if (deleteApplyIds.size() != 0) {
			ObjectNode deleteRequest = mapper.createObjectNode();
			deleteRequest.put("pageSize", "50")
					.putPOJO("applyIds", deleteApplyIds);
			JsonNode resultNode = apiRequest(HttpMethod.DELETE, String.format("https://api.ding.qingflow.com/app/%s/apply", appId),
					token, userId, deleteRequest);

			String requestId = resultNode.at("/requestId").textValue();
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			JsonNode deleteCheck = apiRequest(HttpMethod.GET, String.format("https://api.ding.qingflow.com/operation/%s", requestId), token, userId, null);
			Preconditions.checkArgument(deleteCheck.at("/errorCode").asInt() == 0, "删除错误！");
			updateCount.add(deleteApplyIds.size());
		}
		return updateCount.value();
	}

	private static List<ObjectNode> stringToObjectNode(List<String> list) {
		return list.stream()
				.map(i -> {
					try {
						return (ObjectNode) mapper.readTree(i);
					} catch (IOException e) {
						e.printStackTrace();
					}
					throw new UnsupportedOperationException("String to ObjectNode异常");
				}).collect(Collectors.toList());
	}

	private static List<ObjectNode> getQingliuData(String app, String token) {

		// JsonNode form = form(app, token);
		ObjectNode args = mapper.createObjectNode()
				.put("pageSize", 1000)
				.put("pageNum", 1)
				.put("type", 8)
				.putNull("queryKey");
		args.withArray("queries");
		args.withArray("sorts");
		ObjectNode data = (ObjectNode) apiRequest(HttpMethod.POST,
				String.format("https://api.ding.qingflow.com/app/%s/apply/filter", app),
				token, null, args);

		Preconditions.checkArgument(data.at("/errCode").intValue() == 0);
		int pageAmount = data.at("/result/pageAmount").asInt();
		Stream<ObjectNode> str = Stream.iterate(2, i -> i + 1)
				.limit(Math.max(pageAmount - 1, 0))
				.map(i -> {
					args.put("pageNum", i);
					return apiRequest(HttpMethod.POST,
							String.format("https://api.ding.qingflow.com/app/%s/apply/filter", app),
							token, null, args);
				});

		List<ObjectNode> resultData = Stream.concat(Stream.of(data), str)
				.flatMap(d -> {
					Preconditions.checkArgument(d.at("/errCode").intValue() == 0);
					return Streams.stream(d.at("/result/result"));
				})
				.map(i -> (ObjectNode) i)
				.collect(Collectors.toList());

		return resultData;
	}

	private static Stream<ObjectNode> answers(Iterable<ObjectNode> form, Row p) {
		return Streams.stream(form)
				.map(f -> {
					int queId = f.at("/queId").intValue();
					if (queId == 0) return null;
					String queTitle = f.at("/queTitle").textValue();
					int queType = f.at("/queType").intValue();
					Object value = p.getAs(queTitle);
					if (Objects.isNull(value)) return null;
					ObjectNode r = mapper.createObjectNode()
							.put("queId", queId);
					switch (queType) {
						default:
							logger.warn("type={}", queType);
						case 2:    /*单行文字*/
						case 3:
						case 4:
						case 8:
						case 10:
						case 16:
						case 19: {
							if (value.getClass().isAssignableFrom(String.class)) {
								r.withArray("values").addObject()
										.put("value", (String) value);
							} else if (value.getClass().isAssignableFrom(Long.class)
									|| value.getClass().isAssignableFrom(Double.class)
									|| value.getClass().isAssignableFrom(BigDecimal.class)) {
								r.withArray("values").addObject()
										.putPOJO("value", value);
							} else if (value.getClass().isAssignableFrom(Date.class)) {
								r.withArray("values").addObject()
										.put("value", value.toString());
							} else {
								r.withArray("values").addObject()
										.put("value", (String) value);
							}
							return r;
						}
						case 5:    /*成员*/ {
							r.withArray("values").addObject()
									.put("value", value.toString());
							return r;
						}
						case 18: {
							throw new UnsupportedOperationException(queTitle);
						}
						case 12:    // 流程状态
						case 13:
						case 21: /*地址*/ {
							r.withArray("values").addObject()
									.put("value", value.toString());
							return r;
						}
					}
				})
				.filter(Objects::nonNull)
				.filter(i -> i.at("/tableValues").size() + i.at("/values").size() > 0);
	}

	private static JsonNode getKeyNode(List<ObjectNode> nodeList, String key, Object dsKey) {
		for (ObjectNode jn : nodeList) {
			if (Objects.equals(getKey(jn.withArray("answers"), key), dsKey)) {
				return jn;
			}
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	private static <T> T getKey(ArrayNode answer, String key) {

		for (JsonNode jn : answer) {
			if (jn.at("/queTitle").asText().equals(key) && !jn.at("/queId").asText().equals("0")) {
				String queType = jn.at("/queType").asText();
				switch (queType) {
					case "":
						JsonNodeType type = jn.at("/values/0/value").getNodeType();
						if (type == JsonNodeType.STRING) return (T) jn.at("/values/0/value").asText();
						if (type == JsonNodeType.NUMBER) return (T) (Object) jn.at("/values/0/value").asLong();
						throw new UnsupportedOperationException("未知的类型...");
					case "2":
						return (T) jn.at("/values/0/value").asText();
					case "8":
						return (T) (Object) jn.at("/values/0/value").asLong();
					default:
						throw new UnsupportedOperationException("未知的类型: queType = " + queType);
				}
			}
		}
		throw new IllegalArgumentException();
	}

	private static boolean compare(ArrayNode answer1, ArrayNode answer2) {

		boolean br = false;
		Set<String> idSet = Sets.newTreeSet();
		for (JsonNode jn1 : answer1) {
			idSet.add(jn1.at("/queId").asText());
		}

		for (JsonNode jn2 : answer2) {
			if (idSet.contains(jn2.at("/queId").asText())) {
				for (JsonNode jn1 : answer1) {
					String value1 = jn1.at("/values/0/value").asText();
					if (jn2.at("/queId").asText().equals(jn1.at("/queId").asText())) {
						br = jn2.at("/values/0/value").asText().equals(value1);
					}
				}
			} else {
				return false;
			}
		}
		return br;
	}

	private static ObjectNode apiRequest(String method, String uri, String token, String userId, JsonNode body) {
		RequestBuilder requestBuilder = RequestBuilder.create(method)
				.setUri(uri)
				.setHeader("accessToken", token)
				.setHeader("Content-Type", "application/json");
		Optional.ofNullable(body).ifPresent(i -> {
			requestBuilder.setEntity(new StringEntity(body.toString(), Charsets.UTF_8));
		});
		Optional.ofNullable(userId).ifPresent(i -> {
			requestBuilder.setHeader("userId", userId);
		});

		try (CloseableHttpResponse response = http.execute(requestBuilder.build());
			 InputStream is = response.getEntity().getContent()) {
			// logger.info("{} {}:{}", method, uri, body);
			Preconditions.checkArgument(response.getStatusLine().getStatusCode() == 200, response.getStatusLine());
			return (ObjectNode) mapper.readTree(is);
		} catch (IOException e) {
			throw new RuntimeException(String.format("%s %s:%s", method, uri, body), e);
		}
	}

	private static Optional<JsonNode> user(String name, String token) {
		JsonNode user = requestCache.asMap().computeIfAbsent("https://api.ding.qingflow.com/department/1/user?fetchChild=true", uri -> {
			JsonNode r = apiRequest(HttpMethod.GET, uri, token, null, null);
			String req = "uri" + uri + "\n" + "token" + token;
			// System.out.println(r);
			Preconditions.checkArgument(r.path("errCode").intValue() == 0, req + "返回值: " + r);
			return r.at("/result/userList");
		});
		return Streams.stream(user)
				.filter(i -> i.at("/name").asText().equals(name))
				.findAny();
	}

	private static List<ObjectNode> form(String app, String token, StructType schema) {

		String uri = String.format("https://api.ding.qingflow.com/app/%s/form", app);
		ObjectNode form = apiRequest(HttpMethod.GET, uri, token, null, null);
		Preconditions.checkArgument(form.at("/errCode").asInt(500) == 0, form.at("/errMsg").asText());
		ObjectNode obj = form.with("result");

		List<ObjectNode> fields = Streams.stream(obj.at("/questionBaseInfos"))
				.filter(i -> Arrays.asList(schema.fieldNames()).contains(i.at("/queTitle").asText()))
				.map(i -> (ObjectNode) i)
				.collect(Collectors.toList());

		return fields;
	}

}
