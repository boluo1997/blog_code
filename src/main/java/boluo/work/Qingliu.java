package boluo.work;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.HttpMethod;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
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

	public static long replace(Dataset<Row> ds, String url, String key) {

		String appId = "1670d156";
		String token = "638fb362-7ee9-4d85-9359-e92f5c7b353e";

		// ds.show(false);
		StructType schema = ds.schema();

		// 查询申请人账户
		Optional<JsonNode> creator = user("丁超", token);
		String userId = creator.map(i -> i.at("/userId").asText()).orElse(null);

		// 获取应用表单信息
		JsonNode form = form(appId, token);
		List<JsonNode> fields = Streams.stream(form.at("/questionBaseInfos"))
				.filter(i -> Arrays.asList(schema.fieldNames()).contains(i.at("/queTitle").asText()))
				.collect(Collectors.toList());

		// 分页查询应用数据
		List<JsonNode> qingliuData = getQingliuData(appId, token);

		Set<String> keySet = Sets.newTreeSet();
		for (JsonNode jn : qingliuData) {
			JsonNode answersJsonNode = jn.at("/answers");
			// JsonNode parseAnswer = answer2value(answersJsonNode);
			// String qingliuKey = parseAnswer.at("/" + key).asText();
			// keySet.add(qingliuKey);
		}

		ds.toLocalIterator().forEachRemaining(row -> {

			ObjectNode dsAnswer = mapper.createObjectNode();
			answers(fields, row)
					.forEach(dsAnswer.withArray("answers")::add);

			String dsKey = row.getAs(key).toString();
			if (keySet.contains(dsKey)) {
				for (JsonNode jn : qingliuData) {
					JsonNode answersJsonNode = jn.at("/answers");
//					JsonNode parseAnswer = answer2value(answersJsonNode);
//					String qingliuKey = parseAnswer.at("/" + key).asText();
//					if(dsKey.equals(qingliuKey)){
//						String applyId = jn.at("/applyId").asText();
//					}
				}

			} else {
				// 添加数据
				JsonNode req = apiRequest(HttpMethod.POST, String.format("https://api.ding.qingflow.com/app/%s/apply", appId), token, userId, dsAnswer);
				String requestId = req.at("/result/requestId").textValue();
				System.out.println(requestId);
			}

		});

		throw new UnsupportedOperationException();
	}

	private static List<JsonNode> getQingliuData(String app, String token) {

		// JsonNode form = form(app, token);
		ObjectNode args = mapper.createObjectNode()
				.put("pageSize", 1000)
				.put("pageNum", 1)
				.put("type", 8)
				.putNull("queryKey");
		args.withArray("queries");
		args.withArray("sorts");
		JsonNode data = apiRequest(HttpMethod.POST,
				String.format("https://api.ding.qingflow.com/app/%s/apply/filter", app),
				token, null, args);

		Preconditions.checkArgument(data.at("/errCode").intValue() == 0);
		int pageAmount = data.at("/result/pageAmount").asInt();
		Stream<JsonNode> str = Stream.iterate(2, i -> i + 1)
				.limit(Math.max(pageAmount - 1, 0))
				.map(i -> {
					args.put("pageNum", i);
					return apiRequest(HttpMethod.POST,
							String.format("https://api.ding.qingflow.com/app/%s/apply/filter", app),
							token, null, args);
				});

		List<JsonNode> resultData = Stream.concat(Stream.of(data), str)
				.flatMap(d -> {
					Preconditions.checkArgument(d.at("/errCode").intValue() == 0);
					return Streams.stream(d.at("/result/result"));
				}).collect(Collectors.toList());

		return resultData;
	}

	/**
	 *
	 * @param form 轻流数据
	 * @param p ds数据
	 * @return
	 */
	private static Stream<ObjectNode> answers(Iterable<JsonNode> form, Row p) {
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
						case 12:	// 流程状态
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

	private static boolean compare(JsonNode answer1, JsonNode answer2) {
		throw new UnsupportedOperationException();
	}

	private static JsonNode apiRequest(String method, String uri, String token, String userId, JsonNode body) {
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
			return mapper.readTree(is);
		} catch (IOException e) {
			throw new RuntimeException(String.format("%s %s:%s", method, uri, body), e);
		}
	}

	private static JsonNode answerMixForm(JsonNode answer, JsonNode form) {
		Map<Integer, ObjectNode> form_ = Streams.stream(form)
				.flatMap(i -> Stream.concat(Stream.of(i), Streams.stream(i.at("/subQuestionBaseInfos"))))
				.collect(Collectors.toMap(i -> i.at("/queId").intValue(), i -> (ObjectNode) i));
		answer.forEach(i -> {
			int id = i.at("/queId").intValue();
			ObjectNode f = form_.get(id);
			((ObjectNode) i).setAll(f);
		});
		return answer;
	}

	private static Optional<JsonNode> user(String name, String token) {
		JsonNode user = requestCache.asMap().computeIfAbsent("https://api.ding.qingflow.com/department/1/user?fetchChild=true", uri -> {
			JsonNode r = apiRequest(HttpMethod.GET, uri, token, null, null);
			// System.out.println(r);
			Preconditions.checkArgument(r.path("errCode").intValue() == 0, r.path("errMsg").asText());
			return r.at("/result/userList");
		});
		return Streams.stream(user)
				.filter(i -> i.at("/name").asText().equals(name))
				.findAny();
	}

	private static JsonNode form(String app, String token) {
		return requestCache.asMap().computeIfAbsent(String.format("https://api.ding.qingflow.com/app/%s/form", app), uri -> {
			JsonNode form = apiRequest(HttpMethod.GET, uri, token, null, null);
			Preconditions.checkArgument(form.at("/errCode").asInt(500) == 0, form.at("/errMsg").asText());
			return form.at("/result");
		});
	}

}
