package boluo.work;

import com.clearspring.analytics.util.Lists;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.compress.utils.Charsets;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import javax.ws.rs.HttpMethod;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.*;

public class FromQingliu2 {

	private static final String baseUri = "https://api.ding.qingflow.com";
	private static final ObjectMapper mapper = new ObjectMapper();
	private static final Logger logger = LoggerFactory.getLogger(FromQingliu2.class);
	private static final Cache<String, JsonNode> requestCache = CacheBuilder.newBuilder().build();
	private static final CloseableHttpClient http = HttpClients.createDefault();

	public static void main(String[] args) throws Exception {
		SparkSession spark = SparkSession
				.builder()
				.master("local[*]")
				.appName("Simple Application")
				.getOrCreate();

		CommandLine cli = new GnuParser().parse(new Options()
				.addOption("ta", "api-token", true, "")
				.addOption("o", "output", true, ""), args);

		// 重要的量
		String token = cli.getOptionValue("ta", "638fb362-7ee9-4d85-9359-e92f5c7b353e");
		String localQingliuPath = cli.getOptionValue("o");

		StructType qingliuStruct = new StructType()
				.add("app_id", "string")
				.add("app_name", "string")
				.add("apply_id", "int")
				.add("log_id", "int")
				.add("create_time", "timestamp")
				.add("audit_name", "string")
				.add("audit_result", "int")
				.add("audit_time", "timestamp")
				.add("audit", new StructType()
						.add("uid", "long")
						.add("nickname", "string")
						.add("email", "string")
						.add("head", "string")
				)
				.add("before", "string")
				.add("after", "string");

		// 获取成员
		Optional<JsonNode> creator = user("小兰Robot", token);
		String userId = creator.map(i -> i.at("/userId").asText()).orElse(null);

		// 1.获取工作区所有应用
		JsonNode appIdResultNode = apiRequest(HttpMethod.GET, String.format("%s/app", baseUri),
				token, userId, null);

		JsonNode appIdResult = appIdResultNode.at("/result").withArray("appList");
		Map<String, String> appMap = Maps.newHashMap();
		for (JsonNode jn : appIdResult) {
			appMap.put(jn.at("/appKey").asText(), jn.at("/appName").asText());
		}

		for (String appId : appMap.keySet()) {

			// 查询本地轻流表
			Dataset<Row> df1 = spark.read().format("delta").load(localQingliuPath);
			List<Row> localApplyIdList;

			if (df1.count() == 0) {
				localApplyIdList = Lists.newArrayList();
			} else {
				df1.registerTempTable("qingliu");
				String sql = "select apply_id, log_id, audit_time, after\n" +
						"from\n" +
						"(\n" +
						"    select apply_id, log_id, audit_time, after, row_number() over(partition by apply_id order by log_id desc) rk\n" +
						"    from qingliu\n" +
						"    where app_id = '" + appId + "'" +
						") t\n" +
						"where t.rk = 1 and cast(log_id as string) != 'D' ";
				localApplyIdList = spark.sql(sql).collectAsList();
			}

			LocalDateTime localMaxUpdateTime;
			if(localApplyIdList.isEmpty()){
				Row maxRow = localApplyIdList.get(0);
				Timestamp maxLogTime = maxRow.getAs("audit_time");
				localMaxUpdateTime = maxLogTime.toLocalDateTime().minusHours(1);
			}else {
				localMaxUpdateTime = LocalDateTime.of(2016, 1, 1, 0, 0);
			}

			// 请求参数 - 分页查询
			ObjectNode requestBody = mapper.createObjectNode()
					.put("pageNum", "1")
					.put("pageSize", "1000");

			requestBody.withArray("sorts")
					.addObject()
					.put("queId", 3)
					.put("isAscend", false);

			// 分页获取应用的数据, 通过appId获取appId下所有的applyId
			JsonNode applyIdResultNode = apiRequest(HttpMethod.POST, String.format("%s/app/%s/apply/filter", baseUri, appId),
					token, userId, requestBody);

			int applyIdCount = applyIdResultNode.at("/result/resultAmount").asInt();
			JsonNode applyAnswersNode = applyIdResultNode.at("/result/result");

			Map<Integer, JsonNode> checkAnswersMap = Maps.newTreeMap();
			for (JsonNode jn : applyAnswersNode) {
				checkAnswersMap.put(jn.at("/applyId").asInt(), jn.at("/answers"));
			}

			// 判断2
			// 取checkAnswersMap中最后一条数据时间, 与localMaxUpdateTime比较
			JsonNode lastAnswersNode = (JsonNode) checkAnswersMap.values().toArray()[0];
			String lastTimeStr = null;
			for (JsonNode jn : lastAnswersNode) {
				if (jn.at("/queId").asInt() == 3) {
					lastTimeStr = jn.at("/values/0/value").asText();
				}
			}
			Preconditions.checkArgument(!Strings.isNullOrEmpty(lastTimeStr), "lastTimeStr为空");
			DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
			LocalDateTime lastTime = LocalDateTime.parse(lastTimeStr, format);
			// checkAnswersMap > localMaxUpdateTime

			for (int i = 2; lastTime.compareTo(localMaxUpdateTime) > 0; ++i) {

				ObjectNode tempRequestBody = mapper.createObjectNode();
				tempRequestBody.put("pageNum", i)
						.put("pageSize", "1000");

				tempRequestBody.withArray("sorts")
						.addObject()
						.put("queId", 3)
						.put("isAscend", false);

				// 分页获取应用的数据, 通过appId获取appId下所有的applyId
				JsonNode tempApplyIdResultNode = apiRequest(HttpMethod.POST, String.format("%s/app/%s/apply/filter", baseUri, appId),
						token, userId, tempRequestBody);

				JsonNode tempApplyAnswersNode = tempApplyIdResultNode.at("/result/result");
				for (JsonNode jn : tempApplyAnswersNode) {
					checkAnswersMap.put(jn.at("/applyId").asInt(), jn.at("/answers"));
				}

				// 取checkAnswersMap中最后一条数据时间, 与localMaxUpdateTime比较
				JsonNode lastAnswersNode1 = (JsonNode) checkAnswersMap.values().toArray()[0];
				String lastTimeStr1 = null;
				for (JsonNode jn : lastAnswersNode1) {
					if (jn.at("/queId").asInt() == 3) {
						lastTimeStr1 = jn.at("/values/0/value").asText();
					}
				}
				Preconditions.checkArgument(!Strings.isNullOrEmpty(lastTimeStr1), "lastTimeStr为空");
				DateTimeFormatter format1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
				LocalDateTime lastTime1 = LocalDateTime.parse(lastTimeStr1, format1);

				if (i >= tempApplyIdResultNode.at("/result/pageAmount").asInt()) {
					break;
				}

				lastTime = lastTime1;
			}

			// 判断3 是否有删除的
			// 先找出线上有的数据, 但是本地没有的applyId
			// TODO 删除Map
//			Map<Integer, Tuple2<Integer, String>> localApplyIdMap = Maps.newHashMap();
//			localApplyIdList.toLocalIterator().forEachRemaining(row -> {
//				localApplyIdMap.put(row.getAs(0), Tuple2.apply(row.getAs(1), row.getAs(3)));
//			});

            Set<Integer> currAddApplyIdSet = Sets.difference(
                    checkAnswersMap.keySet(),
                    localApplyIdList.stream().map(i -> i.getAs(0)).collect(Collectors.toSet()));

			Preconditions.checkArgument(localApplyIdList.size() + currAddApplyIdSet.size() >= applyIdCount, "数据异常...appId为: " + appId);
			if (localApplyIdList.size() + currAddApplyIdSet.size() == applyIdCount) {
				// 没有删除的
			} else {
				// 有删除的, 处理
				ObjectNode deleteRequestBody = mapper.createObjectNode()
						.put("pageNum", 1)
						.put("pageSize", "1000");

				deleteRequestBody.withArray("sorts")
						.addObject()
						.put("queId", 3)
						.put("isAscend", false);

				deleteRequestBody.withArray("queries")
						.addObject()
						.put("queId", 3)
						.put("minValue", "2019-12-11 16:10:30");

				// 获取剩下所有的数据, 更新到checkAnswersMap中
				JsonNode data1 = apiRequest(HttpMethod.POST, String.format("%s/app/%s/apply/filter", baseUri, appId),
						token, userId, deleteRequestBody);

				Preconditions.checkArgument(data1.at("/errCode").intValue() == 0);
				int pageAmount = data1.at("/result/pageAmount").asInt();
				Stream<JsonNode> data2 = Stream.iterate(2, j -> j + 1)
						.limit(Math.max(pageAmount - 1, 0))
						.map(k -> {
							deleteRequestBody.put("pageNum", k);
							return apiRequest(HttpMethod.POST, String.format("%s/app/%s/apply/filter", baseUri, appId),
									token, userId, deleteRequestBody);
						});

				Stream.concat(Stream.of(data1), data2)
						.forEach(r -> {
							JsonNode jsonNode = r.at("/result/result");
							for (JsonNode jn : jsonNode) {
								checkAnswersMap.put(jn.at("/applyId").asInt(), jn.at("/answers"));
							}
						});

				// 此时的checkAnswersMap中拥有线上全量的applyId

				// 本地的 - 线上全部的 = 删除的
//				Set<Integer> deleteApplyIdSet = Sets.newCopyOnWriteArraySet(localApplyIdMap.keySet());
//				deleteApplyIdSet.removeAll(checkAnswersMap.keySet());

                Set<Integer> deleteApplyId = Sets.difference(
                        localApplyIdList.stream().map(i -> (Integer) i.getAs(0)).collect(Collectors.toSet()),
                        checkAnswersMap.keySet()
                );

                for (Integer applyId : deleteApplyId) {
					// TODO 给这些 applyId 添加删除行, logId = 'D'

				}
			}

			List<Row> updateRowList = Lists.newArrayList();
			// 处理添加的
			for (Integer applyId : checkAnswersMap.keySet()) {

				// 调用: 获取单条数据的流程日志  第一条applyId 695872
				JsonNode qingliuLogIdNode = apiRequest(HttpMethod.GET, String.format("%s/apply/%s/auditRecord", baseUri, applyId),
						token, userId, null);

				// 存放添加的logId对应的节点
				List<JsonNode> updateLogIdList = Lists.newArrayList();
				// TODO localApplyIdMap == null

                int localMaxLogId = -1;
                String onlineBeforeStr = null;
                for (Row row : localApplyIdList) {
                    if(applyId == row.getAs(0)){
                        localMaxLogId = row.getAs(1);
                        onlineBeforeStr = row.getAs(3);
                    }
                }

				// 获取qingliuLogIdNode中所有的qingliuLogId
				for (JsonNode jn : qingliuLogIdNode.at("/result/auditRecords")) {
					int qingliuLogId = jn.at("/auditRcdId").asInt();
					if (qingliuLogId > localMaxLogId) {
						updateLogIdList.add(jn);
					}
				}

				ArrayNode onlineBefore = Strings.isNullOrEmpty(onlineBeforeStr)
						? mapper.createArrayNode()
						: (ArrayNode) mapper.readTree(onlineBeforeStr);

				List<Row> tempUpdateRowList = Lists.newArrayList();
				for (JsonNode jn : updateLogIdList) {

					Integer logId = jn.at("/auditRcdId").asInt();
					//调用: 获取某条流程日志的详细信息
					JsonNode resultNode = apiRequest(HttpMethod.GET, String.format("%s/apply/%s/auditRecord/%s", baseUri, applyId, logId),
							token, userId, null);

					ArrayNode beforeAnswer = getPatchAnswer((ArrayNode) resultNode.at("/auditModifies"), "beforeAnswer");
					ArrayNode afterAnswer = getPatchAnswer((ArrayNode) resultNode.at("/auditModifies"), "afterAnswer");

					// 第一次校验
					assertCompare(onlineBefore, replace(onlineBefore, beforeAnswer));

					// 替换
					ArrayNode onlineAfter = replace(onlineBefore, afterAnswer);

					String createTimeStr = "";
					for (JsonNode node : checkAnswersMap.get(applyId)) {
						if (node.at("/queId").asInt() == 2) {
							createTimeStr = node.at("/values/0/value").asText();
						}
					}
					Preconditions.checkArgument(!Strings.isNullOrEmpty(createTimeStr), "申请时间为空..., applyId为: " + applyId);

					Timestamp createTime = Timestamp.valueOf(createTimeStr);
					String auditName = jn.at("/auditNodeName").asText();
					Integer auditResult = jn.at("/auditResult").asInt();
					long audit_unix_timestamp = jn.at("/auditTime").asLong();
					Timestamp auditTime = Timestamp.from(Instant.ofEpochMilli(audit_unix_timestamp));

					long uid = jn.at("/auditUser/uid").asLong();
					String nickName = jn.at("/auditUser/nickName").asText();
					String email = jn.at("/auditUser/email").asText();
					String head = jn.at("/auditUser/headImg").asText();
					String onlineBefore_ = Objects.isNull(onlineBefore) ? null : onlineBefore.toString();
					String onlineAfter_ = Objects.isNull(onlineAfter) ? null : afterAnswer.toString();
					Row auditRow = RowFactory.create(uid, nickName, email, head);

					Row lineRow = RowFactory.create(appId, appMap.get(appId), applyId, logId, createTime, auditName,
							auditResult, auditTime, auditRow, onlineBefore_, onlineAfter_);
					// 存储...
					tempUpdateRowList.add(lineRow);

					onlineBefore = onlineAfter;
				}

				// 二次检查
				Row lastRow = tempUpdateRowList.get(tempUpdateRowList.size() - 1);
				String lastAfterStr = lastRow.getAs("after");
				JsonNode lastAfter = mapper.readTree(lastAfterStr);
				JsonNode onlineAfter = checkAnswersMap.get(applyId);

				assertCompare((ArrayNode) lastAfter, (ArrayNode) onlineAfter);
				System.out.println("二次比较完成!!!");
				// Dataset<Row> ds = spark.createDataFrame(tempUpdateRowList, qingliuStruct);

			}

		}


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


	/**
	 * update data to checkAnswersMap
	 *
	 * @param map
	 * @param condition
	 */
	private static void updateCheckAnswersMap(Map<Integer, JsonNode> map, ObjectNode condition) {

	}

	private static void assertCompare(ArrayNode answer1, ArrayNode answer2) throws JsonProcessingException {

		if (Objects.isNull(answer1) && Objects.isNull(answer2)) {
			return;
		}

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
				br = false;
			}
		}

		if (!br) {
			logger.error("answer1: " + mapper.writerWithDefaultPrettyPrinter().writeValueAsString(answer1));
			logger.error("answer2: " + mapper.writerWithDefaultPrettyPrinter().writeValueAsString(answer2));
			throw new RuntimeException("校验结果不正确...");
		}

	}


	private static ArrayNode replace(ArrayNode answer1, ArrayNode answer2) {

		if (Objects.isNull(answer1) && Objects.isNull(answer2)) {
			return null;
		}

		if (Objects.isNull(answer1)) {
			return answer2;
		}

		ArrayNode resultAnswer = answer1.deepCopy();
		Map<String, JsonNode> answer2Map = Maps.newHashMap();

		for (JsonNode jn : answer2) {
			String queId = jn.at("/queId").asText();
			JsonNode values = jn.at("/values");
			answer2Map.put(queId, values);
		}

		for (JsonNode jn : resultAnswer) {
			String queId = jn.at("/queId").asText();
			if (answer2Map.containsKey(queId)) {
				JsonNode newValue = answer2Map.get(queId);
				ObjectNode obj = (ObjectNode) jn;
				obj.set("values", newValue);
			}
		}

		return resultAnswer;
	}

	private static ArrayNode getPatchAnswer(ArrayNode modifies, String key) {
		ArrayNode arrayNode = mapper.createArrayNode();

		for (JsonNode jn : modifies) {
			// TODO
			if (!Objects.isNull(jn.at("/" + key + "/values")) && !Strings.isNullOrEmpty(jn.at("/" + key + "/values").toString())) {

				ObjectNode objectNode = mapper.createObjectNode()
						.putPOJO("queId", jn.at("/queId"))
						.putPOJO("queTitle", jn.at("/queTitle"))
						.putPOJO("queType", jn.at("/queType"))
						.putPOJO("values", jn.at("/" + key + "/values"));

				arrayNode.add(objectNode);
			}
		}

		return arrayNode;
	}

}
