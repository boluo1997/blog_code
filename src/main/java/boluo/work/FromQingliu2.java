package boluo.work;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.*;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.compress.utils.Charsets;
import org.apache.http.HttpEntity;
import org.apache.http.auth.AuthenticationException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
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
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.spark.sql.functions.*;

public class FromQingliu2 {

	private static final String baseUri = "https://api.ding.qingflow.com";
	private static final String baseWebUri = "https://ding.qingflow.com/api";
	private static final ObjectMapper mapper = new ObjectMapper();
	private static final Logger logger = LoggerFactory.getLogger(FromQingliu2.class);
	private static final Cache<String, JsonNode> requestCache = CacheBuilder.newBuilder().build();
	private static final CloseableHttpClient http;

	static {
		PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
		cm.setMaxTotal(2);
		cm.setDefaultMaxPerRoute(2);
		http = HttpClients.custom()
				.setConnectionManager(cm)
				.build();
	}

	public static void main(String[] args) throws Exception {
		SparkSession spark = SparkSession.builder()
				.config("spark.maxRemoteBlockSizeFetchToMem", "64m")
				.getOrCreate();

		CommandLine cli = new GnuParser().parse(new Options()
				.addOption("ta", "api-token", true, "")
				.addOption("tw", "web-token", true, "")
				.addOption("o", "output", true, ""), args);

		// ????????????
		String apiToken = cli.getOptionValue("ta", "638fb362-7ee9-4d85-9359-e92f5c7b353e");
		String webToken = cli.getOptionValue("tw", "b4879ecf-6228-4921-8826-394114729bb4");
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
				.add("auditor", new StructType()
						.add("uid", "long")
						.add("nickname", "string")
						.add("email", "string")
						.add("head", "string")
				)
				.add("after", "string");

		// ????????????
		Optional<JsonNode> creator = user("??????Robot", apiToken);
		String userId = creator.map(i -> i.at("/userId").asText()).orElse(null);

		// 1.???????????????????????????
		JsonNode appIdResultNode = apiRequest(HttpMethod.GET, String.format("%s/app", baseUri),
				apiToken, userId, null);

		JsonNode appIdResult = appIdResultNode.at("/result").withArray("appList");
		Map<String, String> appMap = Maps.newHashMap();
		for (JsonNode jn : appIdResult) {
			appMap.put(jn.at("/appKey").asText(), jn.at("/appName").asText());
		}

		List<Row> writeList = Lists.newArrayList();
		Dataset<Row> writeData = createDataFrame(writeList, qingliuStruct.add("update_time", "timestamp"));

		// ????????????appId???apply_id???????????????update_time
		Dataset<Row> df = spark.read().format("delta").load(localQingliuPath);
		Dataset<Row> appIdDs = df.groupBy(expr("app_id"))
				.agg(countDistinct("apply_id").as("countApplyId"),
						max("update_time").as("update_time"));

		Map<String, Row> appIdMap = appIdDs.collectAsList()
				.stream()
				.collect(Collectors.toMap(i -> i.getAs("app_id"), j -> j));

		for (String appId : appMap.keySet()) {

			long localApplyIdCount = appIdMap.containsKey(appId)
					? appIdMap.get(appId).getAs("countApplyId")
					: 0;

			LocalDateTime localMaxUpdateTime = appIdMap.containsKey(appId)
					? appIdMap.get(appId).<Timestamp>getAs("updateTime").toLocalDateTime()
					: LocalDateTime.of(2016, 1, 1, 0, 0);

			LocalDateTime insertUpdateTime = LocalDateTime.now();
			String insertTimeStr = String.valueOf(insertUpdateTime.atZone(ZoneId.systemDefault()).toEpochSecond());

			// ???????????? - ????????????
			ObjectNode requestBody = mapper.createObjectNode()
					.put("pageNum", "1")
					.put("pageSize", "1000")
					.put("type", 8);

			requestBody.withArray("sorts")
					.addObject()
					.put("queId", 3)
					.put("isAscend", false);
			requestBody.withArray("sorts")
					.addObject()
					.put("queId", 0)
					.put("isAscend", false);

			// ???????????????????????????, ??????appId??????appId????????????applyId
			JsonNode filterResult = apiRequest(HttpMethod.POST, String.format("%s/app/%s/apply/filter", baseUri, appId),
					apiToken, userId, requestBody);

			// ???????????????????????????audit_time, ????????????????????????
			LocalDateTime checkMaxTime = filterResult.at("/result/result").size() != 0 && Objects.nonNull(getValue((ArrayNode) filterResult.at("/result/result/0/answers"), "????????????"))
					? getValue((ArrayNode) filterResult.at("/result/result/0/answers"), "????????????")
					: LocalDateTime.of(2100, 1, 1, 0, 0);

			int applyIdCount = filterResult.at("/result/resultAmount").asInt();
			JsonNode applyAnswersNode = filterResult.at("/result/result");

			Map<Integer, ArrayNode> checkAnswersMap = Maps.newTreeMap();
			for (JsonNode jn : applyAnswersNode) {
				checkAnswersMap.put(jn.at("/applyId").asInt(), (ArrayNode) jn.at("/answers"));
			}

			// ??????2
			// ???checkAnswersMap???????????????????????????, ???localMaxUpdateTime??????
			ArrayNode lastAnswersNode = Iterables.getFirst(checkAnswersMap.values(), null);
			LocalDateTime lastTime = Objects.nonNull(lastAnswersNode)
					? getValue(lastAnswersNode, "????????????")
					: LocalDateTime.of(2016, 1, 1, 0, 0);

			// checkAnswersMap >= localMaxUpdateTime
			for (int i = 2; lastTime.compareTo(localMaxUpdateTime) >= 0; ++i) {

				ObjectNode tempRequestBody = mapper.createObjectNode();
				tempRequestBody.put("pageNum", i)
						.put("pageSize", "1000")
						.put("type", 8);

				tempRequestBody.withArray("sorts")
						.addObject()
						.put("queId", 3)
						.put("isAscend", false);
				tempRequestBody.withArray("sorts")
						.addObject()
						.put("queId", 0)
						.put("isAscend", false);

				// ???????????????????????????, ??????appId??????appId????????????applyId
				JsonNode nextFilterResult = apiRequest(HttpMethod.POST, String.format("%s/app/%s/apply/filter", baseUri, appId),
						apiToken, userId, tempRequestBody);

				JsonNode tempApplyAnswersNode = nextFilterResult.at("/result/result");
				for (JsonNode jn : tempApplyAnswersNode) {
					checkAnswersMap.put(jn.at("/applyId").asInt(), (ArrayNode) jn.at("/answers"));
				}

				// ???checkAnswersMap???????????????????????????, ???localMaxUpdateTime??????
				ArrayNode lastAnswersNode1 = Iterables.getFirst(checkAnswersMap.values(), null);
				LocalDateTime lastTime1 = Objects.nonNull(lastAnswersNode1)
						? getValue(lastAnswersNode1, "????????????")
						: LocalDateTime.of(2016, 1, 1, 0, 0);

				if (i >= nextFilterResult.at("/result/pageAmount").asInt()) {
					break;
				}
				lastTime = lastTime1;
			}

			// ??????checkAnswersMap????????????????????????localMaxUpdateTime?????????
			Optional<ArrayNode> isAdd = checkAnswersMap.values().stream().filter(i -> localMaxUpdateTime.compareTo(getValue(i, "????????????")) <= 0).findAny();
			long addCount = 0;

			if (isAdd.isPresent()) {
				// ????????????: ???????????????
				Dataset<Row> localApplyIdDs = df.where(String.format("app_id = '%s'", appId))
						.distinct()
						.select("apply_id");

				// ??????applyId
				List<Row> onlineApplyIdList = Lists.newArrayList();
				for (Integer applyId : checkAnswersMap.keySet()) {
					Row row = RowFactory.create(applyId);
					onlineApplyIdList.add(row);
				}
				Dataset<Row> onlineApplyIdDs = spark.createDataFrame(onlineApplyIdList, new StructType().add("apply_id", "int"));

				// ???????????????applyId??????
				addCount = onlineApplyIdDs.except(localApplyIdDs).count();

			}

			// ??????: ?????? + ?????? >= ??????
			Preconditions.checkArgument(localApplyIdCount + addCount >= applyIdCount,
					"????????????...appId???: " + appId +
							", ???????????????: " + localApplyIdCount +
							", ???????????????: " + addCount +
							", ????????????: " + applyIdCount +
							", checkAnswersMap??????" + checkAnswersMap.size()
			);

			// ????????????????????????
			List<Row> deleteLine = Lists.newArrayList();
			if (localApplyIdCount + addCount > applyIdCount) {
				// ????????????, ??????
				ObjectNode deleteRequestBody = mapper.createObjectNode()
						.put("pageNum", 1)
						.put("pageSize", "1000")
						.put("type", 8);

				deleteRequestBody.withArray("sorts")
						.addObject()
						.put("queId", 3)
						.put("isAscend", false);
				deleteRequestBody.withArray("sorts")
						.addObject()
						.put("queId", 0)
						.put("isAscend", false);

				deleteRequestBody.withArray("queries")
						.addObject()
						.put("queId", 3)
						// "2019-12-11 16:10:30"
						.put("minValue", lastTime.toString());

				// ???????????????????????????, ?????????checkAnswersMap???
				JsonNode afterApplyIdResult = apiRequest(HttpMethod.POST, String.format("%s/app/%s/apply/filter", baseUri, appId),
						apiToken, userId, deleteRequestBody);

				Preconditions.checkArgument(afterApplyIdResult.at("/errCode").intValue() == 0);
				int pageAmount = afterApplyIdResult.at("/result/pageAmount").asInt();
				Stream<JsonNode> data2 = Stream.iterate(2, j -> j + 1)
						.limit(Math.max(pageAmount - 1, 0))
						.map(k -> {
							deleteRequestBody.put("pageNum", k);
							return apiRequest(HttpMethod.POST, String.format("%s/app/%s/apply/filter", baseUri, appId),
									apiToken, userId, deleteRequestBody);
						});

				Stream.concat(Stream.of(afterApplyIdResult), data2)
						.forEach(r -> {
							JsonNode jsonNode = r.at("/result/result");
							for (JsonNode jn : jsonNode) {
								checkAnswersMap.put(jn.at("/applyId").asInt(), (ArrayNode) jn.at("/answers"));
							}
						});

				// ?????????checkAnswersMap????????????????????????applyId
				// ????????? - ??????????????? = ?????????

				// ????????????: ???????????????
				Dataset<Row> localApplyIdDs = df.where(String.format("app_id = '%s'", appId))
						.distinct()
						.select(expr("apply_id"));

				// ??????applyId
				List<Row> onlineApplyIdList = Lists.newArrayList();
				for (Integer applyId : checkAnswersMap.keySet()) {
					Row row = RowFactory.create(applyId);
					onlineApplyIdList.add(row);
				}
				Dataset<Row> onlineApplyIdDs = spark.createDataFrame(onlineApplyIdList, new StructType().add("apply_id", "int"));
				Stream<Row> deleteApplyId = localApplyIdDs.except(onlineApplyIdDs).collectAsList().stream();

				deleteApplyId.forEach(i -> {
					int applyId = i.getAs(0);
					Row row = RowFactory.create(appId, appMap.get(appId), applyId, Integer.MAX_VALUE, null, "??????", -1, Timestamp.valueOf(insertUpdateTime), null, null);
					deleteLine.add(row);
				});


				// ???????????????
				Dataset<Row> deleteData = spark.createDataFrame(deleteLine, qingliuStruct)
						.withColumn("update_time", to_timestamp(from_unixtime(expr(insertTimeStr))))
						.coalesce(1);

//				Outputs.replace(deleteData, localQingliuPath,
//						expr(String.format("t.app_id='%s' and s.apply_id=t.apply_id and s.log_id=t.log_id", appId)),
//						"app_id");
			}

			// ??????checkAnswerMap???????????????localMaxUpdateTime?????????
			Set<Integer> deleteSet = checkAnswersMap.entrySet().stream()
					.filter(kv -> {
						LocalDateTime currTime = getValue(kv.getValue(), "????????????");
						return currTime.compareTo(localMaxUpdateTime) < 0;
					})
					.map(Map.Entry::getKey)
					.collect(Collectors.toSet());
			checkAnswersMap.keySet().removeAll(deleteSet);

			// ????????????????????????????????????
			if (checkAnswersMap.isEmpty()) {
				continue;
			}

			// ???????????????
			List<Row> updateRowList = addQingliu(appId, appMap.get(appId), checkAnswersMap, webToken);

			// ?????????updateRowList???audit_time??????checkMaxTime?????????
			List<Row> filterRowList = updateRowList.stream()
					.filter(i -> {
						return (((Timestamp) i.getAs(7)).toLocalDateTime()).compareTo(checkMaxTime) <= 0;
					}).collect(Collectors.toList());

			// ??????
			Dataset<Row> tempWriteData = createDataFrame(filterRowList, qingliuStruct)
					.withColumn("update_time", to_timestamp(from_unixtime(expr(insertTimeStr))));
			writeData = writeData.unionAll(tempWriteData);

		}

//		Outputs.replace(writeData, localQingliuPath,
//				expr("s.app_id=t.app_id and s.apply_id=t.apply_id and s.log_id=t.log_id"),
//				"app_id");
	}

	private static Dataset<Row> createDataFrame(List<Row> rows, StructType schema) {
		if (rows.isEmpty()) {
			return SparkSession.active().createDataFrame(rows, schema);
		}
		int blockSize = 500;
		Optional<Dataset<Row>> result = Stream.iterate(0, i -> i + blockSize)
				.limit((rows.size() + blockSize - 1) / blockSize)
				.map(i -> SparkSession.active().createDataFrame(
						rows.subList(i, Math.min(i + blockSize, rows.size())),
						schema))
				.reduce(Dataset::unionAll);
		Preconditions.checkArgument(result.isPresent());
		return result.get();
	}

	private static List<Row> addQingliu(String appId, String appName, Map<Integer, ArrayNode> checkAnswersMap,
										String webToken) throws IOException {

		List<Row> updateRowList = Lists.newArrayList();
		for (Integer applyId : checkAnswersMap.keySet()) {

			// ??????: ?????????????????????????????????
			JsonNode qingliuLogIdList = webRequest(webToken, HttpMethod.GET,
					String.format("%s/app/%s/apply/%d/auditRecord?role=1", baseWebUri, appId, applyId),
					String.format("https://ding.qingflow.com/arch/app/%s/all;type=8?applyId=%d", appId, applyId),
					null
			);
			ObjectNode qingliuLogIdNode = formatAuditRecord(qingliuLogIdList);
			ArrayNode onlineBefore = mapper.createArrayNode();

			// ????????????
			List<JsonNode> noNodeList = Streams.stream(checkAnswersMap.get(applyId))
					.filter(i -> i.at("/queId").asInt() == 0)
					.collect(Collectors.toList());
			Preconditions.checkArgument(noNodeList.size() > 0, "????????????????????????, appId???: " + appId + ", applyId???: " + applyId);
			JsonNode noNode = noNodeList.get(0);
			onlineBefore.add(noNode);

			// ???????????????
			if (!qingliuLogIdNode.at("/result/auditRecords/0/auditUser").isNull()
					&& !Strings.isNullOrEmpty(qingliuLogIdNode.at("/result/auditRecords/0/auditUser/nickName").asText())
					&& !qingliuLogIdNode.at("/result/auditRecords/0/auditUser/nickName").asText().equals("null")) {
				ObjectNode auditUserNode = (ObjectNode) qingliuLogIdNode.at("/result/auditRecords/0/auditUser");
				ObjectNode formatUserNode = formatUserNode(auditUserNode);
				onlineBefore.add(formatUserNode);
			}

			// ??????logId???????????????, ?????????online???????????????????????????
			List<JsonNode> updateLogIdList = Lists.newArrayList();

			// ??????qingliuLogIdList????????????qingliuLogId
			for (JsonNode jn : qingliuLogIdNode.at("/result/auditRecords")) {
				updateLogIdList.add(jn);
			}

			List<Row> tempUpdateRowList = Lists.newArrayList();
			for (JsonNode jn : updateLogIdList) {

				Integer logId = jn.at("/auditRcdId").asInt();
				ObjectNode webAuditRecordResult = webRequest(webToken, HttpMethod.GET,
						String.format("%s/app/%s/apply/%d/auditRecord/%d?role=1", baseWebUri, appId, applyId, logId),
						String.format("https://ding.qingflow.com/arch/app/%s/all;type=8?applyId=%d", appId, applyId),
						null
				);

				ObjectNode auditRecordResult = formatAuditRecordDetail(webAuditRecordResult);

				ArrayNode beforePatch = getPatchAnswer((ArrayNode) auditRecordResult.at("/auditModifies"), "beforeAnswer", "beforeAnswer");
				ArrayNode afterPatch = getPatchAnswer((ArrayNode) auditRecordResult.at("/auditModifies"), "beforeAnswer", "afterAnswer");

				// ???????????????
				assertCompare(onlineBefore, replace(onlineBefore, beforePatch));

				// ??????
				ArrayNode onlineAfter = replace(onlineBefore, afterPatch);

				String createTimeStr = "";
				for (JsonNode node : checkAnswersMap.get(applyId)) {
					if (node.at("/queId").asInt() == 2) {
						createTimeStr = node.at("/values/0/value").asText();
					}
				}
				Preconditions.checkArgument(!Strings.isNullOrEmpty(createTimeStr), "??????????????????..., applyId???: " + applyId);

				Timestamp createTime = Timestamp.valueOf(createTimeStr);
				String auditName = jn.at("/auditNodeName").asText();
				Integer auditResult = jn.at("/auditResult").asInt();
				long audit_unix_timestamp = jn.at("/auditTime").asLong();
				Timestamp auditTime = Timestamp.from(Instant.ofEpochMilli(audit_unix_timestamp));

				long uid = jn.at("/auditUser/uid").asLong();
				String nickName = jn.at("/auditUser/nickName").asText();
				String email = jn.at("/auditUser/email").asText();
				String head = jn.at("/auditUser/headImg").asText();
				String onlineAfter_ = Objects.isNull(onlineAfter) ? null : onlineAfter.toString();

				Row auditRow = uid == 0L ? null : RowFactory.create(uid, nickName, email, head);
				Row lineRow = RowFactory.create(appId, appName, applyId, logId, createTime, auditName,
						auditResult, auditTime, auditRow, onlineAfter_);

				tempUpdateRowList.add(lineRow);
				onlineBefore = onlineAfter;
			}

			// ????????????
			ArrayNode lastAfter = onlineBefore;
			ArrayNode onlineAfter = checkAnswersMap.get(applyId);
			// onlineAfter ??????queId in (2,3,4)
			ArrayNode onlineAfterFiltered = mapper.createArrayNode();
			StreamSupport.stream(onlineAfter.spliterator(), false)
					.filter(i -> i.at("/queId").asInt() > 4 || i.at("/queId").asInt() < 2)
					.forEach(onlineAfterFiltered::add);

			assertCompare(onlineAfterFiltered, lastAfter);
			updateRowList.addAll(tempUpdateRowList);
		}

		return updateRowList;
	}

	private static ObjectNode formatUserNode(ObjectNode auditUserNode) {

		ObjectNode result = mapper.createObjectNode();
		result.putNull("associatedQueType")
				.put("queId", 1)
				.put("queTitle", "?????????")
				.put("queType", 5);
		result.withArray("referValues");
		result.putNull("supId");
		result.withArray("tableValues");
		ArrayNode values = result.withArray("values");

		ObjectNode value = mapper.createObjectNode();
		value.put("dataValue", auditUserNode.at("/nickName").asText())
				.put("email", auditUserNode.at("/email").asText())
				.put("id", auditUserNode.at("/uid").asInt())
				.putNull("ordinal")
				.put("otherInfo", auditUserNode.at("/headImg").asText())
				.putNull("pluginValue")
				.put("queId", 1)
				.set("value", auditUserNode.at("/nickName"));
		values.add(value);

		return result;
	}

	private static ObjectNode formatAuditRecord(JsonNode webQingliuLogIdNode) {
		return (ObjectNode) webQingliuLogIdNode;
	}

	private static ObjectNode formatAuditRecordDetail(ObjectNode webAuditRecordResult) {
		ObjectNode tempFormatAuditRecordResult = webAuditRecordResult.retain("auditModifys", "auditRcdId");
		tempFormatAuditRecordResult.set("auditModifies", webAuditRecordResult.at("/auditModifys"));
		return tempFormatAuditRecordResult.retain("auditModifies", "auditRcdId");
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
			logger.info("{} {}:{}", method, uri, body);
			Preconditions.checkArgument(response.getStatusLine().getStatusCode() == 200, response.getStatusLine());
			return mapper.readTree(is);
		} catch (IOException e) {
			throw new RuntimeException(String.format("%s %s:%s", method, uri, body), e);
		}
	}

	private static ObjectNode webRequest(String token, String method, String uri, String referer, HttpEntity body) {

		RequestBuilder requestBuilder = RequestBuilder.create(method)
				.setUri(uri)
				.setHeader("token", token)
				.setHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36 Edg/80.0.361.69");
		Optional.ofNullable(body).ifPresent(i -> {
			requestBuilder
					.setHeader("Referer", referer);
		});
		Optional.ofNullable(body).ifPresent(i -> {
			requestBuilder
					.setHeader("Content-Type", "application/json")
					.setEntity(i);
		});

		try (CloseableHttpClient http = HttpClients.createDefault();
			 CloseableHttpResponse response = http.execute(requestBuilder.build());
			 InputStream is = response.getEntity().getContent()) {
			Thread.sleep(0);
			JsonNode r = mapper.readTree(is);
			if (r.at("/statusCode").intValue() == 40001) {
				throw new AuthenticationException("web????????????...");
			}
			logger.info("{} {}", method, uri);
			Preconditions.checkArgument(response.getStatusLine().getStatusCode() == 200, response.getStatusLine());
			if (r.isObject()) {
				return (ObjectNode) r;
			} else {
				ObjectNode obj = mapper.createObjectNode();
				obj.put("errCode", 0).put("errMsg", "");
				ArrayNode auditRecords = obj.with("result").withArray("auditRecords");
				ArrayNode currentNodes = obj.with("result").withArray("currentNodes");
				for (JsonNode jn : r) {
					if (jn.at("/auditResult").asInt() == 8) {
						currentNodes.add(jn);
					} else {
						auditRecords.add(jn);
					}
				}

				return obj;
				// throw new UnsupportedOperationException("web??????????????????Object...");
			}
		} catch (IOException | AuthenticationException e) {
			throw new RuntimeException(e);
		} // ????????????
		catch (InterruptedException e) {
			e.printStackTrace();
		}

		throw new UnsupportedOperationException();
	}


	// title???????????????????????????
	private static void assertCompare(ArrayNode answer1, ArrayNode answer2) throws JsonProcessingException {

		if (Objects.isNull(answer1) && Objects.isNull(answer2)) {
			return;
		}

		ArrayNode answerFilter1 = mapper.createArrayNode();
		ArrayNode answerFilter2 = mapper.createArrayNode();
		Streams.stream(answer1)
				.filter(i -> !i.at("/values").isArray() || i.at("/values").size() != 0)
				.forEach(answerFilter1::add);

		Streams.stream(answer2)
				.filter(i -> !i.at("/values").isArray() || i.at("/values").size() != 0)
				.forEach(answerFilter2::add);

		if (answerFilter1.size() == 0 && answerFilter2.size() == 0) {
			return;
		}

		if (answerFilter1.size() == 0 && answerFilter2.size() > 0) {
			Optional<JsonNode> br_value = Streams.stream(answerFilter2).filter(i -> {
				return !i.at("/values").isNull();
			}).findAny();
			if (!br_value.isPresent()) {
				return;
			}
		}

		boolean br = true;

		Set<String> idSet = Sets.newTreeSet();
		for (JsonNode jn1 : answerFilter1) {
			idSet.add(jn1.at("/queId").asText());
		}

		if (answerFilter2.size() == 0) {
			br = false;
		}

		for (JsonNode jn2 : answerFilter2) {
			if (idSet.contains(jn2.at("/queId").asText())) {
				for (JsonNode jn1 : answerFilter1) {
					if (jn2.at("/queId").asText().equals(jn1.at("/queId").asText())) {

						if (!(jn1.at("/queTitle").getNodeType() == JsonNodeType.STRING && jn2.at("/queTitle").getNodeType() == JsonNodeType.STRING)) {
							throw new RuntimeException("queTitle????????????string...");
						}
						if (Strings.isNullOrEmpty(jn1.at("/queTitle").asText()) || Strings.isNullOrEmpty(jn2.at("/queTitle").asText())) {
							throw new RuntimeException("queTitle??????...");
						}

						// ??????title???
						br = br && jn1.at("/queTitle").asText().equals(jn2.at("/queTitle").asText());

						if (!jn1.at("/values").isMissingNode() && !jn2.at("/values").isMissingNode()) {
							br = br && compareValues((ArrayNode) jn1.at("/values"), (ArrayNode) jn2.at("/values"));
						}

						if (!jn2.at("/tableValues").isMissingNode() || !jn1.at("/tableValues").isMissingNode()) {

							Preconditions.checkArgument(jn1.at("/tableValues").size() == jn2.at("/tableValues").size(),
									"????????????????????????...jn1: " + jn1.at("/tableValues") +
											", jn2: " + jn2.at("/tableValues"));
							if (jn2.at("/tableValues").size() > 0) {
								compareTable((ArrayNode) jn1.at("/tableValues"), (ArrayNode) jn2.at("/tableValues"));
							}
						}
					}
				}
			} else if (jn2.at("/values").isArray() && jn2.at("/values").size() != 0) {
				// ??????answer2????????????1?????????, ???values??????null, ???????????????
				br = false;
			}
		}

		if (!br) {
			logger.error("answer1: " + mapper.writerWithDefaultPrettyPrinter().writeValueAsString(answerFilter1));
			logger.error("answer2: " + mapper.writerWithDefaultPrettyPrinter().writeValueAsString(answerFilter2));
			throw new RuntimeException("?????????????????????...");
		}

	}

	private static void compareTable(ArrayNode table1, ArrayNode table2) throws JsonProcessingException {

		for (int i = 0; i < table1.size(); i++) {
			if (table1.get(i).isArray() && table2.get(i).isArray()) {
				compareTable((ArrayNode) table1.get(i), (ArrayNode) table2.get(i));
			} else {
				assertCompare(table1, table2);
			}
		}

	}

	private static boolean compareValues(ArrayNode answer1, ArrayNode answer2) {

		boolean br = true;

		if (answer1.size() != answer2.size()) {
			br = false;
		}

		for (int i = 0; i < answer1.size(); i++) {
			for (int j = 0; j < answer2.size(); j++) {
				if (i == j) {
					br = br && Objects.equals(answer1.get(i).at("/value").asText(), answer2.get(i).at("/value").asText());
				}
			}
		}

		return br;
	}

	private static ArrayNode replace(ArrayNode answer1, ArrayNode answer2) {

		if (Objects.isNull(answer1) || Objects.isNull(answer2)) {
			throw new UnsupportedOperationException("answer1 ??? answer2????????????null???, answer1: " + answer1 + ", answer2: " + answer2);
		}

		if (answer1.size() == 0 && answer2.size() == 0) {
			return mapper.createArrayNode();
		}

		if (answer2.size() == 0) {
			return answer1;
		}

		ArrayNode resultAnswer = mapper.createArrayNode();
		// ??????answer1
		for (JsonNode jn1 : answer1) {

			Optional<ObjectNode> jn2 = Streams.stream(answer2).filter(i -> {
				return i.at("/queId").asInt() == jn1.at("/queId").asInt() && i.at("/queType").asInt() == jn1.at("/queType").asInt();
			}).map(i -> (ObjectNode) i).findAny();

			if (jn2.isPresent()) {

				// ???answer2?????????answer1?????????, ??????????????????, ??????????????????  ????????????, ??????????????????
				if (!jn2.get().at("/values").isNull() || (!jn2.get().at("/tableValues").isMissingNode() && jn2.get().at("/tableValues").size() > 0)) {

					ArrayNode tempA = mapper.createArrayNode();

					// ????????????
					if (jn2.get().at("/queType").asInt() == 18) {

						// answer1????????????, ????????????answer2
						if (jn1.at("/tableValues").isArray() && jn1.at("/tableValues").size() == 0) {
							return answer2;
						}

						// ?????????????????? ????????????[] ?????????
						if (jn1.at("/tableValues").size() == jn2.get().at("/tableValues").size()) {
							for (int i = 0; i < jn1.at("/tableValues").size(); i++) {
								ArrayNode resultA = replace((ArrayNode) jn1.at("/tableValues/" + i), (ArrayNode) jn2.get().at("/tableValues/" + i));
								if (resultA.size() > 0) {
									tempA.add(resultA);
								}
							}
						} else if (jn2.get().at("/tableValues").size() > 0) {
							// jn2?????????????????????, ?????????????????????jn1??????????????????, jn2?????????????????????

							// ???jn2????????????
							Set<Integer> row2Set = Sets.newHashSet();
							for (JsonNode j : jn2.get().at("/tableValues")) {
								for (JsonNode rowJn : j) {
									if (rowJn.at("/values").isArray() && rowJn.at("/values").size() != 0) {
										row2Set.add(rowJn.at("/values/0/ordinal").asInt());
									}
								}
							}

							for (JsonNode temp1 : jn1.at("/tableValues")) {
								// jn1????????????
								List<Integer> row1 = Lists.newArrayList();
								for (JsonNode temp2 : temp1) {
									if (temp2.at("/values").isArray() && temp2.at("/values").size() != 0) {
										row1.add(temp2.at("/values/0/ordinal").asInt());
									}
								}
								int rowNum1 = row1.get(0);

								for (JsonNode temp3 : jn2.get().at("/tableValues")) {
									// jn2????????????
									List<Integer> row2 = Lists.newArrayList();
									for (JsonNode temp4 : temp3) {
										if (temp4.at("/values").isArray() && temp4.at("/values").size() != 0) {
											row2.add(temp4.at("/values/0/ordinal").asInt());
										}
									}
									int rowNum2 = row2.isEmpty() ? -1 : row2.get(0);

									// ?????????rowNum1???jn1???jn2?????????
									if (rowNum1 == rowNum2) {
										ArrayNode resultA = replace((ArrayNode) temp1, (ArrayNode) temp3);
										tempA.add(resultA);
									}
								}

								// ??????jn1???????????????jn2?????????, ??????jn1????????????
								if (!row2Set.contains(rowNum1)) {
									tempA.add(temp1);
								}

							}
						}
					}

					if (!jn2.get().at("/values/0/value").isNull()) {
						if (tempA.size() > 0) {
							ObjectNode obj = mapper.createObjectNode();
							addKey(obj, jn1);
							obj.set("tableValues", tempA);
							resultAnswer.add(obj);
						} else if (jn2.get().at("/_op").asText().equals("add")) {
							// ??????
							ArrayNode values = (ArrayNode) jn1.at("/values");
							for (JsonNode value : jn2.get().at("/values")) {
								values.add(value);
							}
							resultAnswer.add(jn1);
						} else {
							resultAnswer.add(jn2.get());
						}

					}

				}
			} else {
				// ?????????
				resultAnswer.add(jn1);
			}
		}

		// ??????answer2 ???????????????
		for (JsonNode jn2 : answer2) {
			// ???????????????
			// ???????????????

			Optional<ObjectNode> jn1 = Streams.stream(answer1).filter(i -> {
				return i.at("/queId").asInt() == jn2.at("/queId").asInt() && i.at("/queType").asInt() == jn2.at("/queType").asInt();
			}).map(i -> (ObjectNode) i).findAny();

			if (!jn1.isPresent() && !jn2.at("/values/0/value").isNull()) {
				resultAnswer.add(jn2);
			} else {

				// ????????????????????? (?????????) ???????????????

				Optional<ObjectNode> jnRes = Streams.stream(resultAnswer).filter(i -> {
					return i.at("/queId").asInt() == jn2.at("/queId").asInt() && i.at("/queType").asInt() == jn2.at("/queType").asInt();
				}).map(i -> (ObjectNode) i).findAny();

				if (jnRes.isPresent()) {

					// ?????????table2
					for (JsonNode tempC : jn2.at("/tableValues")) {
						// for(int i = 0; i< jn2.at("/tableValues").size(); i++){
						// ?????????table2?????????
						List<Integer> row2 = Lists.newArrayList();
						for (JsonNode tempD : tempC) {
							row2.add(tempD.at("/values/0/ordinal").asInt());
						}
						int rowNum2 = row2.isEmpty() ? -1 : row2.get(0);

						Set<Integer> rowNum1Set = Sets.newHashSet();
						for (JsonNode tempA : jn1.get().at("/tableValues")) {
							// ?????????table1 ???queId??????tableValues?????????
							for (JsonNode t : tempA) {
								rowNum1Set.add(t.at("/values/0/ordinal").asInt());
							}
						}

						if (!rowNum1Set.contains(rowNum2)) {
							ArrayNode res = jnRes.get().withArray("tableValues");
							ArrayNode resArray = mapper.createArrayNode();
							for (JsonNode tempD : tempC) {
								if (tempD.at("/values").isArray() && tempD.at("/values").size() > 0) {
									resArray.add(tempD);
								}
							}
							res.add(resArray);

							// res??????
							List<JsonNode> nodeList = Lists.newArrayList();
							for (int i = 0; i < res.size(); i++) {
								nodeList.add(res.get(i));
							}

							nodeList.sort((jn11, jn21) -> {
								List<Integer> num1List = Lists.newArrayList();
								for (JsonNode jn : jn11) {
									if (jn.at("/values").isArray() && jn.at("/values").size() != 0) {
										num1List.add(jn.at("/values/0/ordinal").asInt());
									}
								}

								List<Integer> num2List = Lists.newArrayList();
								for (JsonNode jn : jn21) {
									if (jn.at("/values").isArray() && jn.at("/values").size() != 0) {
										num2List.add(jn.at("/values/0/ordinal").asInt());
									}
								}
								int num1 = num1List.isEmpty() ? 0 : num1List.get(0);
								int num2 = num2List.isEmpty() ? 0 : num2List.get(0);
								return num1 - num2;
							});

							res.removeAll();
							for (JsonNode jn : nodeList) {
								res.add(jn);
							}

						}
					}
				}
			}
		}

		ArrayNode res = mapper.createArrayNode();
		Streams.stream(resultAnswer).peek(i -> {
			((ObjectNode) i).remove("_op");
		}).forEach(res::add);

		return res;

	}

	private static ArrayNode getPatchAnswer(ArrayNode modifies, String beforeKey, String afterKey) {
		ArrayNode arrayNode = mapper.createArrayNode();

		for (JsonNode jn2 : modifies) {
			JsonNode before = jn2.at("/" + beforeKey);
			JsonNode after = jn2.at("/" + afterKey);

			ObjectNode objectNode = mapper.createObjectNode();

			if (jn2.at("/queType").asInt() != 18) {

				objectNode.put("queId", jn2.at("/queId").asInt())
						.put("queType", jn2.at("/queType").asInt());

				ArrayNode tmpA = objectNode.withArray("values");
				tmpA.add(mapper.createObjectNode().putNull("value"));

				// ??????before???null after??????null ?????????
				if (before.isNull() && after.isNull()) {

				} else if (!before.isNull() && after.isNull()) {    // ??????before?????? after???null, ????????????null???, ????????????
					// ???before??????key???????????????object
					addKey(objectNode, before);
					if (before.at("/values").isArray() && before.at("/values").size() != 0) {
						arrayNode.add(objectNode);
					}
				} else if (before.isNull() && !after.isNull()) {    // before???null after??????
					addKey(objectNode, after);
					((ObjectNode) after).put("_op", "add");
					if (after.at("/values").isArray() && after.at("/values").size() != 0) {
						arrayNode.add(after);
					}
				} else { // ??????before after????????? ??????after
					addKey(objectNode, before);
					if (after.at("/values").isArray() && after.at("/values").size() != 0) {
						arrayNode.add(after);
					}
				}

			} else {
				// ????????????
				arrayNode = tableGetPatchAnswer(before, after, arrayNode.deepCopy(), objectNode.deepCopy(), jn2);
			}
		}

		return arrayNode;
	}


	private static ArrayNode tableGetPatchAnswer(JsonNode before, JsonNode after, ArrayNode arrayNode, ObjectNode objectNode, JsonNode jn2) {

		objectNode.put("queId", jn2.at("/queId").asInt())
				.put("queType", jn2.at("/queType").asInt());
		ArrayNode tempA = objectNode.withArray("tableValues");

		if (before.isNull() && !after.isNull()) {
			addKey(objectNode, after);
			// ??????after ???after???????????????
			for (JsonNode tempO : after.at("/tableValues")) {
				ArrayNode tempB = mapper.createArrayNode();
				for (JsonNode tempI : tempO) {
					if (tempI.at("/values").isArray() && tempI.at("/values").size() != 0) {
						tempB.add(tempI);
					}
				}
				tempA.add(tempB);
			}
			arrayNode.add(objectNode);
		}

		// after???null ??????[]
		if (!before.isNull() && after.isNull()) {
			// addKey(objectNode, before);
			// arrayNode.add(objectNode);
			return arrayNode;
		}

		for (JsonNode jn : before.at("/tableValues")) {

			addKey(objectNode, before);
			ArrayNode tempB = mapper.createArrayNode();
			List<Integer> row = Lists.newArrayList();
			for (JsonNode tempC : jn) {
				// ????????????
				if (tempC.at("/values").isArray() && tempC.at("/values").size() != 0) {
					row.add(tempC.at("/values/0/ordinal").asInt());
				}
			}

			int rowNum = row.get(0);
			// ???after?????????????????????

			List<JsonNode> afterNodes = Streams.stream(after.at("/tableValues")).filter(tableValue -> {
				return Streams.stream(tableValue).flatMap(j -> Streams.stream(j.at("/values")))
						.allMatch(j -> j.at("/ordinal").asInt() == rowNum);
			}).collect(Collectors.toList());

			// JsonNode afterTable = after.at("/tableValues/" + rowNum);
			Preconditions.checkArgument(afterNodes.size() <= 1, "????????????...");
			JsonNode afterTable = mapper.createArrayNode();
			if (afterNodes.size() > 0) {
				afterTable = afterNodes.get(0);
			}

			Set<Integer> queIdSet = Sets.newHashSet();
			for (JsonNode tempC : jn) {

				queIdSet.add(tempC.at("/queId").asInt());

				// afterTable????????????
				Set<Integer> rowAft = Sets.newHashSet();
				for (JsonNode rowJn : after.at("/tableValues")) {
					for (JsonNode rowJ : rowJn) {
						if (rowJ.at("/values").isArray() && rowJ.size() != 0) {
							rowAft.add(rowJ.at("/values/0/ordinal").asInt());
						}
					}
				}

				// ??????answer1???table?????????answer2?????????
				if (!rowAft.contains(rowNum)) {
					ObjectNode objectNodeT = mapper.createObjectNode();
					objectNodeT.put("queId", tempC.at("/queId").asInt())
							.put("queType", tempC.at("/queType").asInt());

					ArrayNode tmpB = objectNodeT.withArray("values");
					addKey(objectNodeT, tempC);
					tmpB.add(mapper.createObjectNode().putNull("value").put("ordinal", rowNum));
					tempB.add(objectNodeT);
				} else if (afterTable.isMissingNode()) {

					ObjectNode objectNodeT = mapper.createObjectNode();
					objectNodeT.put("queId", tempC.at("/queId").asInt())
							.put("queType", tempC.at("/queType").asInt());

					ArrayNode tmpB = objectNodeT.withArray("values");
					// tmpB.add(mapper.createObjectNode().putNull("value").put("ordinal", rowNum));
					if (tempC.at("/values").isArray() && tempC.at("/values").size() != 0) {
						tmpB.add(tempC.at("/values/0"));
					}
					if (objectNodeT.at("/values").isArray() && objectNodeT.at("/values").size() != 0) {
						tempB.add(objectNodeT);
					}
				}

				for (JsonNode tempD : afterTable) {
					if (tempD.at("/queId").asInt() == tempC.at("/queId").asInt()) {

						if (tempC.isNull() && tempD.isNull()) {

						} else if (!tempC.isNull() && tempD.isNull()) {
							if (tempC.at("/values").isArray() && tempC.size() != 0 && !tempC.at("/values/0/value").isNull()) {
								tempB.add(objectNode);
							}
						} else if (tempC.isNull() && !tempD.isNull()) {
							if (tempD.at("/values").isArray() && tempD.at("/values").size() != 0 && !tempD.at("/values/0/value").isNull()) {
								tempB.add(tempD);
							}
						} else {
							// before after ?????????null
							if (tempD.at("/values").isArray() && tempD.at("/values").size() != 0 && !tempD.at("/values/0/value").isNull()) {
								tempB.add(tempD);
							} else if (tempC.at("/values").isArray() && tempC.at("/values").size() != 0 && tempD.at("/values").size() == 0) {
								// before values not [], after values is [], ????????????: value:null
								// tempB.add(tempD);
								ObjectNode objectNodeT = mapper.createObjectNode();
								objectNodeT.put("queId", tempC.at("/queId").asInt())
										.put("queType", tempC.at("/queType").asInt());

								ArrayNode tmpB = objectNodeT.withArray("values");
								addKey(objectNodeT, tempC);
								tmpB.add(mapper.createObjectNode().putNull("value").put("ordinal", rowNum));
								tempB.add(objectNodeT);
							}
						}
					}
				}
			}

			// ??????tableValue???after??????before??????????????????
			for (JsonNode tempD : afterTable) {

				if (!queIdSet.contains(tempD.at("/queId").asInt())) {
					if (tempD.at("/values").isArray() && tempD.at("/values").size() != 0) {
						tempB.add(tempD);
					}
				}
			}

			tempA.add(tempB);
		}

		if (!before.isNull()) {
			// ??????before???????????????
			Set<Integer> rowBef = Sets.newHashSet();
			for (JsonNode bef : before.at("/tableValues")) {
				for (JsonNode rowJn : bef) {
					if (rowJn.at("/values").isArray() && rowJn.at("/values").size() != 0) {
						rowBef.add(rowJn.at("/values/0/ordinal").asInt());
					}
				}
			}

			// ??????after, ??????after??????, before???????????????  (2 -> 4)

			for (JsonNode aft : after.at("/tableValues")) {
				ArrayNode tempB = mapper.createArrayNode();
				// ??????after?????????
				List<Integer> rowAft = Lists.newArrayList();
				for (JsonNode rowJn : aft) {
					if (rowJn.at("/values").isArray() && rowJn.at("/values").size() != 0) {
						rowAft.add(rowJn.at("/values/0/ordinal").asInt());
					}
				}
				int rowAfterNum = rowAft.get(0);

				// ??????after????????????before?????????, ?????????after
				if (!rowBef.contains(rowAfterNum)) {
					for (JsonNode jn : aft) {
						if (jn.at("/values").isArray() && jn.at("/values").size() > 0 && !jn.at("/values/0/value").isNull()) {
							tempB.add(jn);
						}
					}
				}
				if (tempB.size() > 0) {
					tempA.add(tempB);
				}
			}
		}

		// ??????
		List<JsonNode> nodeList = Lists.newArrayList();
		for (int i = 0; i < tempA.size(); i++) {
			nodeList.add(tempA.get(i));
		}

		nodeList.sort((jn1, jn21) -> {
			List<Integer> num1List = Lists.newArrayList();
			for (JsonNode jn : jn1) {
				if (jn.at("/values").isArray() && jn.at("/values").size() != 0) {
					num1List.add(jn.at("/values/0/ordinal").asInt());
				}
			}

			List<Integer> num2List = Lists.newArrayList();
			for (JsonNode jn : jn21) {
				if (jn.at("/values").isArray() && jn.at("/values").size() != 0) {
					num2List.add(jn.at("/values/0/ordinal").asInt());
				}
			}
			int num1 = num1List.get(0);
			int num2 = num2List.get(0);
			return num1 - num2;
		});

		tempA.removeAll();
		for (JsonNode jn : nodeList) {
			tempA.add(jn);
		}

		boolean br = !(before.isNull() && after.isNull()) && !((before.isNull() && !after.isNull()));
		if (br) {
			arrayNode.add(objectNode);
		}

		return arrayNode;
	}

	private static void addKey(ObjectNode obj, JsonNode from) {
		Iterator<String> keyIt = from.fieldNames();
		while (keyIt.hasNext()) {
			String key = keyIt.next();
			if (!key.equals("values") && !key.equals("tableValues") && !key.equals("_op")) {
				obj.set(key, from.get(key));
			}
		}
	}

	@SuppressWarnings("unchecked")
	private static <T> T getValue(ArrayNode answer, String key) {

		for (JsonNode jn : answer) {
			if (jn.at("/queTitle").asText().equals(key) && !jn.at("/queId").asText().equals("0")) {
				String queType = jn.at("/queType").asText();
				switch (queType) {
				case "":
					JsonNodeType type = jn.at("/values/0/value").getNodeType();
					if (type == JsonNodeType.STRING) return (T) jn.at("/values/0/value").asText();
					if (type == JsonNodeType.NUMBER) return (T) (Object) jn.at("/values/0/value").asLong();
					throw new UnsupportedOperationException("???????????????...");
				case "2":
					return (T) jn.at("/values/0/value").asText();
				case "4":
					String lastTimeStr = jn.at("/values/0/value").asText();
					Preconditions.checkArgument(!Strings.isNullOrEmpty(lastTimeStr), "lastTimeStr??????");
					DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
					return (T) LocalDateTime.parse(lastTimeStr, format);
				case "8":
					return (T) (Object) jn.at("/values/0/value").asLong();
				default:
					throw new UnsupportedOperationException("???????????????: queType = " + queType);
				}
			}
		}
		throw new IllegalArgumentException();
	}

}

