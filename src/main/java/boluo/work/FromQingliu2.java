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
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.*;

public class FromQingliu2 {

    private static final String baseUri = "https://api.ding.qingflow.com";
    private static final String baseWebUri = "https://ding.qingflow.com/api";
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(FromQingliu2.class);
    private static final Cache<String, JsonNode> requestCache = CacheBuilder.newBuilder().build();
    private static final CloseableHttpClient http = HttpClients.createDefault();

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .getOrCreate();

        CommandLine cli = new GnuParser().parse(new Options()
                .addOption("ta", "api-token", true, "")
                .addOption("tw", "web-token", true, "")
                .addOption("o", "output", true, ""), args);

        // 重要的量
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
                .add("audit", new StructType()
                        .add("uid", "long")
                        .add("nickname", "string")
                        .add("email", "string")
                        .add("head", "string")
                )
                .add("after", "string");

        // 获取成员
        Optional<JsonNode> creator = user("小兰Robot", apiToken);
        String userId = creator.map(i -> i.at("/userId").asText()).orElse(null);

        // 1.获取工作区所有应用
        JsonNode appIdResultNode = apiRequest(HttpMethod.GET, String.format("%s/app", baseUri),
                apiToken, userId, null);

        JsonNode appIdResult = appIdResultNode.at("/result").withArray("appList");
        Map<String, String> appMap = Maps.newHashMap();
        for (JsonNode jn : appIdResult) {
            appMap.put(jn.at("/appKey").asText(), jn.at("/appName").asText());
        }

        appMap.keySet().retainAll(ImmutableList.of("84b0eecd"));
        appMap.keySet().remove("7bb96d8f");
        for (String appId : appMap.keySet()) {

            // 查询本地轻流表
            Dataset<Row> df1 = spark.read().format("delta").load(localQingliuPath);
            List<Row> localApplyList;

            if (df1.count() == 0) {
                localApplyList = Lists.newArrayList();
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
                localApplyList = spark.sql(sql).collectAsList();
            }

            LocalDateTime localMaxUpdateTime = localApplyList.stream()
                    .map(i -> i.<Timestamp>getAs("audit_time"))
                    .map(Timestamp::toLocalDateTime)
                    .max(LocalDateTime::compareTo)
                    .orElse(LocalDateTime.of(2016, 1, 1, 0, 0))
                    .minusHours(1);

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
                    apiToken, userId, requestBody);

            int applyIdCount = applyIdResultNode.at("/result/resultAmount").asInt();
            JsonNode applyAnswersNode = applyIdResultNode.at("/result/result");

            Map<Integer, ArrayNode> checkAnswersMap = Maps.newTreeMap();
            for (JsonNode jn : applyAnswersNode) {
                checkAnswersMap.put(jn.at("/applyId").asInt(), (ArrayNode) jn.at("/answers"));
            }

            // 判断2
            // 取checkAnswersMap中最后一条数据时间, 与localMaxUpdateTime比较
            ArrayNode lastAnswersNode = Iterables.getFirst(checkAnswersMap.values(), null);
            LocalDateTime lastTime = Objects.nonNull(lastAnswersNode)
                    ? getValue(lastAnswersNode, "更新时间")
                    : LocalDateTime.of(2016, 1, 1, 0, 0);

            // checkAnswersMap >= localMaxUpdateTime
            for (int i = 2; lastTime.compareTo(localMaxUpdateTime) >= 0; ++i) {

                ObjectNode tempRequestBody = mapper.createObjectNode();
                tempRequestBody.put("pageNum", i)
                        .put("pageSize", "1000");

                tempRequestBody.withArray("sorts")
                        .addObject()
                        .put("queId", 3)
                        .put("isAscend", false);

                // 分页获取应用的数据, 通过appId获取appId下所有的applyId
                JsonNode nextFilterResult = apiRequest(HttpMethod.POST, String.format("%s/app/%s/apply/filter", baseUri, appId),
                        apiToken, userId, tempRequestBody);

                JsonNode tempApplyAnswersNode = nextFilterResult.at("/result/result");
                for (JsonNode jn : tempApplyAnswersNode) {
                    checkAnswersMap.put(jn.at("/applyId").asInt(), (ArrayNode) jn.at("/answers"));
                }

                // TODO 取checkAnswersMap中最后一条数据时间, 与localMaxUpdateTime比较
                ArrayNode lastAnswersNode1 = Iterables.getFirst(checkAnswersMap.values(), null);
                LocalDateTime lastTime1 = Objects.nonNull(lastAnswersNode1)
                        ? getValue(lastAnswersNode1, "更新时间")
                        : LocalDateTime.of(2016, 1, 1, 0, 0);

                if (i >= nextFilterResult.at("/result/pageAmount").asInt()) {
                    break;
                }
                lastTime = lastTime1;
            }

            // 判断3 是否有删除的
            // 先找出线上有的数据, 但是本地没有的applyId
            Set<Integer> currAddApplyIdSet = Sets.difference(
                    checkAnswersMap.keySet(),
                    localApplyList.stream().map(i -> i.getAs(0)).collect(Collectors.toSet()));

            // TODO error

            if (localApplyList.size() + currAddApplyIdSet.size() < applyIdCount) {
                System.out.println("aaa");
            }

            Preconditions.checkArgument(localApplyList.size() + currAddApplyIdSet.size() >= applyIdCount,
                    "数据异常...appId为: " + appId +
                            ", 本地数据量: " + localApplyList.size() +
                            ", 增加数据量: " + currAddApplyIdSet.size() +
                            ", 线上总数: " + applyIdCount +
                            ", checkAnswersMap数量" + checkAnswersMap.size()
            );

            // TODO 去掉checkAnswerMap中时间小于localMaxUpdateTime的数据
            Set<Integer> deleteSet = Streams.stream(checkAnswersMap.entrySet())
                    .filter(kv -> {
                        LocalDateTime currTime = getValue(kv.getValue(), "更新时间");
                        return currTime.compareTo(localMaxUpdateTime) < 0;
                    })
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());
            checkAnswersMap.keySet().removeAll(deleteSet);

            if (localApplyList.size() + currAddApplyIdSet.size() == applyIdCount) {
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
                        apiToken, userId, deleteRequestBody);

                Preconditions.checkArgument(data1.at("/errCode").intValue() == 0);
                int pageAmount = data1.at("/result/pageAmount").asInt();
                Stream<JsonNode> data2 = Stream.iterate(2, j -> j + 1)
                        .limit(Math.max(pageAmount - 1, 0))
                        .map(k -> {
                            deleteRequestBody.put("pageNum", k);
                            return apiRequest(HttpMethod.POST, String.format("%s/app/%s/apply/filter", baseUri, appId),
                                    apiToken, userId, deleteRequestBody);
                        });

                Stream.concat(Stream.of(data1), data2)
                        .forEach(r -> {
                            JsonNode jsonNode = r.at("/result/result");
                            for (JsonNode jn : jsonNode) {
                                checkAnswersMap.put(jn.at("/applyId").asInt(), (ArrayNode) jn.at("/answers"));
                            }
                        });

                // 此时的checkAnswersMap中拥有线上全量的applyId
                // 本地的 - 线上全部的 = 删除的
                Stream<Row> deleteApply = localApplyList.stream().filter(i -> {
                    Integer k = i.getAs(0);
                    return checkAnswersMap.containsKey(k);
                });

//				for (Iterator<Row> it = deleteApply.iterator(); it.hasNext();){
//					Row curr = it.next();
//				}
                deleteApply.forEach(i -> {
                    // TODO 给这些 applyId 添加删除行, logId = 'D'
                });
            }

            // 处理添加的
            Map<Integer, Tuple2<Integer, ArrayNode>> beforeMap = Maps.newHashMap();
            for (Row row : localApplyList) {

                ArrayNode onlineBefore = Strings.isNullOrEmpty(row.getAs(3))
                        ? mapper.createArrayNode()
                        : (ArrayNode) mapper.readTree(row.getAs(3).toString());

                beforeMap.put(row.getAs(0), Tuple2.apply(row.getAs(1), onlineBefore));
            }
            checkAnswersMap.keySet().retainAll(ImmutableList.of(706745));
            List<Row> updateRowList = addQingliu(appId, appMap.get(appId), checkAnswersMap, beforeMap,
                    apiToken, webToken);

            // 存储
            Dataset<Row> writeData = spark.createDataFrame(updateRowList, qingliuStruct);
//            Outputs.replace(writeData, localQingliuPath,
//                    expr(String.format("t.app_id='%s' and s.apply_id=t.apply_id and s.log_id=t.log_id", appId)),
//                    "app_id");

        }

    }

    private static List<Row> addQingliu(String appId, String appName, Map<Integer, ArrayNode> checkAnswersMap, Map<Integer, Tuple2<Integer, ArrayNode>> beforeMap,
                                        String apiToken, String webToken) throws IOException {

        List<Row> updateRowList = Lists.newArrayList();
        for (Integer applyId : checkAnswersMap.keySet()) {

            // 调用: 获取单条数据的流程日志  第一条applyId 695872
//			JsonNode qingliuLogIdNode = apiRequest(HttpMethod.GET, String.format("%s/apply/%s/auditRecord", baseUri, applyId),
//					apiToken, null, null);

            JsonNode webQingliuLogIdNode = webRequest(webToken, HttpMethod.GET,
                    String.format("%s/app/%s/apply/%d/auditRecord?role=1", baseWebUri, appId, applyId),
                    String.format("https://ding.qingflow.com/arch/app/%s/all;type=8?applyId=%d", appId, applyId),
                    null
            );
            ObjectNode qingliuLogIdNode = formatAuditRecord(webQingliuLogIdNode);

            // 存放添加的logId对应的节点, 也就是online接口返回的整个结果
            List<JsonNode> updateLogIdList = Lists.newArrayList();

            int localMaxLogId = beforeMap.containsKey(applyId) ? beforeMap.get(applyId)._1 : -1;
            ArrayNode onlineBefore = beforeMap.containsKey(applyId) ? beforeMap.get(applyId)._2 : mapper.createArrayNode();

            // 获取qingliuLogIdNode中所有的qingliuLogId
            for (JsonNode jn : qingliuLogIdNode.at("/result/auditRecords")) {
                int qingliuLogId = jn.at("/auditRcdId").asInt();
                if (qingliuLogId > localMaxLogId) {
                    updateLogIdList.add(jn);
                }
            }

            List<Row> tempUpdateRowList = Lists.newArrayList();
            for (JsonNode jn : updateLogIdList) {

                Integer logId = jn.at("/auditRcdId").asInt();
//				JsonNode resultNode = apiRequest(HttpMethod.GET, String.format("%s/apply/%s/auditRecord/%s", baseUri, applyId, logId),
//						token, userId, null);

                ObjectNode webAuditRecordResult = webRequest(webToken, HttpMethod.GET,
                        String.format("%s/app/%s/apply/%d/auditRecord/%d?role=1", baseWebUri, appId, applyId, logId),
                        String.format("https://ding.qingflow.com/arch/app/%s/all;type=8?applyId=%d", appId, applyId),
                        null
                );
                ObjectNode auditRecordResult = formatAuditRecordDetail(webAuditRecordResult);

                ArrayNode beforeAnswer = getPatchAnswer((ArrayNode) auditRecordResult.at("/auditModifies"), "beforeAnswer", "beforeAnswer");
                ArrayNode afterAnswer = getPatchAnswer((ArrayNode) auditRecordResult.at("/auditModifies"), "beforeAnswer", "afterAnswer");

                if (logId == 1592977) {
                    System.out.println("aaa");
                }

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
                String onlineAfter_ = Objects.isNull(onlineAfter) ? null : onlineAfter.toString();

                Row auditRow = uid == 0L ? null : RowFactory.create(uid, nickName, email, head);
                Row lineRow = RowFactory.create(appId, appName, applyId, logId, createTime, auditName,
                        auditResult, auditTime, auditRow, onlineAfter_);

                tempUpdateRowList.add(lineRow);
                onlineBefore = onlineAfter;
            }

            // 二次检查
            ArrayNode lastAfter = onlineBefore;
            ArrayNode onlineAfter = checkAnswersMap.get(applyId);

            assertCompare(onlineAfter, lastAfter);
            updateRowList.addAll(tempUpdateRowList);
        }

        return updateRowList;
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
            Thread.sleep(128);
            JsonNode r = mapper.readTree(is);
            if (r.at("/statusCode").intValue() == 40001) {
                throw new AuthenticationException();
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
                // throw new UnsupportedOperationException("web接口返回不是Object...");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (AuthenticationException e) {
            // 重试一次
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        throw new UnsupportedOperationException();
    }


    private static void assertCompare(ArrayNode answer1, ArrayNode answer2) throws JsonProcessingException {

        if (Objects.isNull(answer1) && Objects.isNull(answer2)) {
            return;
        }
        if (answer1.size() == 0 && answer2.size() == 0) {
            return;
        }

        if (answer1.size() == 0 && answer2.size() > 0) {
            Optional<JsonNode> br_value = Streams.stream(answer2).filter(i -> {
                return !i.at("/values").isNull();
            }).findAny();
            if (!br_value.isPresent()) {
                return;
            }
        }

        boolean br = true;

        Set<String> idSet = Sets.newTreeSet();
        for (JsonNode jn1 : answer1) {
            idSet.add(jn1.at("/queId").asText());
        }

        if (answer2.size() == 0) {
            br = false;
        }
        for (JsonNode jn2 : answer2) {
            if (idSet.contains(jn2.at("/queId").asText())) {
                for (JsonNode jn1 : answer1) {
                    String value1 = jn1.at("/values/0/value").asText();
                    if (jn2.at("/queId").asText().equals(jn1.at("/queId").asText())) {
                        // 找出answer1和answer2 queId相同的数据, 比较其values中的value
                        br = br && jn2.at("/values/0/value").asText().equals(value1);

                        // TODO 如果一个有表格, 一个没有表格
                        boolean br_table_values = (jn1.at("/tableValues").isMissingNode() && !jn2.at("/tableValues").isMissingNode())
                                || (!jn1.at("/tableValues").isMissingNode() && jn2.at("/tableValues").isMissingNode());

                        br = br && !br_table_values;

                        if (jn2.at("/tableValues").isMissingNode() && jn1.at("/tableValues").isMissingNode()) {
                            continue;
                        } else {

                            Preconditions.checkArgument(jn1.at("/tableValues").size() == jn2.at("/tableValues").size(),
                                    "表格部分列数不同...jn1: " + jn1.at("/tableValues") +
                                            ", jn2: " + jn2.at("/tableValues"));
                            if (jn2.at("/tableValues").size() > 0) {
                                compareTable((ArrayNode) jn1.at("/tableValues"), (ArrayNode) jn2.at("/tableValues"));
                                // compareTable((ArrayNode) jn1.at("/values"), (ArrayNode) jn2.at("/values"));
                                continue;
                            }

                            // 如果两个都有, 比较表格部分
                            Map<Integer, String> mapTable1 = Maps.newHashMap();
                            for (JsonNode jnt1 : jn1.at("/tableValues")) {
                                mapTable1.put(jnt1.at("/queId").asInt(), jnt1.at("/values/0/value").asText());
                            }

                            Preconditions.checkArgument(mapTable1.size() == jn2.at("/tableValues").size(),
                                    "表格部分列数不同...mapTable1.size: " + mapTable1.size() +
                                            ", jn2.size: " + jn2.at("/tableValues").size() +
                                            ", mapTable1: " + mapTable1 +
                                            ", jn2: " + jn2.at("/tableValues"));

                            for (JsonNode jnt2 : jn2.at("/tableValues")) {
                                int queId = jnt2.at("/queId").asInt();
                                br = br && Objects.equals(jnt2.at("/values/0/value").asText(), mapTable1.get(queId));
                            }
                        }

                    }
                }
            } else if (!jn2.at("/values").isNull()) {
                // 如果answer2中的数据1中没有, 且values不为null, 则两者不同
                br = false;
            }
        }

        if (!br) {
            logger.error("answer1: " + mapper.writerWithDefaultPrettyPrinter().writeValueAsString(answer1));
            logger.error("answer2: " + mapper.writerWithDefaultPrettyPrinter().writeValueAsString(answer2));
            throw new RuntimeException("校验结果不正确...");
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

    private static ArrayNode replace(ArrayNode answer1, ArrayNode answer2) {

        if (Objects.isNull(answer1) || Objects.isNull(answer2)) {
            throw new UnsupportedOperationException("answer1 或 answer2不允许为null值, answer1: " + answer1 + ", answer2: " + answer2);
        }

        if (answer1.size() == 0 && answer2.size() == 0) {
            return mapper.createArrayNode();
        }

        if (answer2.size() == 0) {
            return answer1;
        }

        ArrayNode resultAnswer = mapper.createArrayNode();
        // 遍历answer1
        for (JsonNode jn1 : answer1) {

            Optional<ObjectNode> jn2 = Streams.stream(answer2).filter(i -> {
                return i.at("/queId").asInt() == jn1.at("/queId").asInt() && i.at("/queType").asInt() == jn1.at("/queType").asInt();
            }).map(i -> (ObjectNode) i).findAny();

            if (jn2.isPresent()) {

                // 在answer2中找到answer1中的值, 删除的不添加, 非删除的添加  有的替换, 没有的不处理
                if (!jn2.get().at("/values").isNull() || (!jn2.get().at("/tableValues").isMissingNode() && jn2.get().at("/tableValues").size() > 0)) {

                    ArrayNode tempA = mapper.createArrayNode();

                    // 处理表格
                    if (jn2.get().at("/queType").asInt() == 18) {

                        // answer1表格为空, 直接返回answer2
                        if (jn1.at("/tableValues").isArray() && jn1.at("/tableValues").size() == 0) {
                            return answer2;
                        }

                        // 表格数量一致 有表格为[] 的情况
                        if (jn1.at("/tableValues").size() == jn2.get().at("/tableValues").size()) {
                            for (int i = 0; i < jn1.at("/tableValues").size(); i++) {
                                ArrayNode resultA = replace((ArrayNode) jn1.at("/tableValues/" + i), (ArrayNode) jn2.get().at("/tableValues/" + i));
                                if (resultA.size() > 0) {
                                    tempA.add(resultA);
                                }
                            }
                        } else if (jn2.get().at("/tableValues").size() > 0) {
                            // jn2的表格中有数据, 按行列逐个替换jn1中的表格数据, jn2中没有的不替换

                            // 取jn2所有行数
                            Set<Integer> row2Set = Sets.newHashSet();
                            for (JsonNode j : jn2.get().at("/tableValues")) {
                                for (JsonNode rowJn : j) {
                                    if (rowJn.at("/values").isArray() && rowJn.at("/values").size() != 0) {
                                        row2Set.add(rowJn.at("/values/0/ordinal").asInt());
                                    }
                                }
                            }

                            for (JsonNode temp1 : jn1.at("/tableValues")) {
                                // jn1当前行数
                                List<Integer> row1 = Lists.newArrayList();
                                for (JsonNode temp2 : temp1) {
                                    if (temp2.at("/values").isArray() && temp2.at("/values").size() != 0) {
                                        row1.add(temp2.at("/values/0/ordinal").asInt());
                                    }
                                }
                                int rowNum1 = row1.get(0);

                                for (JsonNode temp3 : jn2.get().at("/tableValues")) {
                                    // jn2当前行数
                                    List<Integer> row2 = Lists.newArrayList();
                                    for (JsonNode temp4 : temp3) {
                                        if (temp4.at("/values").isArray() && temp4.at("/values").size() != 0) {
                                            row2.add(temp4.at("/values/0/ordinal").asInt());
                                        }
                                    }
                                    int rowNum2 = row2.isEmpty() ? -1 : row2.get(0);

                                    // 替换第rowNum1行jn1和jn2的数据
                                    if (rowNum1 == rowNum2) {
                                        ArrayNode resultA = replace((ArrayNode) temp1, (ArrayNode) temp3);
                                        tempA.add(resultA);
                                    }
                                }

                                // 如果jn1当前行数在jn2中没有, 添加jn1行的数据
                                if (!row2Set.contains(rowNum1)) {
                                    tempA.add(temp1);
                                }

                            }
                        }
                    }

                    if (!jn2.get().at("/values/0/value").isNull()) {
                        if (tempA.size() > 0) {
                            ObjectNode obj = mapper.createObjectNode();
                            // addKey(obj, jn1);
                            obj.set("tableValues", tempA);
                            resultAnswer.add(obj);
                        } else if (jn2.get().at("/_op").asText().equals("add")) {
                            // 添加
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
                // 未找到
                resultAnswer.add(jn1);
            }
        }

        // 遍历answer2 处理添加的
        for (JsonNode jn2 : answer2) {
            // 有的不处理
            // 没有的添加

            Optional<ObjectNode> jn1 = Streams.stream(answer1).filter(i -> {
                return i.at("/queId").asInt() == jn2.at("/queId").asInt() && i.at("/queType").asInt() == jn2.at("/queType").asInt();
            }).map(i -> (ObjectNode) i).findAny();

            if (!jn1.isPresent() && !jn2.at("/values/0/value").isNull()) {
                resultAnswer.add(jn2);
            } else {

                // 处理表格添加的 (增加行) 先遍历结果

                Optional<ObjectNode> jnRes = Streams.stream(resultAnswer).filter(i -> {
                    return i.at("/queId").asInt() == jn2.at("/queId").asInt() && i.at("/queType").asInt() == jn2.at("/queType").asInt();
                }).map(i -> (ObjectNode) i).findAny();

                if (jnRes.isPresent()) {

                    // 先遍历table2
                    for (JsonNode tempC : jn2.at("/tableValues")) {
                        // for(int i = 0; i< jn2.at("/tableValues").size(); i++){
                        // 先拿到table2当前行
                        List<Integer> row2 = Lists.newArrayList();
                        for (JsonNode tempD : tempC) {
                            row2.add(tempD.at("/values/0/ordinal").asInt());
                        }
                        int rowNum2 = row2.isEmpty() ? -1 : row2.get(0);

                        Set<Integer> rowNum1Set = Sets.newHashSet();
                        for (JsonNode tempA : jn1.get().at("/tableValues")) {
                            // 再拿到table1 该queId下的tableValues的行数
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

                            // res排序
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
                // .putNull("values");

                ArrayNode tmpA = objectNode.withArray("values");
                tmpA.add(mapper.createObjectNode().putNull("value"));

                // 如果before为null after也为null 不添加
                if (before.isNull() && after.isNull()) {

                } else if (!before.isNull() && after.isNull()) {    // 如果before有值 after为null, 存入一个null值, 代表删除
                    if (before.at("/values").isArray() && before.at("/values").size() != 0) {
                        arrayNode.add(objectNode);
                    }
                } else if (before.isNull() && !after.isNull()) {    // before为null after有值
                    if (after.at("/values").isArray() && after.at("/values").size() != 0) {
                        arrayNode.add(after);
                    }
                } else { // 如果before after都有值 添加after
                    if (after.at("/values").isArray() && after.at("/values").size() != 0) {
                        arrayNode.add(after);
                    }
                }

            } else {
                // 处理表格
                arrayNode = tableGetPatchAnswer(before, after, arrayNode.deepCopy(), objectNode.deepCopy(), jn2);
            }
        }

        return arrayNode;
    }

    private static ArrayNode tableGetPatchAnswer(JsonNode before, JsonNode after, ArrayNode arrayNode, ObjectNode objectNode, JsonNode jn2){

        objectNode.put("queId", jn2.at("/queId").asInt())
                .put("queType", jn2.at("/queType").asInt());
        ArrayNode tempA = objectNode.withArray("tableValues");

        if (before.isNull() && !after.isNull()) {


            // 遍历after 把after中的值传出
            for (JsonNode tempO : after.at("/tableValues")) {
                ArrayNode tempB = mapper.createArrayNode();
                for (JsonNode tempI : tempO) {
                    // if (tempI.at("/queId").asInt() == tempO.at("/queId").asInt()) {
//                            if (tempI.at("/values").isArray() && tempI.at("/values").size() != 0) {
                    tempB.add(tempI);
//							} else {
//								ObjectNode o = mapper.createObjectNode()
//										.put("queId", tempI.at("/queId").asInt())
//										.put("queType", tempI.at("/queType").asInt());
//								ArrayNode a = o.withArray("values");
//								a.add(mapper.createObjectNode().putNull("value"));
//								tempB.add(o);
//							}
                }
                tempA.add(tempB);
            }
            arrayNode.add(objectNode);
        }

        for (JsonNode jn : before.at("/tableValues")) {

            ArrayNode tempB = mapper.createArrayNode();

            List<Integer> row = Lists.newArrayList();
            for (JsonNode tempC : jn) {
                // 当前行数
                row.add(tempC.at("/values/0/ordinal").asInt());
            }

            int rowNum = row.get(0);
            JsonNode afterTable = after.at("/tableValues/" + rowNum);
            for (JsonNode tempC : jn) {

                if (afterTable.isMissingNode()) {

                    ObjectNode objectNodeT = mapper.createObjectNode();
                    objectNodeT.put("queId", tempC.at("/queId").asInt())
                            .put("queType", tempC.at("/queType").asInt());

                    ArrayNode tmpB = objectNodeT.withArray("values");
                    tmpB.add(mapper.createObjectNode().putNull("value").put("ordinal", tempC.at("/values/0/ordinal").asInt()));

                    tempB.add(objectNodeT);
                }

                for (JsonNode tempD : afterTable) {
                    if (tempD.at("/queId").asInt() == tempC.at("/queId").asInt()) {

                        if (tempC.isNull() && tempD.isNull()) {

                        } else if (!tempC.isNull() && tempD.isNull()) {
                            if (tempC.at("/values").isArray() && tempC.size() != 0) {
                                tempB.add(objectNode);
                            }
                        } else if (tempC.isNull() && !tempD.isNull()) {
                            if (tempD.at("/values").isArray() && tempD.at("/values").size() != 0) {
                                tempB.add(tempD);
                            }
                        } else {
                            if (tempD.at("/values").isArray() && tempD.at("/values").size() != 0) {
                                tempB.add(tempD);
                            }
                        }
                    }
                }
            }

            tempA.add(tempB);
        }

        boolean br = !(before.isNull() && after.isNull()) && !((before.isNull() && !after.isNull()));
        // if (!(before.isNull() && after.isNull())) {
        if (br) {
            arrayNode.add(objectNode);
        }

        return arrayNode;
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
                        throw new UnsupportedOperationException("未知的类型...");
                    case "2":
                        return (T) jn.at("/values/0/value").asText();
                    case "4":
                        String lastTimeStr = jn.at("/values/0/value").asText();
                        Preconditions.checkArgument(!Strings.isNullOrEmpty(lastTimeStr), "lastTimeStr为空");
                        DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                        return (T) LocalDateTime.parse(lastTimeStr, format);
                    case "8":
                        return (T) (Object) jn.at("/values/0/value").asLong();
                    default:
                        throw new UnsupportedOperationException("未知的类型: queType = " + queType);
                }
            }
        }
        throw new IllegalArgumentException();
    }

}
