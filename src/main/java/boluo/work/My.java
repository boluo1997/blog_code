package boluo.work;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;
import com.github.fge.jsonpatch.JsonPatch;
import com.github.fge.jsonpatch.JsonPatchException;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.compress.utils.Charsets;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.catalyst.expressions.DateDiff;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.mutable.Buffer;
import scala.collection.mutable.WrappedArray;

import javax.ws.rs.HttpMethod;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.*;


import static org.apache.spark.sql.functions.*;

import java.time.LocalDate;
import java.util.List;
import java.util.Objects;

public class My {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(My.class);
    // private final Logger logger = LoggerFactory.getLogger(getClass());
    private static final Cache<String, JsonNode> requestCache = CacheBuilder.newBuilder().build();
    private static final CloseableHttpClient http = HttpClients.createDefault();

    public static Optional<JsonNode> fromLinks(JsonNode links, String id) {
        if (Strings.isNullOrEmpty(id) || Objects.isNull(links)) {
            return Optional.empty();
        }
        return Streams.stream(links)
                .filter(i -> i.path("id").asText().equals(id))
                .map(i -> i.path("o"))
                .findFirst();
    }

    private static String answer2key(JsonNode answer) {
        int id = answer.at("/queId").intValue();
        String title = answer.at("/queTitle").textValue();
        Preconditions.checkArgument(id >= 0);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(title));
        return title;
    }

    public static JsonNode answer2value(JsonNode answer) {
        if (answer.isNull()) {
            return MissingNode.getInstance();
        } else if (answer.isArray()) {
            ObjectNode r = mapper.createObjectNode();
            Streams.stream(answer).forEach(i -> {
                r.set(answer2key(i), answer2value(i));
            });
            return r;
        }

        int type = answer.at("/queType").intValue();
        JsonNode values = answer.at("/values");
        switch (type) {
            case 1:/*描述文字*/
            case 2:/*单行文字*/
            case 3:/*多行文字*/
            case 4:/*时间*/
            case 6:/*邮箱*/
            case 7:/*手机*/
            case 9:/*链接*/
            case 10:/*单选*/
            case 11:/*下拉选择*/
            case 16:/*富文本*/
            case 19:/*数据关联*/ {
                Preconditions.checkArgument(answer.at("/values").size() == 1, answer);
                JsonNode v = answer.at("/values/0/value");
                Preconditions.checkArgument(v.isTextual());
                return v;
            }
            case 12:/*多选*/
            case 13:/*上传附件*/
            case 15:/*图片选择*/
            case 21:/*地址*/ {
                ArrayNode r = mapper.createArrayNode();
                Streams.stream(answer.at("/values"))
                        .map(i -> {
                            JsonNode v = i.at("/value");
                            Preconditions.checkArgument(v.isTextual());
                            return v;
                        })
                        .forEach(r::add);
                return r;
            }
            case 8: {
                // 数字
                Preconditions.checkArgument(values.size() == 1, answer);
                JsonNode v = answer.at("/values/0/value");
                Preconditions.checkArgument(v.isTextual());
                try {
                    // 整数
                    BigInteger integer = new BigInteger(v.textValue());
                    return LongNode.valueOf(integer.longValue());
                } catch (NumberFormatException ignored) {
                }
                try {
                    // 分数
                    BigDecimal f = new BigDecimal(v.textValue());
                    return DecimalNode.valueOf(f);
                } catch (NumberFormatException ignored) {
                }
                if (v.textValue().length() == 0) {
                    return NullNode.getInstance();
                }
                int queId = answer.at("/queId").intValue();
                if (queId == 0) {
                    // 编号允许是字符串
                    return v;
                }
                return v;
            }
            case 5: {
                // 人
                ArrayNode res = mapper.createArrayNode();
                Streams.stream(values)
                        .map(v -> {
                            int uid = v.at("/id").intValue();
                            Preconditions.checkArgument(uid > 0, "missing id");
                            String name = v.at("/value").textValue();
                            String email = v.at("/email").textValue();
                            String otherInfo = v.at("/otherInfo").textValue();
                            return mapper.createObjectNode()
                                    .put("uid", uid)
                                    .put("name", name)
                                    .put("email", email)
                                    .put("head", otherInfo);
                        })
                        .forEach(res::add);
                return res;
            }
            case 22:/*部门*/ {
                ArrayNode res = mapper.createArrayNode();
                Streams.stream(values)
                        .map(i -> {
                            int id = i.at("/id").intValue();
                            Preconditions.checkArgument(id > 0, "missing id");
                            String name = i.at("/value").textValue();
                            return mapper.createObjectNode()
                                    .put("id", id)
                                    .put("name", name);
                        })
                        .forEach(res::add);
                return res;
            }
            case 14: /*起止时间*/ {
                Preconditions.checkArgument(values.size() == 1, answer);
                JsonNode v = answer.at("/values/0/value");
                Preconditions.checkArgument(v.isTextual());
                String[] value_ = v.textValue().split("~");
                ArrayNode res = mapper.createArrayNode();
                Arrays.stream(value_).forEach(res::add);
                return res;
            }
            case 18: {
                // 表格
                ArrayNode res = mapper.createArrayNode();
                answer.at("/tableValues")
                        .forEach(i -> res.add(answer2value(i)));
                return res;
            }
            default:
                throw new UnsupportedOperationException(String.format("%d:%s", type, answer));
        }
    }

    public static UserDefinedFunction answer2value = functions.udf((UDF1<String, String>) (String a) -> {
        return mapper.writeValueAsString(answer2value(mapper.readTree(a)));
    }, StringType$.MODULE$);

    /******************************************************************************************************************/

    public static Column json2patch(String json) {
        return udf((UDF1<?, ?>) (String p) -> {
            ObjectNode patchNode = (ObjectNode) mapper.readTree(p);
            ArrayNode arrayNode = mapper.createArrayNode();
            patchNode.fields().forEachRemaining(kv -> {
                arrayNode.addObject()
                        .put("op", "replace")
                        .put("path", "/" + kv.getKey())
                        .set("value", kv.getValue());
            });

            return arrayNode.toString();
            //throw new UnsupportedOperationException();
        }, StringType$.MODULE$).apply(expr(json));
    }

    public static Column jsonReplace(String a, String b, String path) {
        return udf((UDF2<?, ?, ?>) (String jsonA, String jsonB) -> {

            if (Objects.isNull(jsonB) || Objects.isNull(jsonA)) {
                return jsonA;
            }

            ObjectNode objNodeA = (ObjectNode) mapper.readTree(jsonA);
            JsonNode objNodeB = mapper.readTree(jsonB);

            String pathStr = path.substring(1);
            objNodeA.set(pathStr, objNodeB);

            return objNodeA.toString();
            //throw new UnsupportedOperationException();
        }, StringType$.MODULE$).apply(expr(a), expr(b));
    }

    public static Column patchFilter(String patch, String... retain) {
        return udf((UDF1<?, ?>) (String p) -> {

            ArrayNode a = mapper.createArrayNode();
            String pathValueStr = "";

            JsonNode jsonNode = mapper.readTree(p);

            for (JsonNode node : jsonNode) {
                switch (node.path("op").textValue()) {
                    case "replace":
                    case "add":

                        String pathStr = node.findValue("path").asText();

                        if (!node.at("/value").isObject()) {
                            boolean b = Arrays.stream(retain).anyMatch(i -> {
                                return (pathStr + "/").startsWith(i + "/");
                            });
                            if (b) {
                                a.add(node);
                            }
                        } else {

                            List<String> list = new LinkedList<>();
                            for (String tempStr : retain) {

                                boolean br = (tempStr + "/").startsWith(pathStr + "/");

                                if (br) {
                                    list.add(tempStr.substring(pathStr.length()));
                                }
                            }
                            String[] strArr = list.toArray(new String[0]);

                            ObjectNode o = (ObjectNode) node.at("/value");
                            recursionOp(o, strArr);
                            if (o.size() != 0) {
                                a.addObject()
                                        .put("op", node.findValue("op").asText())
                                        .put("path", node.path("path").asText())
                                        .set("value", o);
                            }
                        }

                        break;
                    case "remove":

                        pathValueStr = node.findValue("path").asText();

                        for (int j = 0; j < retain.length; j++) {
                            String tempStr = retain[j];

                            String[] pathValueArr = pathValueStr.split("/");
                            String[] tempStrArr = tempStr.split("/");

                            //路径深度一致, 判断是否完全一致
                            if (pathValueArr.length == tempStrArr.length && pathValueStr.equals(tempStr)) {
                                a.add(node);
                            } else if (pathValueArr.length > tempStrArr.length) {    //如果数据湖中的路径深度 > 传入需要判断的路径深度
                                if (pathValueStr.startsWith(tempStr)) {
                                    a.add(node);
                                }
                            } else {    //如果数据湖中的路径深度 < 传入的需要比较的路径深度
                                if (tempStr.startsWith(pathValueStr)) {
                                    a.add(node);
                                }
                            }
                        }

                        break;

                    default:
                        throw new UnsupportedOperationException("非add, remove, replace操作");

                }

            }

            return a.toString();
        }, StringType$.MODULE$).apply(expr(patch));
    }

    private static void recursionOp(ObjectNode obj, String... retainStrArr) {

        Map<String, String[]> p = new HashMap<>();
        String[] strArr;

        for (String retainStr : retainStrArr) {
            if (retainStr.length() == 0) {
                return;
            }
            int indexChar = retainStr.indexOf("/", 1);
            if (indexChar == -1) {
                p.put(retainStr, null);
            } else {
                List<String> listStr = new ArrayList<>();
                String preStr = retainStr.substring(0, indexChar);
                String aftStr = retainStr.substring(indexChar);
                listStr.add(aftStr);
                strArr = listStr.toArray(new String[0]);
                p.put(preStr, strArr);
            }
        }

        p.forEach((k, v) -> {
            if (!obj.at(k).isObject() || v == null) {
                return;
            }
            recursionOp((ObjectNode) obj.at(k), v);
        });

        String tempStr = "";
        List<String> tempList = p.keySet().stream().map(i -> {
            return i.substring(1);
        }).collect(Collectors.toList());

        obj.retain(tempList);

    }


    public static Column 分摊分成(String instantTime, String income, String subject,
                              String fixedAmount, String noLiquidRate, String liquidRate, String endDate) {
        return new UserDefinedAggregateFunction() {

            private int IDX_TOTALAMOUNT = 0;
            private int IDX_TOTALDAYS = 1;
            private int IDX_NOLIQUIDRATE = 2;
            private int IDX_LIQUIDRATE = 3;
            private int IDX_PREVIOUSNOLIQUIDSUM = 4;
            private int IDX_PREVIOUSLIQUIDSUM = 5;
            private int IDX_OUTAMOUNT = 6;
            private int IDX_PREVIOUSRATESUM = 7;
            private int IDX_WHICHDAY = 8;
            private int IDX_WHICHDATE = 9;

            @Override
            public StructType inputSchema() {
                return new StructType()
                        .add("date", "timestamp")
                        .add("收入", "long")
                        .add("科目", "string")
                        .add("固定金额", "long")
                        .add("非加液分成比例", "double")
                        .add("加液分成比例", "double")
                        .add("结束日", "timestamp");
            }

            @Override
            public StructType bufferSchema() {
                StructType aType = new StructType()
                        .add("totalAmount", "long")            // a 固定总金额 0
                        .add("totalDays", "int")                // d 总天数 1
                        .add("noLiquidRate", "double")        // k 非加液利率 2
                        .add("liquidRate", "double")            // q 加液利率 3
                        .add("previousNoLiquidSum", "long")    // San	上一天非加液收入总和 4
                        .add("previousLiquidSum", "long")    // Sbn  上一天非加液收入总和 5
                        .add("outAmount", "long")            // Sxn  上一天固定收入的支出总和 6
                        .add("previousRateSum", "long")        // Syn  上一天利率收入的支出总和 7
                        .add("whichDay", "int")                // n 执行到第几天的标志 8
                        .add("whichDate", "date")            // date  日期 9
                        ;

                return new StructType()
                        .add("tempArray", ArrayType.apply(
                                new StructType()
                                        .add("curr", aType)
                                        .add("next", aType))
                        );
            }

            @Override
            public DataType dataType() {
                return ArrayType.apply(new StructType()
                        .add("date", "date")
                        .add("amount", "long"));
            }

            @Override
            public boolean deterministic() {
                return false;
            }

            @Override
            public void initialize(MutableAggregationBuffer buffer) {

                List<Row> list = Lists.newArrayList();
                buffer.update(0, list.toArray(new Row[0]));
            }

            @Override
            public void update(MutableAggregationBuffer buffer, Row input) {

                List<Row> tempList = Lists.newArrayList(buffer.getList(0));

                Timestamp nextTimestamp = input.getAs(0);
                Date date2 = new Date(nextTimestamp.getTime());
                String subject = input.getAs(2);
                Long amount = input.getAs(1);
                Long nextSum = input.getAs(3);
                Double nextNoLiquidRate = input.getAs(4);
                Double nextLiquidRate = input.getAs(5);
                Timestamp endDay = input.getAs(6);

                int nextDays = 0;
                if (!Objects.isNull(endDay)) {
                    nextDays = (int) nextTimestamp.toLocalDateTime().toLocalDate().until(endDay.toLocalDateTime().toLocalDate(), ChronoUnit.DAYS);
                }

                // b1为false代表是有效策略行
                boolean b1 = Objects.isNull(nextSum) && Objects.isNull(nextNoLiquidRate)
                        && Objects.isNull(nextLiquidRate) && Objects.isNull(endDay);

                // 如果是结束日, 执行营收计算
                if (!Objects.isNull(endDay) && nextTimestamp.getTime() == endDay.getTime()) {
                    b1 = true;
                }

                if (!b1) {
                    nextSum = MoreObjects.firstNonNull(nextSum, 0L);
                    nextDays = MoreObjects.firstNonNull(nextDays, 0);
                    nextNoLiquidRate = MoreObjects.firstNonNull(nextNoLiquidRate, 0.0);
                    nextLiquidRate = MoreObjects.firstNonNull(nextLiquidRate, 0.0);

                    Row row1 = RowFactory.create(0L, 0, 0.0, 0.0, 0L, 0L, 0L, 0L, 0, null);
                    Row row2 = RowFactory.create(nextSum, nextDays, nextNoLiquidRate, nextLiquidRate, 0L, 0L, 0L, 0L, 0, date2);
                    Row tempAddRow = RowFactory.create(row1, row2);
                    tempList.add(tempAddRow);
                } else {
                    for (int i = 0; i < tempList.size(); i++) {
                        Row currType = tempList.get(i).getAs(1);
                        Row nextRow = updateNextRow(currType, nextTimestamp, subject, amount);
                        if (Objects.isNull(nextRow)) {
                            tempList.remove(i--);
                        } else {
                            Row evaRow = RowFactory.create(currType, nextRow);
                            tempList.set(i, evaRow);
                        }
                    }
                }
                buffer.update(0, tempList);
            }

            @Override
            public void merge(MutableAggregationBuffer buffer1, Row buffer2) {

            }

            @Override
            public Object evaluate(Row buffer) {

                List<Row> result = Lists.newArrayList();
                List<Row> bufferList = buffer.getList(0);

                for (int k = 0; k < bufferList.size(); k++) {
                    Row bufferListItem = bufferList.get(k);
                    Row currType = bufferListItem.getAs(0);
                    Row nextType = bufferListItem.getAs(1);

                    // 四个定值 总金额、总天数、加液利率、非加液利率
                    long sum = currType.getAs(IDX_TOTALAMOUNT);                            // 固定总金额		a
                    int days = currType.getAs(IDX_TOTALDAYS);                            // 总天数		d
                    double noLiquidRate = currType.getAs(IDX_NOLIQUIDRATE);                // 非加液利率		k
                    double liquidRate = currType.getAs(IDX_LIQUIDRATE);                    // 加液利率		q
                    long preNoLiquidSum = currType.getAs(IDX_PREVIOUSNOLIQUIDSUM);        // 上一天非加液收入总和	San
                    long preLiquidSum = currType.getAs(IDX_PREVIOUSLIQUIDSUM);            // 上一天加液收入总和		Sbn
                    long outAmount = currType.getAs(IDX_OUTAMOUNT);                        // 上一天固定收入的支出总和	Sxn
                    long preRateSum = currType.getAs(IDX_PREVIOUSRATESUM);                // 上一天固定收入的支出总和	Syn
                    int n = currType.getAs(IDX_WHICHDAY);                                // n1
                    Date date1 = currType.getAs(IDX_WHICHDATE);                            // date1
                    int n2 = nextType.getAs(IDX_WHICHDAY);                                // n2

                    for (int i = n; i < n2; i++) {

                        long res1 = Math.round(1.0 * sum / days * (i + 1) - outAmount);            // Sxn`
                        long res2 = Math.round(preNoLiquidSum * noLiquidRate + preLiquidSum * liquidRate - preRateSum);

                        if (preRateSum + res2 < sum) {
                            result.add(RowFactory.create(Date.valueOf(date1.toLocalDate()), res1));
                        } else if (preRateSum > sum) {
                            result.add(RowFactory.create(Date.valueOf(date1.toLocalDate()), res2));
                        } else {
                            long res = preRateSum + res2 - outAmount;
                            result.add(RowFactory.create(Date.valueOf(date1.toLocalDate()), res));
                        }

                        outAmount += res1;
                        preRateSum += res2;
                        date1 = Date.valueOf(date1.toLocalDate().plusDays(1));
                    }
                }

                return JavaConverters.asScalaIteratorConverter(result.iterator()).asScala().toSeq();
            }

            private Row updateNextRow(Row currType, Timestamp ts, String subject, Long amount) {
                Date date2 = new Date(ts.getTime());

                // 四个定值 总金额、总天数、加液利率、非加液利率
                long sum = currType.getAs(IDX_TOTALAMOUNT);                            // 固定总金额		a
                int days = currType.getAs(IDX_TOTALDAYS);                            // 总天数		d
                double noLiquidRate = currType.getAs(IDX_NOLIQUIDRATE);                // 非加液利率		k
                double liquidRate = currType.getAs(IDX_LIQUIDRATE);                    // 加液利率		q
                long preNoLiquidSum = currType.getAs(IDX_PREVIOUSNOLIQUIDSUM);        // 上一天非加液收入总和	San
                long preLiquidSum = currType.getAs(IDX_PREVIOUSLIQUIDSUM);            // 上一天加液收入总和		Sbn
                long outAmount = currType.getAs(IDX_OUTAMOUNT);                        // 上一天固定收入的支出总和	Sxn
                long preRateSum = currType.getAs(IDX_PREVIOUSRATESUM);                // 上一天分成收入的支出总和	Syn
                int n = currType.getAs(IDX_WHICHDAY);                                // n1
                Date date1 = currType.getAs(IDX_WHICHDATE);                            // date1

                if (n == days) {
                    return null;
                }

                int n2 = 0;
                if (!Objects.isNull(date1)) {
                    long differDays = date1.toLocalDate().until(date2.toLocalDate(), ChronoUnit.DAYS);
                    n2 = n + (int) differDays;
                }

                long liquidIncome = 0L;
                long noLiquidIncome = 0L;

                if (Strings.isNullOrEmpty(subject)) {
                    logger.warn("科目为空!!!");
                } else if (subject.equals("主营业务收入.收入.加液") || subject.equals("主营业务收入.退款.加液")) {
                    liquidIncome = amount;
                } else {
                    noLiquidIncome = amount;
                }

                for (int i = n; i < n2; i++) {

                    long res = Math.round(1.0 * sum / days * (i + 1) - outAmount);            // Sxn`
                    outAmount += res;

                    long res2 = Math.round(preNoLiquidSum * noLiquidRate + preLiquidSum * liquidRate - preRateSum);
                    preRateSum += res2;
                }

                preNoLiquidSum += noLiquidIncome;        // San`
                preLiquidSum += liquidIncome;            // Sbn`

                Row nextRow = RowFactory.create(sum, days, noLiquidRate, liquidRate, preNoLiquidSum,
                        preLiquidSum, outAmount, preRateSum, n2, date2);
                return nextRow;
            }

        }.apply(expr(instantTime), expr(income), expr(subject), expr(fixedAmount),
                expr(noLiquidRate), expr(liquidRate), expr(endDate));
    }

    public static Column 阶梯分成(String instantTime, String income, String subject,
                              String rate2, String liquidRate) {

        return new UserDefinedAggregateFunction() {

            private int IDX_PREVIOUSNOLIQUIDSUM = 0;
            private int IDX_PREVIOUSLIQUIDSUM = 1;
            private int IDX_PREVIOUSOUTSUM = 2;
            private int IDX_WHICHDAY = 3;
            private int IDX_WHICHDATE = 4;
            private int IDX_LIQUIDRATE = 5;
            private int IDX_NOLIQUIDRATE = 6;

            @Override
            public StructType inputSchema() {
                return new StructType()
                        .add("日期", "timestamp")
                        .add("收入", "long")
                        .add("科目", "string")
                        .add("其余阶梯合作分成比例", "array<struct<k:double,a:bigint>>")
                        .add("加液分成比例", "double")
                        ;
            }

            @Override
            public StructType bufferSchema() {

                StructType aType = new StructType()
                        .add("previousNoLiquidSum", "long")        // 上一天非加液收入总和 0
                        .add("previousLiquidSum", "long")        // 上一天加液收入总和   1
                        .add("previousOutSum", "long")            // 上一天支出总和		 2
                        .add("whichDay", "int")            // n 执行到第几天的标志	3
                        .add("whichDate", "date")        // date 日期	4
                        .add("liquidRate", "double")                // 加液分成比例		5
                        .add("ShareRate", "array<struct<k:double,a:bigint>>")    // 非加液分成比例  6
                        ;

                return new StructType()
                        .add("curr", aType)
                        .add("next", aType)
                        ;
            }

            @Override
            public DataType dataType() {
                return ArrayType.apply(new StructType()
                        .add("date", "date")
                        .add("amount", "long"));
            }

            @Override
            public boolean deterministic() {
                return false;
            }

            @Override
            public void initialize(MutableAggregationBuffer buffer) {
                Row row = RowFactory.create(0L, 0L, 0L, 0, null, 0.0, null);
                buffer.update(0, row);
                buffer.update(1, row);
            }

            @Override
            public void update(MutableAggregationBuffer buffer, Row input) {
                buffer.update(0, buffer.get(1));

                Timestamp nextTimestamp = input.getAs(0);
                Date date2 = new Date(nextTimestamp.getTime());
                String subject = input.getAs(2);
                WrappedArray<Row> nextNoLiquidRate = input.getAs(3);
                Double nextLiquidRate = input.getAs(4);

                Row currType = buffer.getAs(0);

                long previousNoLiquidSum = currType.getAs(IDX_PREVIOUSNOLIQUIDSUM);        // 上一天的非加液收入总和		S
                long previousLiquidSum = currType.getAs(IDX_PREVIOUSLIQUIDSUM);        // 上一日的加液收入总和
                long previousOutSum = currType.getAs(IDX_PREVIOUSOUTSUM);            // 上一天的支出总和
                int n = currType.getAs(IDX_WHICHDAY);                        // 执行到第几天的标志
                Date date1 = currType.getAs(IDX_WHICHDATE);                    // 日期
                double liquidRate = currType.getAs(IDX_LIQUIDRATE);            // 加液分成比例
                WrappedArray<Row> noLiquidRate = currType.getAs(IDX_NOLIQUIDRATE);    // 非加液分成比例

                int n2 = 0;
                if (!Objects.isNull(date1)) {
                    long differDays = date1.toLocalDate().until(date2.toLocalDate(), ChronoUnit.DAYS);
                    n2 = n + (int) differDays;
                }

                long liquidIncome = 0L;
                long noLiquidIncome = 0L;

                if (Strings.isNullOrEmpty(subject)) {
                    logger.warn("科目为空!!!");
                } else if (Objects.equals(subject, "主营业务收入.收入.加液") || Objects.equals(subject, "主营业务收入.退款.加液")) {
                    liquidIncome = input.getAs(1);
                } else {
                    noLiquidIncome = input.getAs(1);
                }

                for (int i = n; i < n2; i++) {

                    double liquidRes = previousLiquidSum * liquidRate;
                    double noLiquidRes = 0.0;

                    double preK = 0;
                    for (int j = 0; j < noLiquidRate.size(); j++) {
                        Row row = noLiquidRate.apply(j);
                        double k = row.getAs(0);
                        long a = row.getAs(1);
                        noLiquidRes += Math.max(0, previousNoLiquidSum - a) * (k - preK);
                        preK = k;
                    }

                    long res = Math.round(liquidRes + noLiquidRes - previousOutSum);
                    previousOutSum += res;
                }

                previousLiquidSum += liquidIncome;
                previousNoLiquidSum += noLiquidIncome;

                boolean b1 = Objects.isNull(nextNoLiquidRate) && Objects.isNull(nextLiquidRate);
                if (!b1) {
                    MoreObjects.firstNonNull(nextLiquidRate, 0.0);
                    Row nextRow = RowFactory.create(previousNoLiquidSum, previousLiquidSum, previousOutSum,
                            n2, date2, nextLiquidRate, nextNoLiquidRate);
                    buffer.update(1, nextRow);
                } else {
                    Row nextRow = RowFactory.create(previousNoLiquidSum, previousLiquidSum, previousOutSum,
                            n2, date2, liquidRate, noLiquidRate);
                    buffer.update(1, nextRow);
                }
            }

            @Override
            public void merge(MutableAggregationBuffer buffer1, Row buffer2) {

            }

            @Override
            public Object evaluate(Row buffer) {

                Row currType = buffer.getAs(0);
                Row nextType = buffer.getAs(1);

                long previousNoLiquidSum = currType.getAs(IDX_PREVIOUSNOLIQUIDSUM);        // 上一天的非加液收入总和		S
                long previousLiquidSum = currType.getAs(IDX_PREVIOUSLIQUIDSUM);        // 上一日的加液收入总和
                long previousOutSum = currType.getAs(IDX_PREVIOUSOUTSUM);            // 上一天的支出总和
                int n = currType.getAs(IDX_WHICHDAY);                        // 执行到第几天的标志
                Date date1 = currType.getAs(IDX_WHICHDATE);                    // 日期
                double liquidRate = currType.getAs(IDX_LIQUIDRATE);            // 加液分成比例
                WrappedArray<Row> noLiquidRate = currType.getAs(IDX_NOLIQUIDRATE);    // 非加液分成比例
                int n2 = nextType.getAs(IDX_WHICHDAY);                        // 日期n2

                List<Row> result = Lists.newArrayList();
                for (int i = n; i < n2; i++) {
                    double liquidRes = previousLiquidSum * liquidRate;
                    double noLiquidRes = 0.0;

                    double preK = 0;
                    for (int j = 0; j < noLiquidRate.size(); j++) {
                        Row row = noLiquidRate.apply(j);
                        double k = row.getAs(0);
                        long a = row.getAs(1);
                        noLiquidRes += Math.max(0, previousNoLiquidSum - a) * (k - preK);
                        preK = k;
                    }

                    long res = Math.round(liquidRes + noLiquidRes - previousOutSum);
                    result.add(RowFactory.create(date1, res));
                    previousOutSum += res;
                    date1 = Date.valueOf(date1.toLocalDate().plusDays(1));
                }
                return result.toArray(new Row[0]);
            }
        }.apply(expr(instantTime), expr(income), expr(subject),
                expr(rate2), expr(liquidRate));
    }

    // 将分摊分成和阶梯分成整合到一个函数中
    public static Column avgAmount(String instantTime, String income, String subject, String fixedAmount,
                                   String noLiquidRate, String liquidRate, String startDate, String endDate) {
        return new UserDefinedAggregateFunction() {

            private int IDX_TOTALAMOUNT = 0;
            private int IDX_TOTALDAYS = 1;
            private int IDX_NOLIQUIDRATE = 2;
            private int IDX_LIQUIDRATE = 3;
            private int IDX_PREVIOUSNOLIQUIDSUM = 4;
            private int IDX_PREVIOUSLIQUIDSUM = 5;
            private int IDX_OUTAMOUNT = 6;
            private int IDX_PREVIOUSRATESUM = 7;
            private int IDX_WHICHDAY = 8;
            private int IDX_WHICHDATE = 9;

            private static final int IDX_INPUT_DATE = 0;
            private static final int IDX_INPUT_INCOME = 1;
            private static final int IDX_INPUT_SUBJECT = 2;
            private static final int IDX_INPUT_FIXEDAMOUNT = 3;
            private static final int IDX_INPUT_NOLIQUID = 4;
            private static final int IDX_INPUT_LIQUID = 5;
            private static final int IDX_INPUT_STARTDATE = 6;
            private static final int IDX_INPUT_ENDDATE = 7;


            @Override
            public StructType inputSchema() {
                return new StructType()
                        .add("date", "timestamp")
                        .add("收入", "long")
                        .add("科目", "string")
                        .add("固定金额", "long")
                        .add("非加液分成比例", "array<struct<k:double,a:bigint>>")
                        .add("加液分成比例", "double")
                        .add("开始时间", "timestamp")
                        .add("结束时间", "timestamp")
                        ;
            }

            @Override
            public StructType bufferSchema() {
                StructType aType = new StructType()
                        .add("totalAmount", "long")            // a 固定总金额 0
                        .add("totalDays", "int")                // d 总天数 1
                        .add("noLiquidRate", "array<struct<k:double,a:bigint>>") // k 非加液利率 2
                        .add("liquidRate", "double")            // q 加液利率 3
                        .add("previousNoLiquidSum", "long")    // San	上一天非加液收入总和 4
                        .add("previousLiquidSum", "long")    // Sbn  上一天非加液收入总和 5
                        .add("outAmount", "long")            // Sxn  上一天固定收入的支出总和 6
                        .add("previousRateSum", "long")        // Syn  上一天利率收入的支出总和 7
                        .add("whichDay", "int")                // n 执行到第几天的标志 8
                        .add("whichDate", "date")            // date  日期 9
                        ;
                return new StructType()
                        .add("curr", aType)
                        .add("next", aType);
            }

            @Override
            public DataType dataType() {
                return ArrayType.apply(new StructType()
                        .add("date", "date")
                        .add("amount", "long"));
            }

            @Override
            public boolean deterministic() {
                return false;
            }

            @Override
            public void initialize(MutableAggregationBuffer buffer) {

                Row row = RowFactory.create(0L, 0, null, 0.0, 0L, 0L, 0L, 0L, 0, null);
                buffer.update(0, row);
                buffer.update(1, row);
            }

            @Override
            public void update(MutableAggregationBuffer buffer, Row input) {

                buffer.update(0, buffer.get(1));

                Timestamp currLineDate = input.getAs(IDX_INPUT_DATE);
                Long income = input.getAs(IDX_INPUT_INCOME);
                String subject = input.getAs(IDX_INPUT_SUBJECT);
                Long nextSum = input.getAs(IDX_INPUT_FIXEDAMOUNT);
                WrappedArray<Row> nextNoLiquidRate = input.getAs(IDX_INPUT_NOLIQUID);
                Double nextLiquidRate = input.getAs(IDX_INPUT_LIQUID);
                Timestamp startDate = input.getAs(IDX_INPUT_STARTDATE);
                Timestamp endDate = input.getAs(IDX_INPUT_ENDDATE);
                int days = (int) startDate.toLocalDateTime().until(endDate.toLocalDateTime(), ChronoUnit.DAYS);
                Date date2 = new Date(currLineDate.getTime());

                if (Objects.isNull(nextNoLiquidRate)) {
                    nextNoLiquidRate = WrappedArray.empty();
                }

                nextSum = MoreObjects.firstNonNull(nextSum, 0L);
                nextLiquidRate = MoreObjects.firstNonNull(nextLiquidRate, 0.0);

                Row currType = buffer.getAs(0);

                // 四个定值 总金额、总天数、加液利率、非加液利率
                double liquidRate = currType.getAs(IDX_LIQUIDRATE);                    // 加液利率		q
                long preNoLiquidSum = currType.getAs(IDX_PREVIOUSNOLIQUIDSUM);        // 上一天非加液收入总和	San
                long preLiquidSum = currType.getAs(IDX_PREVIOUSLIQUIDSUM);            // 上一天加液收入总和		Sbn
                long outAmount = currType.getAs(IDX_OUTAMOUNT);                        // 上一天固定收入的支出总和	Sxn
                long preRateSum = currType.getAs(IDX_PREVIOUSRATESUM);                // 上一天固定收入的支出总和	Syn
                int n = currType.getAs(IDX_WHICHDAY);                                // n1
                Date date1 = currType.getAs(IDX_WHICHDATE);                            // date1

                if (Objects.isNull(date1)) {
                    date1 = Date.valueOf(startDate.toLocalDateTime().toLocalDate());
                    Row tempRow = RowFactory.create(nextSum, days, nextNoLiquidRate, nextLiquidRate, 0L, 0L, 0L, 0L, 0, date1);
                    buffer.update(0, tempRow);
                }

                long differDays = date1.toLocalDate().until(date2.toLocalDate(), ChronoUnit.DAYS);
                int n2 = n + (int) differDays;

                long liquidIncome = 0L;
                long noLiquidIncome = 0L;
                long income1 = MoreObjects.firstNonNull(income, 0L);

                if (Strings.isNullOrEmpty(subject)) {
                    logger.warn("科目为空!!!");
                } else if (subject.equals("主营业务收入.收入.加液") || subject.equals("主营业务收入.退款.加液")) {
                    liquidIncome = income1;
                } else {
                    noLiquidIncome = income1;
                }

                for (int i = n; i < n2; i++) {

                    outAmount = fixedAmount(i + 1, days, nextSum) - fixedAmount(i, days, nextSum);
                    long res2 = shareSum(preLiquidSum, liquidRate, preNoLiquidSum, nextNoLiquidRate) - preRateSum;
                    preRateSum += res2;
                }

                preNoLiquidSum += noLiquidIncome;        // San`
                preLiquidSum += liquidIncome;            // Sbn`

                Row nextRow = RowFactory.create(nextSum, days, nextNoLiquidRate, nextLiquidRate, preNoLiquidSum,
                        preLiquidSum, outAmount, preRateSum, n2, date2);

                buffer.update(1, nextRow);
            }

            @Override
            public void merge(MutableAggregationBuffer buffer1, Row buffer2) {

            }

            @Override
            public Object evaluate(Row buffer) {

                Row currType = buffer.getAs(0);
                Row nextType = buffer.getAs(1);

                // 四个定值 总金额、总天数、加液利率、非加液利率
                long sum = currType.getAs(IDX_TOTALAMOUNT);                            // 固定总金额		a
                int days = currType.getAs(IDX_TOTALDAYS);                            // 总天数		d
                WrappedArray<Row> noLiquidRate = currType.getAs(IDX_NOLIQUIDRATE);        // 非加液利率		k
                double liquidRate = currType.getAs(IDX_LIQUIDRATE);                    // 加液利率		q
                long preNoLiquidSum = currType.getAs(IDX_PREVIOUSNOLIQUIDSUM);        // 上一天非加液收入总和	San
                long preLiquidSum = currType.getAs(IDX_PREVIOUSLIQUIDSUM);            // 上一天加液收入总和		Sbn
                long outAmount = currType.getAs(IDX_OUTAMOUNT);                        // 上一天固定收入的支出总和	Sxn
                long preRateSum = currType.getAs(IDX_PREVIOUSRATESUM);                // 上一天固定收入的支出总和	Syn
                int n = currType.getAs(IDX_WHICHDAY);                                // n1
                Date date1 = currType.getAs(IDX_WHICHDATE);                            // date1
                int n2 = nextType.getAs(IDX_WHICHDAY);                                // n2

                Stream<Row> stream = Stream.iterate(n, i -> ++i)
                        .limit(n2 - n)
                        .flatMap((Integer i) -> {
                            LocalDate date = date1.toLocalDate().plusDays(i - n);
                            long res1 = fixedAmount(i + 1, days, sum) - fixedAmount(i, days, sum);
                            long res2 = 0L;
                            if (i == n) {
                                res2 = shareSum(preLiquidSum, liquidRate, preNoLiquidSum, noLiquidRate) - preRateSum;
                            }
                            if (preRateSum + res2 < sum) {
                                if (res1 == 0) return Stream.of();
                                return Stream.of(RowFactory.create(Date.valueOf(date), res1));
                            } else if (preRateSum > sum) {
                                if (res2 == 0) return Stream.of();
                                return Stream.of(RowFactory.create(Date.valueOf(date), res2));
                            } else {
                                long res = preRateSum + res2 - fixedAmount(i, days, sum);
                                if (res == 0) return Stream.of();
                                return Stream.of(RowFactory.create(Date.valueOf(date), res));
                            }
                        });

                Iterator<Row> it = new Iterator<Row>() {

                    @Override
                    public boolean hasNext() {
                        return false;
                    }

                    @Override
                    public Row next() {
                        return null;
                    }
                };

                return JavaConverters.asScalaIteratorConverter(stream.iterator()).asScala().toSeq();
            }

            private long fixedAmount(int day, int days, long sum) {
                if (day < 0) {
                    return 0L;
                }
                long res1 = Math.round(1.0 * sum / days * day);
                return res1;
            }

            private long shareSum(long liquidIncome, double liquidRate, long noLiquidIncome, WrappedArray<Row> noLiquidRate) {
                double liquid = liquidIncome * liquidRate;
                double noliquid = 0.0;
                double preK = 0;
                // 专门计算非加液的阶梯分成部分金额
                for (int j = 0; j < noLiquidRate.size(); j++) {
                    Row row = noLiquidRate.apply(j);
                    double k = row.getAs(0);
                    long a = row.getAs(1);
                    noliquid += Math.max(0, noLiquidIncome - a) * (k - preK);
                    preK = k;
                }
                return Math.round(liquid + noliquid);
            }

        }.apply(expr(instantTime), expr(income), expr(subject), expr(fixedAmount),
                expr(noLiquidRate), expr(liquidRate), expr(startDate), expr(endDate));
    }

    public static Column 分摊策略(String ts1, String share1, String biz1) {

        final int IDX_NO = 0;
        final int IDX_PAYEE = 1;
        final int IDX_ACCOUNT = 2;
        final int IDX_SUBJECT = 3;
        final int IDX_AMOUNT = 4;
        final int IDX_NOLIQUID = 5;
        final int IDX_LIQUID = 6;
        final int IDX_TIME = 7;

        return udf((UDF3<Timestamp, Row, Row, Row[]>) (Timestamp ts, Row share, Row biz) -> {

            String subject = share.getAs(IDX_SUBJECT);
            Long amount = (Long) Optional.ofNullable(share.getAs(IDX_AMOUNT)).orElse(0L);
            WrappedArray<Row> noLiquid = share.getAs(IDX_NOLIQUID);
            Double liquid = share.getAs(IDX_LIQUID);
            Timestamp startTs = share.getAs(IDX_TIME);

            if (subject.equals("主营业务成本.装修费") || subject.equals("主营业务成本.装修费（空间设计费）")) {

                Timestamp endTs = biz.getAs("合同截止日期");
                LocalDate startTime = startTs.toLocalDateTime().toLocalDate();
                LocalDate endTime = endTs.toLocalDateTime().toLocalDate();
                long years = startTime.until(endTime.plusYears(1).minusDays(1), ChronoUnit.YEARS);
                if (years <= 3L) {
                    endTime = startTime.plusYears(3);
                } else if (years <= 5) {

                } else {
                    endTime = startTime.plusYears(5);
                }

                List<Row> result = Lists.newArrayList();
                // 计算months
                long days = startTime.until(endTime, ChronoUnit.DAYS);
                List<Integer> months = Lists.newArrayList();
                int k = 1;
                for (LocalDate start = startTime; start.compareTo(endTime) < 0; start = startTime.plusMonths(k++)) {
                    LocalDate end = Stream.of(start.plusMonths(1), endTime).min(LocalDate::compareTo).get();
                    months.add((int) start.until(end, ChronoUnit.DAYS));
                }
                Preconditions.checkArgument(months.stream().mapToInt(i -> i).sum() == days, "months中天数不正确!");
                Iterator<Long> eachMonthAmountIt = shareAmount(amount, months).iterator();

                int j = 1;
                for (LocalDate start = startTime; start.compareTo(endTime) < 0; start = startTime.plusMonths(j++)) {
                    Long currAmount = eachMonthAmountIt.hasNext() ? eachMonthAmountIt.next() : 0L;
                    LocalDate end = Stream.of(start.plusMonths(1), endTime).min(LocalDate::compareTo).get();

                    Timestamp resultStartTs = Timestamp.valueOf(start.atTime(0, 0));
                    Timestamp resultEndTs = Timestamp.valueOf(end.atTime(0, 0));
                    result.add(RowFactory.create(currAmount, noLiquid, liquid, resultStartTs, resultEndTs));
                }

                return result.toArray(new Row[0]);
            } else {
                return new Row[]{
                        RowFactory.create(amount, noLiquid, liquid, startTs, ts)
                };
            }

        }, ArrayType.apply(new StructType().add("固定金额", DataTypes.LongType)
                .add("非加液分成比例", ArrayType.apply(new StructType().add("k", DataTypes.DoubleType).add("a", DataTypes.LongType)))
                .add("加液分成比例", DataTypes.DoubleType)
                .add("开始时间", DataTypes.TimestampType)
                .add("结束时间", DataTypes.TimestampType)
        )).apply(expr(ts1), expr(share1), expr(biz1));
    }

    private static Iterable<Long> shareAmount(Long amount, List<Integer> months) {

        int length = months.stream().mapToInt(i -> i).sum();
        List<Long> list = Lists.newArrayList();
        Iterator<Integer> it = months.iterator();
        int sum = 0;
        while (it.hasNext()) {
            int currMonthDays = it.next();
            list.add(Math.round(1.0 * (sum + currMonthDays) / length * amount) - Math.round(1.0 * sum / length * amount));
            sum += currMonthDays;
        }
        return list;
    }

    public static Column merge(String col, StructType type) {

        return new UserDefinedAggregateFunction() {
            @Override
            public StructType inputSchema() {
                return new StructType()
                        .add("a", type);
            }

            @Override
            public StructType bufferSchema() {
                return type;
            }

            @Override
            public DataType dataType() {
                return type;
            }

            @Override
            public boolean deterministic() {
                return false;
            }

            @Override
            public void initialize(MutableAggregationBuffer buffer) {

                // 这里先构建一个 [null, null]的初始化结构体
                // 注意此处的buffer从初始化开始 就会贯穿整个函数 -> update() -> merge() -> evaluate()
                for (int i = 0; i < buffer.length(); i++) {
                    buffer.update(i, null);
                }
            }

            @Override
            public void update(MutableAggregationBuffer buffer, Row input) {    //input是一个[[a1, b1]]

                //row取第一个结构体[a1, b1]
                Row row = input.getAs(0);

                //row有多少列, 就和buffer比较多少次
                for (int i = 0; i < row.length(); i++) {
                    Object tempInput = row.get(i);
                    if(tempInput != null){
                        buffer.update(i, tempInput);
                    }
                }
            }

            @Override
            public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Object evaluate(Row buffer) {
                return buffer;
            }
        }.apply(expr(col));
    }
}
