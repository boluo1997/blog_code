package work;

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
                        .add("总天数", "int");
            }

            @Override
            public StructType bufferSchema() {
                StructType aType = new StructType()
                        .add("totalAmount", "long")			// a 固定总金额 0
                        .add("totalDays", "int")				// d 总天数 1
                        .add("noLiquidRate", "double")		// k 非加液利率 2
                        .add("liquidRate", "double")			// q 加液利率 3
                        .add("previousNoLiquidSum", "long")	// San	上一天非加液收入总和 4
                        .add("previousLiquidSum", "long")	// Sbn  上一天非加液收入总和 5
                        .add("outAmount", "long")			// Sxn  上一天固定收入的支出总和 6
                        .add("previousRateSum", "long")		// Syn  上一天利率收入的支出总和 7
                        .add("whichDay", "int")				// n 执行到第几天的标志 8
                        .add("whichDate", "date")			// date  日期 9
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

                Row row = RowFactory.create(0L, 0, 0.0, 0.0, 0L, 0L, 0L, 0L, 0, null);
                buffer.update(0, row);
                buffer.update(1, row);
            }

            @Override
            public void update(MutableAggregationBuffer buffer, Row input) {

                buffer.update(0, buffer.get(1));

                Timestamp nextTimestamp = input.getAs(0);
                Date date2 = new Date(nextTimestamp.getTime());

                Row currType = buffer.getAs(0);
                // 四个定值 总金额、总天数、加液利率、非加液利率
                long sum = currType.getAs(IDX_TOTALAMOUNT);							// 固定总金额		a
                int days = currType.getAs(IDX_TOTALDAYS);							// 总天数		d
                double noLiquidRate = currType.getAs(IDX_NOLIQUIDRATE);				// 非加液利率		k
                double liquidRate = currType.getAs(IDX_LIQUIDRATE);					// 加液利率		q
                long preNoLiquidSum = currType.getAs(IDX_PREVIOUSNOLIQUIDSUM);		// 上一天非加液收入总和	San
                long preLiquidSum = currType.getAs(IDX_PREVIOUSLIQUIDSUM);			// 上一天加液收入总和		Sbn
                long outAmount = currType.getAs(IDX_OUTAMOUNT);						// 上一天固定收入的支出总和	Sxn
                long preRateSum = currType.getAs(IDX_PREVIOUSRATESUM);				// 上一天固定收入的支出总和	Syn
                int n = currType.getAs(IDX_WHICHDAY);								// n1
                Date date1 = currType.getAs(IDX_WHICHDATE);							// date1

                int n2 = 0;
                if (!Objects.isNull(date1)) {
                    n2 = n + date2.toLocalDate().compareTo(date1.toLocalDate());	// n2 计算得出
                }

                long liquidIncome = 0L;
                long noLiquidIncome = 0L;

                String subject = input.getAs(2);
                if (Strings.isNullOrEmpty(subject)) {

                } else if (subject.equals("主营业务收入.收入.加液") || subject.equals("主营业务收入.退款.加液")) {
                    liquidIncome = input.getAs(1);
                } else {
                    noLiquidIncome = input.getAs(1);
                }

                for (int i = n; i < n2; i++) {

                    long res = Math.round(1.0 * sum / days * (i+1) - outAmount);			// Sxn`
                    outAmount += res;

                    long res2 = Math.round(preNoLiquidSum * noLiquidRate + preLiquidSum * liquidRate - preRateSum);
                    preRateSum += res2;
                }
                preNoLiquidSum += noLiquidIncome;		// San`
                preLiquidSum += liquidIncome;			// Sbn`

                Long nextSum = input.getAs(3);
                Double nextNoLiquidRate = input.getAs(4);
                Double nextLiquidRate = input.getAs(5);
                Integer nextDays = input.getAs(6);
                boolean b1 = Objects.isNull(nextSum) && Objects.isNull(nextNoLiquidRate)
                        && Objects.isNull(nextLiquidRate) && Objects.isNull(nextDays);
                if(!b1){
                    nextSum = MoreObjects.firstNonNull(nextSum, 0L);
                    nextDays = MoreObjects.firstNonNull(nextDays, 0);
                    nextNoLiquidRate = MoreObjects.firstNonNull(nextNoLiquidRate, 0.0);
                    nextLiquidRate = MoreObjects.firstNonNull(nextLiquidRate, 0.0);
                    Row nextRow2 = RowFactory.create(nextSum, nextDays, nextNoLiquidRate, nextLiquidRate, preNoLiquidSum,
                            preLiquidSum, outAmount, preRateSum, n2, date2);
                    buffer.update(1, nextRow2);
                }else {
                    Row nextRow = RowFactory.create(sum, days, noLiquidRate, liquidRate, preNoLiquidSum,
                            preLiquidSum, outAmount, preRateSum, n2, date2);
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

                // 四个定值 总金额、总天数、加液利率、非加液利率
                long sum = currType.getAs(IDX_TOTALAMOUNT);							// 固定总金额		a
                int days = currType.getAs(IDX_TOTALDAYS);							// 总天数		d
                double noLiquidRate = currType.getAs(IDX_NOLIQUIDRATE);				// 非加液利率		k
                double liquidRate = currType.getAs(IDX_LIQUIDRATE);					// 加液利率		q
                long preNoLiquidSum = currType.getAs(IDX_PREVIOUSNOLIQUIDSUM);		// 上一天非加液收入总和	San
                long preLiquidSum = currType.getAs(IDX_PREVIOUSLIQUIDSUM);			// 上一天加液收入总和		Sbn
                long outAmount = currType.getAs(IDX_OUTAMOUNT);						// 上一天固定收入的支出总和	Sxn
                long preRateSum = currType.getAs(IDX_PREVIOUSRATESUM);				// 上一天固定收入的支出总和	Syn
                int n = currType.getAs(IDX_WHICHDAY);								// n1
                Date date1 = currType.getAs(IDX_WHICHDATE);							// date1
                int n2 = nextType.getAs(IDX_WHICHDAY);								// n2

                List<Row> result = Lists.newArrayList();
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
                return result.toArray(new Row[0]);
            }
        }.apply(expr(instantTime), expr(income), expr(subject), expr(fixedAmount),
                expr(noLiquidRate), expr(liquidRate), expr(endDate));
    }


    public static Column 阶梯分成(String instantTime, String income, String subject,
                              String rate2, String liquidRate) {

        return new UserDefinedAggregateFunction(){

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
                        .add("previousNoLiquidSum", "long")		// 上一天非加液收入总和 0
                        .add("previousLiquidSum", "long")		// 上一天加液收入总和   1
                        .add("previousOutSum", "long")			// 上一天支出总和		  2
                        .add("whichDay", "int")			// n 执行到第几天的标志	3
                        .add("whichDate", "date")		// date 日期	4
                        .add("liquidRate", "double")				// 加液分成比例		5
                        .add("ShareRate", "array<struct<k:double,a:bigint>>")	// 分成比例  6
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

                long previousNoLiquidSum = currType.getAs(0);		// 上一天的非加液收入总和		S
                long previousLiquidSum = currType.getAs(1);		// 上一日的加液收入总和
                long previousOutSum = currType.getAs(2);			// 上一天的支出总和
                int n = currType.getAs(3);						// 执行到第几天的标志
                Date date1 = currType.getAs(4);					// 日期
                double liquidRate = currType.getAs(5);			// 加液分成比例

                int n2 = 0;
                if (!Objects.isNull(date1)) {
                    n2 = date2.toLocalDate().compareTo(date1.toLocalDate());
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

                    for (int j = 0; j < nextNoLiquidRate.size(); j++) {
                        Row row = nextNoLiquidRate.apply(j);
                        System.out.println(row);
                    }

                    double noLiquidRes = previousNoLiquidSum;
                    long res = Math.round(liquidRes + noLiquidRes - previousOutSum);
                    previousOutSum += res;
                }

                previousLiquidSum += liquidIncome;
                previousNoLiquidSum += noLiquidIncome;

                boolean b1 = Objects.isNull(nextNoLiquidRate) && Objects.isNull(nextLiquidRate);
                if (!b1) {
                    MoreObjects.firstNonNull(nextLiquidRate, 0.0);
                    Row nextRow = RowFactory.create();
                    buffer.update(1, nextRow);
                } else {
                    Row nextRow = RowFactory.create();
                }
            }

            @Override
            public void merge(MutableAggregationBuffer buffer1, Row buffer2) {

            }

            @Override
            public Object evaluate(Row buffer) {
                return null;
            }
        }.apply(expr(instantTime), expr(income), expr(subject),
                expr(rate2), expr(liquidRate));
    }
}
