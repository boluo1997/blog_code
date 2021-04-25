package work;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;
import com.github.fge.jsonpatch.JsonPatch;
import com.github.fge.jsonpatch.JsonPatchException;
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

    public static Column 分摊分成(String instantTime, String income, String subject,
                              String fixedAmount, String liquidRatio, String noLiquidRatio, String endDate) {
        return new UserDefinedAggregateFunction() {

            private int IDX_LASTDAY = 0;

            @Override
            public StructType inputSchema() {
                return new StructType()
                        .add("date", "timestamp")
                        .add("收入", "long")
                        .add("科目", "string")
                        .add("固定金额", "string")
                        .add("加液分成比例", "string")
                        .add("非加液分成比例", "string")
                        .add("结束日", "date");
            }

            @Override
            public StructType bufferSchema() {
                return new StructType()
                        .add("lastDay", "date")                 // 0上一天
                        .add("today", "date")                   // 1今天
                        .add("endDay", "date")                  // 2结束日
                        .add("daySumAmount", "long")            // 3今天(未结束)的金额
                        .add("fixedAmount", "long")             // 4固定总金额
                        .add("alreadyOutAmount", "long")        // 5已经分配的金额
                        .add("isOutput", "boolean")             // 6本行是否输出
                        .add("line", "int")                     // 7执行到第几行了
                        .add("days", "double")                  // 8总天数
                        .add("whichDay", "int")              // 9执行到第几天
                        .add("res", "long")                     // 10本行的结果
                        .add("lastAmount", "long")                // 11上一行结束时的累加
                        .add("lastDay1", "int");                  // 12上一行执行到的天数

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
                buffer.update(IDX_LASTDAY, null);
                buffer.update(1, null);
                buffer.update(2, null);
                buffer.update(3, 0L);
                buffer.update(4, 0L);
                buffer.update(5, 0L);
                buffer.update(6, false);
                buffer.update(7, 1);
                buffer.update(8, 0.0);
                buffer.update(9, 1);
                buffer.update(10, 0L);
                buffer.update(11, 0L);

            }

            @Override
            public void update(MutableAggregationBuffer buffer, Row input) {
                buffer.update(0, buffer.get(1));
                buffer.update(11, buffer.get(5));
                buffer.update(12, buffer.get(9));

                Timestamp startTs = (Timestamp) input.get(0);        // 输出起始日, 上一天, 因为今天要输出上一天的结果
                Date inputTodayDate = new Date(startTs.getTime());

                Date inputEndDay = (Date) input.get(6);
                Date lastDay = (Date) buffer.get(IDX_LASTDAY);
                int lines = (int) buffer.get(7);
                long sum = Long.parseLong((String) input.get(3));                // 固定总金额

                if (lines == 1) {     //拿初始日, 计算总天数
                    int days = inputEndDay.toLocalDate().getDayOfYear() - inputTodayDate.toLocalDate().getDayOfYear();
                    buffer.update(8, (double) days);
                }

                if (Objects.isNull(lastDay) ||
                        lastDay.toLocalDate().getDayOfYear() == inputTodayDate.toLocalDate().getDayOfYear()) {

                    buffer.update(6, false);    // 本行不输出
                } else {
                    buffer.update(6, true);

                    double days = (double) buffer.get(8);            // 总天数

                    for (LocalDate startDay = lastDay.toLocalDate(); startDay.compareTo(inputTodayDate.toLocalDate()) < 0; startDay = startDay.plusDays(1)) {
                        int whichDay = buffer.getAs(9);        // 执行到第几天的数据了(不是第几行)
                        long alreadyConsume = buffer.getAs(5);        // 已经分配出去的金额
                        long res = Math.round(whichDay / days * sum - alreadyConsume);

                        buffer.update(5, alreadyConsume + res);        // 这一行结束后, 分配出去的总金额
                        buffer.update(9, whichDay + 1);                // 这一行结束后, 会执行到第几天
                        buffer.update(10, res);    // 这一行最后的结果
                    }

                }

                buffer.update(1, inputTodayDate);
                buffer.update(2, inputEndDay);
                buffer.update(7, lines + 1);
                buffer.update(4, sum);
            }

            @Override
            public void merge(MutableAggregationBuffer buffer1, Row buffer2) {

            }

            @Override
            public Object evaluate(Row buffer) {
                boolean b1 = (boolean) buffer.get(6);
                if (!b1) {
                    return null;
                } else {
                    List<Row> result = Lists.newArrayList();
                    Date lastDay = (Date) buffer.get(0);
                    Date todayDay = (Date) buffer.get(1);


                    int lastDayCount = buffer.getAs(12);
                    int todayCount = buffer.getAs(9);
                    double days = (double) buffer.get(8);            // 总天数
                    long sum = (long) buffer.get(4);            // 总金额

                    long lastSumMoney = buffer.getAs(11);
                    long todaySumMoney = buffer.getAs(5);

                    for (LocalDate startDay = lastDay.toLocalDate(); startDay.compareTo(todayDay.toLocalDate()) < 0; startDay = startDay.plusDays(1)) {

                        long res = Math.round( lastDayCount++ / days * sum - lastSumMoney);
                        result.add(RowFactory.create(Date.valueOf(startDay), res));
                        lastSumMoney += res;

                    }

                    return result.toArray(new Row[0]);
                }
            }

        }.apply(expr(instantTime), expr(income), expr(subject), expr(fixedAmount),
                expr(liquidRatio), expr(noLiquidRatio), expr(endDate));
    }
}
