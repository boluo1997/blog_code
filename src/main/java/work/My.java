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
                              String fixedAmount, String noLiquidRatio, String liquidRatio, String endDate) {
        return new UserDefinedAggregateFunction() {

            private int IDX_ISOUTPUT = 0;
            private int IDX_WHICHLINE = 1;
            private int IDX_LASTDAY = 2;
            private int IDX_TODAY = 3;
            private int IDX_TOTALDAYS = 4;
            private int IDX_WHICHDAY = 5;
            private int IDX_TOTALAMOUNT = 6;
            private int IDX_OUTAMOUNT = 7;
            private int IDX_PREVIOUSDAY = 8;
            private int IDX_PREVIOUSRESULT = 9;
            private int IDX_THERESULT = 10;
            private int IDX_PREVIOUSRATESUM = 11;
            private int IDX_PREVIOUSlIQUIDSUM = 12;
            private int IDX_PREVIOUSNOLIQUIDSUM = 13;
            private int IDX_LIQUIDRATE = 14;
            private int IDX_NOLIQUIDRATE = 15;

            @Override
            public StructType inputSchema() {
                return new StructType()
                        .add("date", "timestamp")
                        .add("收入", "long")
                        .add("科目", "string")
                        .add("固定金额", "long")
                        .add("非加液分成比例", "double")
                        .add("加液分成比例", "double")
                        .add("结束日", "date");
            }

            @Override
            public StructType bufferSchema() {
                return new StructType()
                        .add("isOutput", "boolean")			// 0本行是否输出
                        .add("whichLine", "int")				// 1执行到第几行了
                        .add("lastDay", "date")				// 2上一天
                        .add("today", "date")				// 3今天
                        .add("totalDays", "double")			// 4总天数
                        .add("whichDay", "int")				// 5执行到第几天
                        .add("totalAmount", "long")			// 6固定总金额
                        .add("outAmount", "long")			// 7已经分配的金额
                        .add("previousDay", "int")			// 8上一行执行到的天数
                        .add("previousResult", "long")		// 9上一行结束时的累加
                        .add("theResult", "long")			// 10本行的结果
                        .add("previousRateSum", "long") 		// 11上一行结束时, 分成收入总和.加液非加液
                        .add("previousLiquidSum", "long")	// 12上一行结束时, 加液收入总和
                        .add("previousNoLiquidSum", "long") 	// 13上一行结束时, 非加液收入总和
                        .add("liquidRate", "double")			// 14加液利率
                        .add("noLiquidRate", "double");		// 15非加液利率

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
                buffer.update(IDX_ISOUTPUT, false);
                buffer.update(IDX_WHICHLINE, 1);
                buffer.update(IDX_LASTDAY, null);
                buffer.update(IDX_TODAY, null);
                buffer.update(IDX_TOTALDAYS, 0.0);
                buffer.update(IDX_WHICHDAY, 1);			// 5
                buffer.update(IDX_TOTALAMOUNT, 0L);
                buffer.update(IDX_OUTAMOUNT, 0L);
                buffer.update(IDX_PREVIOUSDAY, 0);
                buffer.update(IDX_PREVIOUSRESULT, 0L);
                buffer.update(IDX_THERESULT, 0L);			// 10
                buffer.update(IDX_PREVIOUSRATESUM, 0L);
                buffer.update(IDX_PREVIOUSlIQUIDSUM, 0L);
                buffer.update(IDX_PREVIOUSNOLIQUIDSUM, 0L);
                buffer.update(IDX_LIQUIDRATE, 0.0);
                buffer.update(IDX_NOLIQUIDRATE, 0.0);
            }

            @Override
            public void update(MutableAggregationBuffer buffer, Row input) {
                buffer.update(IDX_LASTDAY, buffer.get(IDX_TODAY));
                buffer.update(IDX_PREVIOUSRESULT, buffer.get(IDX_OUTAMOUNT));
                buffer.update(IDX_PREVIOUSDAY, buffer.get(IDX_WHICHDAY));

                Timestamp startTs = input.getAs(0);        // 输出起始日, 上一天, 因为今天要输出上一天的结果
                Date inputTodayDate = new Date(startTs.getTime());

                Date inputEndDay = input.getAs(6);
                Date lastDay = buffer.getAs(IDX_LASTDAY);
                int lines = buffer.getAs(IDX_WHICHLINE);
                // 四个定值 总金额、总天数、加液利率、非加液利率
                long sum = input.getAs(3);                	// 固定总金额
                double days = buffer.getAs(IDX_TOTALDAYS);      // 总天数
                double liquidRate = input.getAs(5);			// 加液利率
                double noLiquidRate = input.getAs(4);			// 非加液利率

                if (lines == 1) {     //拿初始日, 计算总天数
                    int startDay = inputEndDay.toLocalDate().getDayOfYear() - inputTodayDate.toLocalDate().getDayOfYear();
                    buffer.update(IDX_TOTALDAYS, (double) startDay);
                }

                if (Objects.isNull(lastDay) || lastDay.toLocalDate().compareTo(inputTodayDate.toLocalDate()) == 0){
                    buffer.update(IDX_ISOUTPUT, false);    // 本行不输出
                } else {
                    buffer.update(IDX_ISOUTPUT, true);

                    for (LocalDate startDay = lastDay.toLocalDate(); startDay.compareTo(inputTodayDate.toLocalDate()) < 0; startDay = startDay.plusDays(1)) {
                        int whichDay = buffer.getAs(IDX_WHICHDAY);        					// 执行到第几天的数据了(不是第几行)
                        long alreadyConsume = buffer.getAs(IDX_OUTAMOUNT);        			// 已经分配出去的金额

                        // 取上一天的加液收入和  以及  上一天的非加液收入和

                        long res = Math.round(whichDay / days * sum - alreadyConsume);

                        buffer.update(IDX_OUTAMOUNT, alreadyConsume + res);        	// 这一行结束后, 分配出去的总金额
                        buffer.update(IDX_WHICHDAY, whichDay + 1);                	// 这一行结束后, 会执行到第几天

                        // 更新这一天非加液收入和  以及  上一天的非加液收入和

                    }
                }

                buffer.update(IDX_TODAY, inputTodayDate);		// 今天的日期
                buffer.update(IDX_WHICHLINE, lines + 1);
                buffer.update(IDX_TOTALAMOUNT, sum);			// 固定金额总金额
                buffer.update(IDX_LIQUIDRATE, liquidRate);		// 加液利率
                buffer.update(IDX_NOLIQUIDRATE, noLiquidRate);	// 非加液利率
            }

            @Override
            public void merge(MutableAggregationBuffer buffer1, Row buffer2) {

            }

            @Override
            public Object evaluate(Row buffer) {
                boolean b1 = buffer.getAs(IDX_ISOUTPUT);
                if (!b1) {
                    return null;
                } else {
                    List<Row> result = Lists.newArrayList();
                    Date lastDay = buffer.getAs(IDX_LASTDAY);           //上一行日期
                    Date todayDay = buffer.getAs(IDX_TODAY);            //本行日期

                    int lastDayCount = buffer.getAs(IDX_PREVIOUSDAY);     // 上一行开始是第几天
                    double days = buffer.getAs(IDX_TOTALDAYS);            // 总天数
                    long sum = buffer.getAs(IDX_TOTALAMOUNT);             // 总金额
                    long lastSumMoney = buffer.getAs(IDX_PREVIOUSRESULT); // 上一行结束时的累加金额

                    for (LocalDate startDay = lastDay.toLocalDate(); startDay.compareTo(todayDay.toLocalDate()) < 0; startDay = startDay.plusDays(1)) {
                        long res = Math.round(lastDayCount++ / days * sum - lastSumMoney);
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
