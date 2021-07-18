package boluo.work;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.fge.jsonpatch.JsonPatch;
import com.github.fge.jsonpatch.JsonPatchException;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.commons.io.FilenameUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.*;
import scala.collection.mutable.WrappedArray;

import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

public class Func {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static Column patchFilter(String patch, String... retain) {
        return udf((UDF1<?, ?>) (String p) -> {

            ArrayNode a = mapper.createArrayNode();
            String pathValueStr = "";

            JsonNode jsonNode = mapper.readTree(p);

            for (JsonNode node : jsonNode) {
                switch (node.path("op").textValue()) {
                    case "replace":

                        String pathStr = node.findValue("path").asText();

                        if(!node.at("/value").isObject()){
                            boolean b = Arrays.stream(retain).anyMatch(i -> {
                                return (pathStr+"/").startsWith(i+"/");
                            });
                            if(b){
                                a.add(node);
                            }
                        } else {
                            // value是一个对象

                            List<String> list = new LinkedList<>();
                            for (String tempStr : retain) {
                                //boolean br = Arrays.stream(retain).anyMatch(i -> {
                                //    return (i+"/").startsWith(pathStr+"/");
                                //});

                                boolean br = (tempStr+"/").startsWith(pathStr+"/");

                                if (br) {
                                    list.add(tempStr.substring(pathStr.length()));
                                }
                            }
                            String[] strArr = list.toArray(new String[0]);

                            ObjectNode o = (ObjectNode) node.at("/value");

                            recursionOp(o, strArr);
                            if(o.size() != 0){
                                a.addObject()
                                        .put("op", node.findValue("op").asText())
                                        .put("path", node.path("path").asText())
                                        .set("value", o);
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
            if(retainStr.charAt(0) != '/'){
                return ;
            }
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

        List<String> tempList = p.keySet().stream().map(i -> {
            return i.substring(1);
        }).collect(Collectors.toList());

        obj.retain(tempList);
    }

    public static Column 按策略分摊1(String whichDay, String currIncome, String sumDays,
                                String sumIncome, String shareMethod, String rate) {
        return new UserDefinedAggregateFunction() {

            private static final int IDX_INPUT_WHICHDAY = 0;
            private static final int IDX_INPUT_CURRINCOME = 1;
            private static final int IDX_INPUT_SUMDAYS = 2;
            private static final int IDX_INPUT_SUMINCOME = 3;
            private static final int IDX_INPUT_SHAREMETHOD = 4;
            private static final int IDX_INPUT_RATE = 5;

            private static final int IDX_BUFFER_N1 = 0;
            private static final int IDX_BUFFER_N2 = 1;
            private static final int IDX_BUFFER_DAYS = 2;
            private static final int IDX_BUFFER_PRESUMINCOME = 3;
            private static final int IDX_BUFFER_CURRINCOME = 4;
            private static final int IDX_BUFFER_SUMINCOME = 5;
            private static final int IDX_BUFFER_SUM = 6;
            private static final int IDX_BUFFER_RATE = 7;
            private static final int IDX_BUFFER_TAB = 8;

            @Override
            public StructType inputSchema() {
                return new StructType()
                        .add("whichDay", "int")
                        .add("当天收入", "long")
                        .add("总天数", "int")
                        .add("总收入", "long")
                        .add("分法", "string")
                        .add("比例", "array<struct<k:double,a:bigint>>")
                        ;
            }

            @Override
            public StructType bufferSchema() {

                StructType aType = new StructType()
                        .add("n1", "int")    // 上一天是第几天
                        .add("n2", "int")    // 当天是第几天
                        .add("days", "int")    // 总天数
                        .add("preSumIncome", "long")        // 到上一天累计收入和
                        .add("currIncome", "long")    // 当天收入
                        .add("sumIncome", "long")    // 总收入
                        .add("sum", "long")    // 总金额
                        .add("rate", "array<struct<k:double,a:bigint>>")
                        .add("tab", "int")    // 标志	1:天数 2:金额 3:分成
                        ;

                return new StructType()
                        .add("curr", aType);
            }

            @Override
            public DataType dataType() {
                return ArrayType.apply(new StructType()
                        .add("n", "int")
                        .add("amount", "long"));
            }

            @Override
            public boolean deterministic() {
                return false;
            }

            @Override
            public void initialize(MutableAggregationBuffer buffer) {
                Row row = RowFactory.create(0, 0, 0, 0L, 0L, 0L, 0L, null, 0);
                buffer.update(0, row);
            }

            @Override
            public void update(MutableAggregationBuffer buffer, Row input) {

                Integer whichDay = input.getAs(IDX_INPUT_WHICHDAY);        // 第n天
                Long currIncome = input.getAs(IDX_INPUT_CURRINCOME);    // 当天收入
                currIncome = MoreObjects.firstNonNull(currIncome, 0L);
                Integer sumDays = input.getAs(IDX_INPUT_SUMDAYS);        // 总天数
                Long sumIncome = input.getAs(IDX_INPUT_SUMINCOME);        // 总收入
                String shareMethod = input.getAs(IDX_INPUT_SHAREMETHOD);    // 分法
                WrappedArray<Row> rate = input.getAs(IDX_INPUT_RATE);        // 分成比例 固定总金额也在这里

                Row currType = buffer.getAs(0);
                int n1 = currType.getAs(IDX_BUFFER_N1);    // 上一天是第几天
                int n2 = currType.getAs(IDX_BUFFER_N2);    // 当天是第几天

                long preSumIncome = currType.getAs(IDX_BUFFER_PRESUMINCOME);    // 到上一天累计收入和
                long preDayIncome = currType.getAs(IDX_BUFFER_CURRINCOME);        // 上一天的收入

                // 如果按'天数' 或 '金额'来分摊, 则与分成无关
                long amount = rate.apply(0).getAs(1);
                int tab = 0;
                if (shareMethod.equals("天数")) {
                    tab = 1;
                } else if (shareMethod.equals("金额")) {
                    // 又分为比例分摊和金额分摊
                    if (amount != 0L) {
                        tab = 2;
                    } else {
                        tab = 3;
                    }
                }

                Row next = RowFactory.create(n2, whichDay, sumDays, preSumIncome + preDayIncome, currIncome, sumIncome, amount, rate, tab);
                buffer.update(0, next);
            }

            @Override
            public void merge(MutableAggregationBuffer buffer1, Row buffer2) {

            }

            @Override
            public Object evaluate(Row buffer) {

                Row currType = buffer.getAs(0);

                int n1 = currType.getAs(IDX_BUFFER_N1);         // 上一天
                int n2 = currType.getAs(IDX_BUFFER_N2);         // 当天
                int days = currType.getAs(IDX_BUFFER_DAYS);        // 总天数

                long preSumIncome = currType.getAs(IDX_BUFFER_PRESUMINCOME);    // 到上一天累计收入和
                long currIncome = currType.getAs(IDX_BUFFER_CURRINCOME);    // 当天收入
                long sumIncome = currType.getAs(IDX_BUFFER_SUMINCOME);     // 总收入

                Long sum = currType.getAs(IDX_BUFFER_SUM);        // 总金额
                WrappedArray<Row> rate = currType.getAs(IDX_BUFFER_RATE);    // 分成比例
                int tab = currType.getAs(IDX_BUFFER_TAB);

                List<Row> result = Lists.newArrayList();
                switch (tab) {
                case 1:
                    for (int i = n1; i < n2; i++) {
                        long res1 = fixedAmount(i + 1, days, sum);
                        result.add(RowFactory.create(i, res1));
                    }
                    break;
                case 2:
                    for (int i = n1; i < n2; i++) {
                        long res2 = fixedAmount((int) (preSumIncome), (int) sumIncome, sum);
                        result.add(RowFactory.create(i, res2));
                    }
                    break;
                case 3:
                    // 比例分成
                    for (int i = n1; i < n2; i++) {
                        long res3 = shareSum(preSumIncome, rate);
                        result.add(RowFactory.create(i, res3));
                    }
                    break;
                default:
                    throw new UnsupportedOperationException("未知的分摊方式...");
                }

                return result.toArray(new Row[0]);
            }

            // 固定金额到第n天的总收入
            private long fixedAmount(int day, int days, long sum) {
                if (day < 0) {
                    return 0L;
                }
                return Math.round(1.0 * sum * day / days);
            }

            // 分成部分到第n天的总收入
            private long shareSum(long rateIncome, WrappedArray<Row> rate) {
                double rateSumAmount = 0.0;
                double preK = 0;
                // 专门计算非加液的阶梯分成部分金额
                for (int j = 0; j < rate.size(); j++) {
                    Row row = rate.apply(j);
                    double k = row.getAs(0);
                    long a = row.getAs(1);
                    rateSumAmount += Math.max(0, rateIncome - a) * (k - preK);
                    preK = k;
                }
                return rateSumAmount < 0 ? -Math.round(-rateSumAmount) : Math.round(rateSumAmount);
            }

        }.apply(expr(whichDay), expr(currIncome), expr(sumDays),
                expr(sumIncome), expr(shareMethod), expr(rate));
    }

    public static Column 按策略分摊2(String dAmount, String item) {
        return new UserDefinedAggregateFunction() {

            private static final int IDX_INPUT_AMOUNT = 0;
            private static final int IDX_INPUT_ITEM = 1;

            private static final int IDX_BUFFER_PREVAMOUNT1 = 0;
            private static final int IDX_BUFFER_PREVAMOUNT2 = 1;
            private static final int IDX_BUFFER_CURRAMOUNT1 = 2;
            private static final int IDX_BUFFER_CURRAMOUNT2 = 3;

            @Override
            public StructType inputSchema() {
                return new StructType()
                        .add("dAmount", "long")
                        .add("项", "int")
                        ;
            }

            @Override
            public StructType bufferSchema() {

                return new StructType()
                        .add("prevItemAmount1", "long")    // 上一天累计的项1的amount
                        .add("prevItemAmount2", "long")    // 上一天累计的项2的amount
                        .add("currAmount1", "long")        // 当天累计的项1的amount
                        .add("currAmount2", "long")        // 当天累计的项2的amount
                        ;
            }

            @Override
            public DataType dataType() {
                return LongType$.MODULE$;
            }

            @Override
            public boolean deterministic() {
                return false;
            }

            @Override
            public void initialize(MutableAggregationBuffer buffer) {
                buffer.update(0, 0L);
                buffer.update(1, 0L);
                buffer.update(2, 0L);
                buffer.update(3, 0L);
            }

            @Override
            public void update(MutableAggregationBuffer buffer, Row input) {
                long dAmount = input.getAs(IDX_INPUT_AMOUNT);
                int item = input.getAs(IDX_INPUT_ITEM);

                // 项1是固定, 项2是比例
                long currItemAmount1 = buffer.getAs(IDX_BUFFER_CURRAMOUNT1);
                long currItemAmount2 = buffer.getAs(IDX_BUFFER_CURRAMOUNT2);

                long updateItemAmount1 = currItemAmount1;
                long updateItemAmount2 = currItemAmount2;
                // 用input计算当天的itemAmount1和2
                switch (item) {
                case 1:
                    updateItemAmount1 += dAmount;
                    break;
                case 2:
                    updateItemAmount2 += dAmount;
                    break;
                default:
                    throw new UnsupportedOperationException("未知的项!");
                }

                buffer.update(0, currItemAmount1);
                buffer.update(1, currItemAmount2);
                buffer.update(2, updateItemAmount1);
                buffer.update(3, updateItemAmount2);
            }

            @Override
            public void merge(MutableAggregationBuffer buffer1, Row buffer2) {

            }

            @Override
            public Object evaluate(Row buffer) {

                long prevItemAmount1 = buffer.getAs(IDX_BUFFER_PREVAMOUNT1);
                long prevItemAmount2 = buffer.getAs(IDX_BUFFER_PREVAMOUNT2);
                long currItemAmount1 = buffer.getAs(IDX_BUFFER_CURRAMOUNT1);
                long currItemAmount2 = buffer.getAs(IDX_BUFFER_CURRAMOUNT2);

                return Math.min(currItemAmount1, currItemAmount2) - Math.min(prevItemAmount1, prevItemAmount2);
            }

        }.apply(expr(dAmount), expr(item));
    }

}

