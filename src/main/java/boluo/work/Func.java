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
                            // value???????????????

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
                        throw new UnsupportedOperationException("???add, remove, replace??????");

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

    public static Column ???????????????1(String whichDay, String currIncome, String sumDays,
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
                        .add("????????????", "long")
                        .add("?????????", "int")
                        .add("?????????", "long")
                        .add("??????", "string")
                        .add("??????", "array<struct<k:double,a:bigint>>")
                        ;
            }

            @Override
            public StructType bufferSchema() {

                StructType aType = new StructType()
                        .add("n1", "int")    // ?????????????????????
                        .add("n2", "int")    // ??????????????????
                        .add("days", "int")    // ?????????
                        .add("preSumIncome", "long")        // ???????????????????????????
                        .add("currIncome", "long")    // ????????????
                        .add("sumIncome", "long")    // ?????????
                        .add("sum", "long")    // ?????????
                        .add("rate", "array<struct<k:double,a:bigint>>")
                        .add("tab", "int")    // ??????	1:?????? 2:?????? 3:??????
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

                Integer whichDay = input.getAs(IDX_INPUT_WHICHDAY);        // ???n???
                Long currIncome = input.getAs(IDX_INPUT_CURRINCOME);    // ????????????
                currIncome = MoreObjects.firstNonNull(currIncome, 0L);
                Integer sumDays = input.getAs(IDX_INPUT_SUMDAYS);        // ?????????
                Long sumIncome = input.getAs(IDX_INPUT_SUMINCOME);        // ?????????
                String shareMethod = input.getAs(IDX_INPUT_SHAREMETHOD);    // ??????
                WrappedArray<Row> rate = input.getAs(IDX_INPUT_RATE);        // ???????????? ???????????????????????????

                Row currType = buffer.getAs(0);
                int n1 = currType.getAs(IDX_BUFFER_N1);    // ?????????????????????
                int n2 = currType.getAs(IDX_BUFFER_N2);    // ??????????????????

                long preSumIncome = currType.getAs(IDX_BUFFER_PRESUMINCOME);    // ???????????????????????????
                long preDayIncome = currType.getAs(IDX_BUFFER_CURRINCOME);        // ??????????????????

                // ?????????'??????' ??? '??????'?????????, ??????????????????
                long amount = rate.apply(0).getAs(1);
                int tab = 0;
                if (shareMethod.equals("??????")) {
                    tab = 1;
                } else if (shareMethod.equals("??????")) {
                    // ????????????????????????????????????
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

                int n1 = currType.getAs(IDX_BUFFER_N1);         // ?????????
                int n2 = currType.getAs(IDX_BUFFER_N2);         // ??????
                int days = currType.getAs(IDX_BUFFER_DAYS);        // ?????????

                long preSumIncome = currType.getAs(IDX_BUFFER_PRESUMINCOME);    // ???????????????????????????
                long currIncome = currType.getAs(IDX_BUFFER_CURRINCOME);    // ????????????
                long sumIncome = currType.getAs(IDX_BUFFER_SUMINCOME);     // ?????????

                Long sum = currType.getAs(IDX_BUFFER_SUM);        // ?????????
                WrappedArray<Row> rate = currType.getAs(IDX_BUFFER_RATE);    // ????????????
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
                    // ????????????
                    for (int i = n1; i < n2; i++) {
                        long res3 = shareSum(preSumIncome, rate);
                        result.add(RowFactory.create(i, res3));
                    }
                    break;
                default:
                    throw new UnsupportedOperationException("?????????????????????...");
                }

                return result.toArray(new Row[0]);
            }

            // ??????????????????n???????????????
            private long fixedAmount(int day, int days, long sum) {
                if (day < 0) {
                    return 0L;
                }
                return Math.round(1.0 * sum * day / days);
            }

            // ??????????????????n???????????????
            private long shareSum(long rateIncome, WrappedArray<Row> rate) {
                double rateSumAmount = 0.0;
                double preK = 0;
                // ????????????????????????????????????????????????
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

    public static Column ???????????????2(String dAmount, String item) {
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
                        .add("???", "int")
                        ;
            }

            @Override
            public StructType bufferSchema() {

                return new StructType()
                        .add("prevItemAmount1", "long")    // ?????????????????????1???amount
                        .add("prevItemAmount2", "long")    // ?????????????????????2???amount
                        .add("currAmount1", "long")        // ??????????????????1???amount
                        .add("currAmount2", "long")        // ??????????????????2???amount
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

                // ???1?????????, ???2?????????
                long currItemAmount1 = buffer.getAs(IDX_BUFFER_CURRAMOUNT1);
                long currItemAmount2 = buffer.getAs(IDX_BUFFER_CURRAMOUNT2);

                long updateItemAmount1 = currItemAmount1;
                long updateItemAmount2 = currItemAmount2;
                // ???input???????????????itemAmount1???2
                switch (item) {
                case 1:
                    updateItemAmount1 += dAmount;
                    break;
                case 2:
                    updateItemAmount2 += dAmount;
                    break;
                default:
                    throw new UnsupportedOperationException("????????????!");
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

