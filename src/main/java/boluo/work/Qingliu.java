package boluo.work;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.StringType$;
import scala.collection.JavaConverters;
import boluo.work.My;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;


import static boluo.work.My.patchFilter;
import static boluo.work.My.json2patch;
import static org.apache.spark.sql.functions.*;

public class Qingliu {

    static LocalDateTime staticTs = LocalDateTime.of(2016, 1, 1, 0, 0);

    public static Dataset<Row> load(Dataset<Row> df) {
        //管理费
        Dataset<Row> r1 = load(df.where("app_id='012e234a'"), "Q")
                .withColumn("patch", patchFilter("patch", "/项目编号", "/本次付款金额",
                        "/打款日期", "/收款户名", "/付款周期-起始日", "/付款周期-截止日", "/编号", "/收款账户", "/费用类别", "/计算规则"))
                .selectExpr("id row_key", "*", "'qingliu.order' format");

        //居间费申请
        Dataset<Row> r2 = load(df.where("app_id='f754d22e'"), "W")
                .withColumn("patch", patchFilter("patch", "/项目编号", "/本次付款金额",
                        "/打款日期", "/收款户名", "/付款周期-起始日", "/付款周期-截止日", "/编号", "/收款账户", "/费用类别", "/计算规则"))
                .selectExpr("id row_key", "*", "'qingliu.order' format");

        //项目运营体统一规范
        Dataset<Row> r3 = load(df.where("app_id='907be88e'"), "E")
                .withColumn("patch", patchFilter("patch", "/门店编号", "/门店名称",
                        "/海尔门店名称", "/美的门店名称", "/运营体编号", "/运营体名称", "/兰德力项目编号", "/海尔项目编号",
                        "/美的项目编号", "/编号", "/兰德力门店编号"))
                .selectExpr("id row_key", "*", "'qingliu.storeBiz' format");

        //不分摊费用
        Dataset<Row> r4 = load(df.where("app_id='90cd9cf7'"), "R")
                .withColumn("patch", patchFilter("patch", "/编号", "/审批编号", "/完成时间",
                        "/费用类别", "/支付对象", "/银行账户", "/付款金额", "/项目编号"))
                .selectExpr("id row_key", "*", "'qingliu.noAvgOrder' format");

        //水电费
        Dataset<Row> r5 = load(df.where("app_id='02a2b322'"), "T")
                .withColumn("patch", patchFilter("patch", "/打款日期", "/收款户名", "/收款账户",
                        "/付款金额", "/编号", "/费用明细说明", "/费用起始日", "/费用截止日", "/付款明细", "/费用类别"))
                .selectExpr("id row_key", "*", "'qingliu.utilities' format");

        //施工合同
        Dataset<Row> r6 = load(df.where("app_id='af379434'"), "Y")
                .withColumn("patch", patchFilter("patch", "/签订日期", "/打款户名", "/开户银行",
                        "/合同金额", "/文件名称", "/项目编号", "/编号", "/项目备注"))
                .selectExpr("id row_key", "*", "'qingliu.sgContract' format");

        //合同补充协议
        Dataset<Row> r7 = load(df.where("app_id='642fe59e'"), "U")
                .withColumn("patch", patchFilter("patch", "/合同签订日期", "/打款户名", "/开户银行",
                        "/增减项金额", "/合同文件名称", "/项目编号", "/备注：", "/编号"))
                .selectExpr("id row_key", "*", "'qingliu.contractBc' format");

        //设计合同
        Dataset<Row> r8 = load(df.where("app_id='7e7e7a69'"), "I")
                .withColumn("patch", patchFilter("patch", "/签订日期", "/打款户名", "/开户银行",
                        "/合同金额", "/文件名称", "/项目编号", "/合同备注", "/编号"))
                .selectExpr("id row_key", "*", "'qingliu.sjContract' format");

        //海尔设备编号及识别码
        Dataset<Row> r9 = load(df.where("app_id='7819950e'"), "P")
                .withColumn("patch", patchFilter("patch", "/设备识别码", "/机器型号"))
                .selectExpr("id row_key", "*", "'qingliu.hrDeviceCode' format");

        //设备类型成本表
        Dataset<Row> r10 = load(df.where("app_id='6e4bb0ea'"), "O")
                .withColumn("patch", patchFilter("patch", "/型号", "/设备成本", "/品牌", "/设备总成本"))
                .selectExpr("id row_key", "*", "'qingliu.deviceCost' format");

        //保洁费
        Dataset<Row> r11 = load(df.where("app_id='84cc0e3f'"), "A")
                .withColumn("patch", patchFilter("patch", "/打款日期", "/收款户名", "/收款账户",
                        "/编号", "/费用类别", "/项目明细"))
                .selectExpr("id row_key", "*", "'qingliu.cleanFee' format");

        //物料领用
        Dataset<Row> r12 = load(df.where("app_id='0decccb2'"), "S")
                .withColumn("patch", patchFilter("patch", "/编号", "/领用单号", "/项目编号", "/领用金额",
                        "/期望交付日期", "/表格", "/门店编号", "/领用明细（备库）", "/领用明细（临时）", "/领用事由", "/audit_name"))
                .selectExpr("id row_key", "*", "'qingliu.materielReceive' format");

        //服务合同
        Dataset<Row> r13 = load(df.where("app_id='66ca5c3f'"), "D")
                .withColumn("patch", patchFilter("patch", "/编号", "/费用核算起始日期", "/文件名称",
                        "/固定年租金/年管理费", "/加液营收参与分成比例", "/加液协议分成比例", "/分成费（比例）", "/费用核算起始日期",
                        "/合同截止日期", "/项目编号", "/管理费备注", "/管理费结算方式", "/项目年管理费", "/门店编号"))
                .selectExpr("id row_key", "*", "'qingliu.serviceContract' format");

        //付款申请审批
        Dataset<Row> r14 = load(df.where("app_id='af9570eb'"), "F")
                .withColumn("patch", patchFilter("patch", "/编号", "/所在部门", "/打款日期", "/收款户名",
                        "/收款账号", "/总付款金额", "/付款明细"))
                .selectExpr("id row_key", "*", "'qingliu.fukuan' format");

        //日常报销审批
        Dataset<Row> r15 = load(df.where("app_id='bd15d028'"), "G")
                .withColumn("patch", patchFilter("patch", "/编号", "/所在部门", "/打款日期", "/总报销金额",
                        "/报销明细"))
                .selectExpr("id row_key", "*", "'qingliu.baoxiao' format");

        return Stream.of(r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15).reduce(Dataset::unionAll).get();
    }

    public static Row[] storeBiz_store_data(JsonNode curr, String rev) {

        if (curr.size() == 0) {
            return null;
        }

        Timestamp ts = Timestamp.valueOf(staticTs);
        long tempTime = ts.getTime() / 1000;
        String tempRev = String.format("%010d", tempTime) + rev.substring(10);

        String storeId = curr.at("/门店编号").asText();
        String storeName = curr.at("/门店名称").asText();
        String bizId = curr.at("/运营体编号").asText();
        Row store = RowFactory.create(storeId, storeName, null);

        String haierStoreId2 = curr.at("/海尔门店名称").asText();
        String meidiStoreId2 = curr.at("/美的门店名称").asText();
        String landeliStoreId2 = curr.at("/兰德力门店编号").asText();

        Map<String, Row> storeMap = Maps.newTreeMap();

        if (!Strings.isNullOrEmpty(haierStoreId2)) {
            // 海尔美的可能出现多行的情况
            String[] haierStoreId2Nums = haierStoreId2.split("[\\r\\n]+");
            for (String haierStore : haierStoreId2Nums) {
                storeMap.put(haierStore, RowFactory.create(rev, tempRev, ts, haierStore, store, bizId));
            }
        }
        if (!Strings.isNullOrEmpty(meidiStoreId2)) {
            String[] meidiStoreId2Nums = meidiStoreId2.split("[\\r\\n]+");
            for (String meidiStore : meidiStoreId2Nums) {
                storeMap.put(meidiStore, RowFactory.create(rev, tempRev, ts, meidiStore, store, bizId));
            }
        }
        if (!Strings.isNullOrEmpty(landeliStoreId2)) {
            storeMap.put(landeliStoreId2, RowFactory.create(rev, tempRev, ts, landeliStoreId2, store, bizId));
        }

        return modifyRev(storeMap.values());
    }

    public static Row[] storeBiz_biz_data(JsonNode curr, String rev) {

        if (curr.size() == 0) {
            return null;
        }

        Timestamp ts = Timestamp.valueOf(staticTs);
        long tempTime = ts.getTime() / 1000;
        String tempRev = String.format("%010d", tempTime) + rev.substring(10);
        String bizId = curr.at("/运营体编号").asText();
        String bizName = curr.at("/运营体名称").asText();
        Row biz = RowFactory.create(bizId, bizName, null, null, null, null);

        String haierBizId2 = curr.at("/海尔项目编号").asText();
        String meidiBizId2 = curr.at("/美的项目编号").asText();
        String landeliBizId2 = curr.at("/兰德力项目编号").asText();

        Map<String, Row> bizMap = Maps.newHashMap();
        if (!Strings.isNullOrEmpty(haierBizId2)) {
            bizMap.put(haierBizId2, RowFactory.create(rev, tempRev, ts, haierBizId2, biz));
        }
        if (!Strings.isNullOrEmpty(meidiBizId2)) {
            bizMap.put(meidiBizId2, RowFactory.create(rev, tempRev, ts, meidiBizId2, biz));
        }
        if (!Strings.isNullOrEmpty(landeliBizId2)) {
            bizMap.put(landeliBizId2, RowFactory.create(rev, tempRev, ts, landeliBizId2, biz));
        }

        return modifyRev(bizMap.values());
    }

    public static Row[] order_expend_data(JsonNode curr, String rev) {

        if (curr.size() == 0) {
            return null;
        }

        Timestamp ts;
        Date startDate;
        Date endDate;
        String tempRev;

        String startDateStr = curr.at("/付款周期-起始日").asText();
        String endDateStr = curr.at("/付款周期-截止日").asText();

        if (Strings.isNullOrEmpty(startDateStr)) {
            return null;
        }

        ts = Timestamp.valueOf(LocalDate.parse(startDateStr).atTime(9, 0));
        long tempTime = ts.getTime() / 1000;
        tempRev = String.format("%010d", tempTime) + rev.substring(10);

        startDate = Date.valueOf(startDateStr);
        endDate = Date.valueOf(endDateStr);
        endDate = Date.valueOf(endDate.toLocalDate().plusDays(1));

        String biz = curr.at("/项目编号").asText();
        String payee = curr.at("/收款户名").asText();
        String no = curr.at("/编号").asText();
        String ca = curr.at("/收款账户").asText();

        String costCategory = curr.at("/费用类别").asText();
        String subject = costCategory.replaceAll("^\\d+\\.\\d+([^\\-]+)\\-(.*)", "$1.$2");

        long paymentAmount = Math.round(curr.at("/本次付款金额").asDouble() * 100);

        String rule = curr.at("/计算规则").asText();
        long amount = takeAmount(rule, paymentAmount);
        if (amount == 0L) {
            return null;
        }
        amount = -amount;

        Row ord = RowFactory.create(no, null, startDate, endDate);
        Row payment = RowFactory.create(ts, subject, payee, ca, amount, null);
        return new Row[]{
                RowFactory.create(
                        rev, tempRev, ts,
                        new Row[]{payment}, ord,
                        null, biz
                )
        };
    }

    public static Row[] noAvgOrder_cost_data(JsonNode curr, String rev) {

        if (curr.size() == 0) {
            return null;
        }

        Timestamp ts = Timestamp.valueOf(curr.at("/完成时间").asText());
        long tempTime = ts.getTime() / 1000;
        String tempRev = String.format("%010d", tempTime) + rev.substring(10);

        String biz = curr.at("/项目编号").asText();
        String payee = curr.at("/支付对象").asText();
        String no = curr.at("/审批编号").asText();
        String ca = curr.at("/银行账户").asText();

        String costCategory = curr.at("/费用类别").asText();
        String subject = costCategory.replaceAll("^\\d+\\.\\d+([^\\-]+)\\-(.*)", "$1.$2");

        long amount = -Math.round(curr.at("/付款金额").asDouble() * 100);
        if (amount == 0) {
            return null;
        }

        Row cost = RowFactory.create(no, null);
        Row payment = RowFactory.create(ts, subject, payee, ca, amount, null);
        return new Row[]{
                RowFactory.create(
                        rev, tempRev, ts,
                        new Row[]{payment}, cost,
                        null, biz
                )
        };
    }

    //水电费
    public static Row[] utilities_expend_data(JsonNode curr, String rev) {

        if (curr.size() == 0) {
            return null;
        }

        String timeStr = curr.at("/打款日期").asText();
        if (Strings.isNullOrEmpty(timeStr)) {
            return null;
        }

        Timestamp ts = Timestamp.valueOf(LocalDate.parse(timeStr).atTime(9, 0));
        long tempTime = ts.getTime() / 1000;
        String tempRev = String.format("%010d", tempTime) + rev.substring(10);

        String payee = curr.at("/收款户名").asText();
        String no = curr.at("/编号").asText();
        String ca = curr.at("/收款账户").asText();
        String costCategory = curr.at("/费用类别").asText();
        String subject = costCategory.replaceAll("^\\d+\\.\\d+([^\\-]+)\\-(.*)", "$1.$2");

        List<Row> result = Lists.newArrayList();
        for (JsonNode i : curr.at("/付款明细")) {

            Date startDateTime = Date.valueOf(i.at("/费用起始日").asText());
            Date endDateTime = Date.valueOf(Date.valueOf(i.at("/费用截止日").asText()).toLocalDate().plusDays(1));

            long amount = -Math.round(i.at("/付款金额").asDouble() * 100);
            if (amount == 0L) {
                continue;
            }

            String storeId2 = i.at("/门店编号").asText();

            Row ord = RowFactory.create(no, null, startDateTime, endDateTime);
            Row payment = RowFactory.create(ts, subject, payee, ca, amount, null);

            result.add(RowFactory.create(
                    rev, tempRev, ts,
                    new Row[]{payment}, ord,
                    storeId2, null
            ));
        }

        return modifyRev(result);
    }

    public static Row[] sgContract_expend_data(JsonNode curr, String rev) {

        if (curr.size() == 0) {
            return null;
        }

        String timeStr = curr.at("/签订日期").asText();
        if (Strings.isNullOrEmpty(timeStr)) {
            return null;
        }

        Timestamp ts = Timestamp.valueOf(LocalDate.parse(timeStr).atTime(9, 0));
        long tempTime = ts.getTime() / 1000;
        String tempRev = String.format("%010d", tempTime) + rev.substring(10);

        LocalDate startTime = Instant.ofEpochSecond(tempTime).atZone(ZoneOffset.ofHours(8)).toLocalDate();
        LocalDate endTime = startTime.plusYears(5);

        Date startDateTime = Date.valueOf(startTime);
        Date endDateTime = Date.valueOf(endTime);

        String no1 = curr.at("/编号").asText();
        String biz = curr.at("/项目编号").asText();
        String payee = curr.at("/打款户名").asText();
        String no = curr.at("/文件名称").asText();
        String ca = curr.at("/开户银行").asText();
        String subject = "主营业务成本.装修费";
        String bizRemarks = curr.at("/项目备注").asText();
        if (biz.contains("/") && Strings.isNullOrEmpty(bizRemarks)) {
            throw new UnsupportedOperationException("项目有多条但备注为空！！编号为：" + no1);
        }

        Row ord = RowFactory.create(no, null, startDateTime, endDateTime);

        List<Row> result = Lists.newArrayList();

        long allAmount = -Math.round(curr.at("/合同金额").asDouble() * 100);
        if (allAmount == 0L) {
            return null;
        }

        if (!biz.contains("/")) {
            Row payment = RowFactory.create(ts, subject, payee, ca, allAmount, null);
            result.add(RowFactory.create(
                    rev, tempRev, ts,
                    new Row[]{payment}, ord,
                    null, biz
            ));
        } else {
            Map<String, Long> bizMap = Maps.newHashMap();
            String[] bizRemarkArr = bizRemarks.split("\n");
            for (String bizLine : bizRemarkArr) {
                if (bizLine.startsWith("项目编号：")) {
                    String tempBizNo = bizLine.substring(bizLine.indexOf("项目编号：") + 5, bizLine.indexOf("合同金额：")).trim();
                    String tempAmount = bizLine.substring(bizLine.indexOf("合同金额：") + 5).trim();
                    long amount = -Math.round(Double.parseDouble(tempAmount) * 100);
                    bizMap.put(tempBizNo, amount);
                }
            }
            Preconditions.checkArgument(bizMap.values().stream().mapToLong(i -> i).sum() == allAmount,
                    "金额不等！！编号为: " + no1);
            for (String bizKey : bizMap.keySet()) {
                Row payment = RowFactory.create(ts, subject, payee, ca, bizMap.get(bizKey), null);
                result.add(RowFactory.create(
                        rev, tempRev, ts,
                        new Row[]{payment}, ord,
                        null, bizKey
                ));
            }
        }

        return modifyRev(result);
    }

    //合同补充协议
    public static Row[] contractBc_expend_data(JsonNode curr, String rev) {

        if (curr.size() == 0) {
            return null;
        }

        String timeStr = curr.at("/合同签订日期").asText();

        if (Strings.isNullOrEmpty(timeStr)) {
            return null;
        }

        Timestamp ts = Timestamp.valueOf(LocalDate.parse(timeStr).atTime(9, 0));
        long tempTime = ts.getTime() / 1000;
        String tempRev = String.format("%010d", tempTime) + rev.substring(10);

        LocalDate startTime = Instant.ofEpochSecond(tempTime).atZone(ZoneOffset.ofHours(8)).toLocalDate();
        LocalDate endTime = startTime.plusYears(5);

        Date startDateTime = Date.valueOf(startTime);
        Date endDateTime = Date.valueOf(endTime);

        String no1 = curr.at("/编号").asText();
        String biz = curr.at("/项目编号").asText();
        String payee = curr.at("/打款户名").asText();
        String no = curr.at("/合同文件名称").asText();
        String ca = curr.at("/开户银行").asText();
        String subject = "主营业务成本.装修费";
        String bizRemarks = curr.at("/备注：").asText();
        if (biz.contains("/") && Strings.isNullOrEmpty(bizRemarks)) {
            throw new UnsupportedOperationException("项目有多条但备注为空！！编号为：" + no1);
        }

        long allAmount = -Math.round(curr.at("/增减项金额").asDouble() * 100);
        if (allAmount == 0L) {
            return null;
        }

        List<Row> result = Lists.newArrayList();
        Row ord = RowFactory.create(no, null, startDateTime, endDateTime);
        if (!biz.contains("/")) {
            Row payment = RowFactory.create(ts, subject, payee, ca, allAmount, null);
            result.add(RowFactory.create(
                    rev, tempRev, ts,
                    new Row[]{payment}, ord,
                    null, biz
            ));
        } else {
            Map<String, Long> bizMap = Maps.newHashMap();
            String[] bizRemarkArr = bizRemarks.split("\n");
            for (String bizLine : bizRemarkArr) {
                if (bizLine.startsWith("项目编号：")) {
                    String tempBizNo = bizLine.substring(bizLine.indexOf("项目编号：") + 5, bizLine.indexOf("合同金额：")).trim();
                    String tempAmount = bizLine.substring(bizLine.indexOf("合同金额：") + 5).trim();
                    long amount = -Math.round(Double.parseDouble(tempAmount) * 100);
                    bizMap.put(tempBizNo, amount);
                }
            }
            Preconditions.checkArgument(bizMap.values().stream().mapToLong(i -> i).sum() == allAmount,
                    "金额不等！！编号为: " + no1);
            for (String bizKey : bizMap.keySet()) {
                Row payment = RowFactory.create(ts, subject, payee, ca, bizMap.get(bizKey), null);
                result.add(RowFactory.create(
                        rev, tempRev, ts,
                        new Row[]{payment}, ord,
                        null, bizKey
                ));
            }
        }

        return modifyRev(result);
    }

    //设计合同
    public static Row[] sjContract_expend_data(JsonNode curr, String rev) {

        if (curr.size() == 0) {
            return null;
        }

        String timeStr = curr.at("/签订日期").asText();
        if (Strings.isNullOrEmpty(timeStr)) {
            return null;
        }

        Timestamp ts = Timestamp.valueOf(LocalDate.parse(timeStr).atTime(9, 0));
        long tempTime = ts.getTime() / 1000;
        String tempRev = String.format("%010d", tempTime) + rev.substring(10);

        LocalDate startTime = Instant.ofEpochSecond(tempTime).atZone(ZoneOffset.ofHours(8)).toLocalDate();
        LocalDate endTime = startTime.plusYears(5);

        Date startDateTime = Date.valueOf(startTime);
        Date endDateTime = Date.valueOf(endTime);

        String no1 = curr.at("/编号").asText();
        String biz = curr.at("/项目编号").asText();
        String payee = curr.at("/打款户名").asText();
        String no = curr.at("/文件名称").asText();
        String ca = curr.at("/开户银行").asText();
        String subject = "主营业务成本.装修费（空间设计费）";
        String bizRemarks = curr.at("/合同备注").asText();
        if (biz.contains("/") && Strings.isNullOrEmpty(bizRemarks)) {
            throw new UnsupportedOperationException("项目有多条但备注为空！！编号为：" + no1);
        }

        long allAmount = -Math.round(curr.at("/合同金额").asDouble() * 100);
        if (allAmount == 0L) {
            return null;
        }

        List<Row> result = Lists.newArrayList();
        Row ord = RowFactory.create(no, null, startDateTime, endDateTime);
        if (!biz.contains("/")) {
            Row payment = RowFactory.create(ts, subject, payee, ca, allAmount, null);
            result.add(RowFactory.create(
                    rev, tempRev, ts,
                    new Row[]{payment}, ord,
                    null, biz
            ));
        } else {
            Map<String, Long> bizMap = Maps.newHashMap();
            String[] bizRemarkArr = bizRemarks.split("\n");
            for (String bizLine : bizRemarkArr) {
                if (bizLine.startsWith("项目编号：")) {
                    String tempBizNo = bizLine.substring(bizLine.indexOf("项目编号：") + 5, bizLine.indexOf("合同金额：")).trim();
                    String tempAmount = bizLine.substring(bizLine.indexOf("合同金额：") + 5).trim();
                    long amount = -Math.round(Double.parseDouble(tempAmount) * 100);
                    bizMap.put(tempBizNo, amount);
                }
            }
            Preconditions.checkArgument(bizMap.values().stream().mapToLong(i -> i).sum() == allAmount,
                    "金额不等！！编号为: " + no1);
            for (String bizKey : bizMap.keySet()) {
                Row payment = RowFactory.create(ts, subject, payee, ca, bizMap.get(bizKey), null);
                result.add(RowFactory.create(
                        rev, tempRev, ts,
                        new Row[]{payment}, ord,
                        null, bizKey
                ));
            }
        }

        return modifyRev(result);
    }

    public static Row[] hrDeviceCode_device_data(JsonNode curr, String rev) {

        if (curr.size() == 0) {
            return null;
        }

        Timestamp ts = Timestamp.valueOf(staticTs);
        long tempTime = ts.getTime() / 1000;
        String tempRev = String.format("%010d", tempTime) + rev.substring(10);

        String devId = curr.at("/设备识别码").asText();
        String devCode = curr.at("/机器型号").asText();

        Row device = RowFactory.create(devId, devCode, null, null);
        return new Row[]{
                RowFactory.create(
                        rev, tempRev, ts,
                        device,
                        null, null
                )
        };
    }

    public static Row[] deviceCost_model_data(JsonNode curr, String rev) {

        if (curr.size() == 0) {
            return null;
        }

        Timestamp ts = Timestamp.valueOf(staticTs);
        long tempTime = ts.getTime() / 1000;
        String tempRev = String.format("%010d", tempTime) + rev.substring(10);

        String deviceCode = curr.at("/型号").asText();
        String brand = curr.at("/品牌").asText();
        long TempCost = -Math.round(curr.at("/设备总成本").asDouble() * 100);

        long cost;
        if (brand.equals("LG")) {
            cost = TempCost / 2920L;        //8年的天数
        } else {
            cost = TempCost / 1825L;        //5年的天数
        }

        Row model = RowFactory.create(deviceCode, cost);

        return new Row[]{
                RowFactory.create(
                        rev, tempRev, ts,
                        model
                )
        };
    }

    // 保洁费
    public static Row[] cleanFee_expend_data(JsonNode curr, String rev) {

        if (curr.size() == 0) {
            return null;
        }

        String timeStr = curr.at("/打款日期").asText();
        if (Strings.isNullOrEmpty(timeStr)) {
            return null;
        }

        Timestamp ts = Timestamp.valueOf(LocalDate.parse(timeStr).atTime(9, 0));
        long tempTime = ts.getTime() / 1000;
        String tempRev = String.format("%010d", tempTime) + rev.substring(10);

        String no = curr.at("/编号").asText();
        String payee = curr.at("/收款户名").asText();
        String ca = curr.at("/收款账户").asText();
        String costCategory = curr.at("/费用类别").asText();
        String subject = costCategory.replaceAll("^\\d+\\.\\d+([^\\-]+)\\-(.*)", "$1.$2");

        List<Row> result = Lists.newArrayList();
        for (JsonNode i : curr.at("/项目明细")) {

            Date startDateTime = Date.valueOf(i.at("/付款起始日").asText());
            Date endDateTime = Date.valueOf(Date.valueOf(i.at("/付款截止日").asText()).toLocalDate().plusDays(1));

            long amount = -Math.round(i.at("/付款金额").asDouble() * 100);
            if (amount == 0L) {
                continue;
            }

            String storeNo = i.at("/门店编号").asText();
            String bizNo = i.at("/项目编号").asText();

            Row expend = RowFactory.create(no, null, startDateTime, endDateTime);

            if (!storeNo.equals("0") && !storeNo.equals("")) {

                String[] storeNoArrays = storeNo.split("/");
                Iterator<String> it = Arrays.asList(storeNo.split("/")).iterator();
                for (Long a : shareAmount(amount, storeNoArrays.length)) {
                    Row payment = RowFactory.create(ts, subject, payee, ca, a, null);
                    result.add(RowFactory.create(
                            rev, tempRev, ts,
                            new Row[]{payment}, expend,
                            it.next(), null
                    ));
                }

            } else {

                String[] bizNoArrays = bizNo.split("/");
                Iterator<String> it = Arrays.asList(bizNo.split("/")).iterator();
                for (Long a : shareAmount(amount, bizNoArrays.length)) {
                    Row payment = RowFactory.create(ts, subject, payee, ca, a, null);
                    result.add(RowFactory.create(
                            rev, tempRev, ts,
                            new Row[]{payment}, expend,
                            null, it.next()
                    ));
                }
            }
        }

        return modifyRev(result);
    }

    // 物料领用
    public static Row[] materielReceive_expend_data(JsonNode curr, String rev) {

        if (curr.size() == 0) {
            return null;
        }

        String auditName = curr.at("/audit_name").asText();
        if (Strings.isNullOrEmpty(auditName)) {
            return null;
        }

        Long time = Long.parseLong(rev.substring(0, 10) + "000");
        Instant instant = Instant.ofEpochMilli(time);
        ZoneId zoneId = ZoneId.systemDefault();
        LocalDateTime tempLocalDateTime = LocalDateTime.ofInstant(instant, zoneId);

        Timestamp ts = Timestamp.valueOf(tempLocalDateTime);
        long tempTime = ts.getTime() / 1000;
        String tempRev = String.format("%010d", tempTime) + rev.substring(10);

        String no1 = curr.at("/编号").asText();
        String no = curr.at("/领用单号").asText();
        String bizId = curr.at("/项目编号").asText();
        String subject = "销售费用.低值易耗品（门店运营耗材）";
        JsonNode tempNode = curr.at("/领用明细（临时）");
        JsonNode libraryNode = curr.at("/领用明细（备库）");
        long amount = Math.round(curr.at("/领用金额").asDouble() * 100);

        String startDateStr = curr.at("/期望交付日期").asText();
        if (Strings.isNullOrEmpty(startDateStr)) {
            throw new UnsupportedOperationException("期望交付日期为空!");
        }

        Date startDateTime = Date.valueOf(startDateStr);
        Date endDateTime = Date.valueOf(startDateTime.toLocalDate().plusDays(1));

        List<Row> result = Lists.newArrayList();
        Row expend = RowFactory.create(no, null, startDateTime, endDateTime);

        Long removeAmount = 0L;
        Long checkAmount = 0L;
        for (JsonNode node : libraryNode) {
            String materielName = node.at("/物料名称").asText();
            String materialCode = node.at("/物料编码").asText();
            Long lineAmount = Math.round(node.at("/小计").asDouble() * 100);

            boolean b1 = (materielName.contains("洗衣机") || materielName.contains("烘干机") || materielName.contains("干衣机"))
                    && materialCode.startsWith("0301");
            boolean b2 = (materielName.contains("洗鞋机") || materielName.contains("安卓屏"))
                    && materialCode.startsWith("0302");

            // 如果是需要排除的数据
            if (b1 || b2) {
                removeAmount += lineAmount;
            }
            checkAmount += lineAmount;
        }

        for (JsonNode tempAmount : tempNode) {
            Long lineAmount = Math.round(tempAmount.at("/小计").asDouble() * 100);
            checkAmount += lineAmount;
        }

//        Preconditions.checkArgument(checkAmount == amount,
//                "金额不等!! 编号为: " + no1);

        if (checkAmount != amount) {
            System.out.println("金额不等!! 编号为: " + no1);
        }

        Long afterAmount = checkAmount - removeAmount;
        if (afterAmount == 0L) {
            return null;
        }

        if (curr.at("/表格").size() != 0) {   // 如果表格中有数据, 按表格中门店分摊金额

            List<String> storeIdList = Lists.newArrayList();
            for (JsonNode node : curr.at("/表格")) {
                storeIdList.add(node.at("/门店编号").asText());
            }

            Iterator<String> it = storeIdList.iterator();
            for (Long a : shareAmount(afterAmount, storeIdList.size())) {
                Row payment = RowFactory.create(ts, subject, null, null, -a, null);
                result.add(RowFactory.create(
                        rev, tempRev, ts,
                        new Row[]{payment}, expend,
                        it.next(), null
                ));
            }

        } else if (!Strings.isNullOrEmpty(bizId)) {     // 表格中无数据,无门店, 但是bizId中有数据, 按照bizId拆分

            if (!bizId.contains("/")) {
                Row payment = RowFactory.create(ts, subject, null, null, -afterAmount, null);
                result.add(RowFactory.create(
                        rev, tempRev, ts,
                        new Row[]{payment}, expend,
                        null, bizId
                ));
            } else {
                Map<String, Long> bizMap = Maps.newHashMap();
                String[] bizArr = curr.at("/领用事由").asText().split("\n");
                for (String bizLine : bizArr) {
                    if (bizLine.startsWith("项目编号：")) {
                        String tempBizId = bizLine.substring(bizLine.indexOf("项目编号：") + 5, bizLine.indexOf("领用金额：")).trim();
                        String tempAmountStr = bizLine.substring(bizLine.indexOf("领用金额：") + 5).trim();
                        long tempAmount = Math.round(Double.parseDouble(tempAmountStr) * 100);
                        bizMap.put(tempBizId, tempAmount);
                    }
                }
                Preconditions.checkArgument(bizMap.values().stream().mapToLong(i -> i).sum() == afterAmount,
                        "金额不等!!编号为:" + no1);
                for (String bizKey : bizMap.keySet()) {
                    Row payment = RowFactory.create(ts, subject, null, null, (-1) * bizMap.get(bizKey), null);
                    result.add(RowFactory.create(
                            rev, tempRev, ts,
                            new Row[]{payment}, expend,
                            null, bizKey
                    ));
                }
            }

        } else {
            Row payment = RowFactory.create(ts, subject, null, null, -afterAmount, null);
            result.add(RowFactory.create(
                    rev, tempRev, ts,
                    new Row[]{payment}, expend,
                    null, null
            ));
        }

        return modifyRev(result);
    }

    // 服务合同
    public static Row[] serviceContract_share_data(JsonNode curr, String rev) {

        if (curr.size() == 0) {
            return null;
        }

        String no1 = curr.at("/编号").asText();
        String fileName = curr.at("/文件名称").asText();
        String fixedAmountStr = curr.at("/固定年租金/年管理费").asText();
        String annualFeeStr = curr.at("/项目年管理费").asText();
        long fixedAmount = -Math.round(Double.parseDouble(Strings.isNullOrEmpty(fixedAmountStr) ? "0" : fixedAmountStr) * 100);
        long annualFee = -Math.round(Double.parseDouble(Strings.isNullOrEmpty(annualFeeStr) ? "0" : annualFeeStr) * 100);
        String revenueRate = curr.at("/加液营收参与分成比例").asText();
        String agreement = curr.at("/加液协议分成比例").asText();
        String noLiquidRateStr = curr.at("/分成费（比例）").asText();
        String startDateStr = curr.at("/费用核算起始日期").asText();
        String endDateStr = curr.at("/合同截止日期").asText();
        String bizId2 = curr.at("/项目编号").asText();
        String storeId2 = curr.at("/门店编号").asText();

        if (!storeId2.equals("0") && !Strings.isNullOrEmpty(storeId2)) {
            bizId2 = null;
        } else {
            storeId2 = null;
        }

        if (Strings.isNullOrEmpty(startDateStr) || Strings.isNullOrEmpty(endDateStr)) {
            throw new UnsupportedOperationException("起始日或截止日为空, 编号为: " + no1);
        }

        double liquidRate = -Double.parseDouble(Strings.isNullOrEmpty(revenueRate) ? "0" : revenueRate) * Double.parseDouble(Strings.isNullOrEmpty(agreement) ? "0" : agreement);
        double noLiquidRate = -Double.parseDouble(Strings.isNullOrEmpty(noLiquidRateStr) ? "0" : noLiquidRateStr);

        String remarks = curr.at("/管理费备注").textValue();
        String[] remarkLines = remarks.split("\n");
        List<String> discountList = Lists.newArrayList();
        Pattern pattern = Pattern.compile("(\\d{4}-\\d{2}-\\d{2})至(\\d{4}-\\d{2}-\\d{2})\\s*优惠([\\d\\.]+)元.*");
        for (String remarkLine : remarkLines) {
            Matcher m = pattern.matcher(remarkLine);
            if (m.matches()) {
                discountList.add(remarkLine);
            }
        }

        List<Row> result = Lists.newArrayList();

        if (discountList.size() > 0) {

            if (!Strings.isNullOrEmpty(storeId2) && storeId2.contains("/")) {
                throw new UnsupportedOperationException("门店编号为多个且包含优惠, 编号为: " + no1);
            } else if (!Strings.isNullOrEmpty(bizId2) && bizId2.contains("/")) {
                throw new UnsupportedOperationException("项目编号为多个且包含优惠, 编号为: " + no1);
            }

            for (String discount : discountList) {
                String startTimeStr = discount.substring(0, discount.indexOf("至"));
                String endTimeStr = discount.substring(discount.indexOf("至") + 1, discount.indexOf("优惠"));
                String discountAmountStr = discount.substring(discount.indexOf("优惠") + 2, discount.indexOf("元"));

                // 注意这里取正值, 因为是优惠的, 可以理解为是收入
                long discountAmount = Math.round(Double.parseDouble(discountAmountStr) * 100);

                LocalDate discountStartTime = LocalDate.parse(startTimeStr);
                LocalDate discountEndTime = LocalDate.parse(endTimeStr).plusDays(1);
                Timestamp discountTS = Timestamp.valueOf(discountEndTime.atTime(0, 0));
                long tempTS = discountTS.getTime() / 1000;
                String discountTempRev = replaceIndex(0, rev, String.format("%010d", tempTS));

                Timestamp discountStartTimeTs = Timestamp.valueOf(discountStartTime.atTime(0, 0));
                Row shareRow = RowFactory.create(fileName, "主营业务成本.管理费", discountAmount, null, null, null, discountStartTimeTs);

                result.add(RowFactory.create(rev, discountTempRev, discountTS, shareRow, bizId2, storeId2));
            }
        }

        LocalDate startTime = LocalDate.parse(startDateStr);
        LocalDate endTime = LocalDate.parse(endDateStr).plusDays(1);
        Timestamp endDateTS = Timestamp.valueOf(endTime.atTime(0, 0));

        String computeMethod = curr.at("/管理费结算方式").asText();
        switch (computeMethod) {
            case "固定年租金形式":
            case "台年费形式":

                long amount = Strings.isNullOrEmpty(fixedAmountStr) ? annualFee : fixedAmount;

                if (!Strings.isNullOrEmpty(storeId2)) {
                    String[] storeIdArrays = storeId2.split("/");
                    Iterator<String> it = Arrays.asList(storeId2.split("/")).iterator();
                    for (Long a : shareAmount(amount, storeIdArrays.length)) {
                        String tempId = it.next();
                        for (LocalDate start = startTime; start.compareTo(endTime) < 0; start = start.plusYears(1)) {

                            Timestamp ts0 = Timestamp.valueOf(start.atTime(0, 0));
                            LocalDate end = Stream.of(start.plusYears(1), endTime).min(LocalDate::compareTo).get();
                            Timestamp ts1 = Timestamp.valueOf(end.atTime(0, 0));
                            long tempTime1 = ts1.getTime() / 1000;
                            String tempRev1 = replaceIndex(0, rev, String.format("%10d", tempTime1));

                            long shareDays = start.until(end, ChronoUnit.DAYS);
                            long sumDays = start.until(start.plusYears(1), ChronoUnit.DAYS);
                            if (start.plusYears(1).compareTo(endTime) <= 0) {
                                Row shareRow = RowFactory.create(fileName, "主营业务成本.管理费", a, null, null, null, ts0);
                                result.add(RowFactory.create(rev, tempRev1, ts1, shareRow, null, tempId));
                            } else {
                                long shareAmount = Math.round(1.0 * a * shareDays / sumDays);
                                Row shareRow = RowFactory.create(fileName, "主营业务成本.管理费", shareAmount, null, null, null, ts0);
                                result.add(RowFactory.create(rev, tempRev1, ts1, shareRow, null, tempId));
                            }
                        }
                    }
                } else if (!Strings.isNullOrEmpty(bizId2)) {
                    String[] bizIdArrays = bizId2.split("/");
                    Iterator<String> it = Arrays.asList(bizId2.split("/")).iterator();
                    for (Long a : shareAmount(amount, bizIdArrays.length)) {
                        String tempId = it.next();
                        for (LocalDate start = startTime; start.compareTo(endTime) < 0; start = start.plusYears(1)) {

                            Timestamp ts0 = Timestamp.valueOf(start.atTime(0, 0));
                            LocalDate end = Stream.of(start.plusYears(1), endTime).min(LocalDate::compareTo).get();
                            Timestamp ts1 = Timestamp.valueOf(end.atTime(0, 0));
                            long tempTime1 = ts1.getTime() / 1000;
                            String tempRev1 = replaceIndex(0, rev, String.format("%10d", tempTime1));

                            long shareDays = start.until(end, ChronoUnit.DAYS);
                            long sumDays = start.until(start.plusYears(1), ChronoUnit.DAYS);
                            if (start.plusYears(1).compareTo(endTime) <= 0) {
                                Row shareRow = RowFactory.create(fileName, "主营业务成本.管理费", a, null, null, null, ts0);
                                result.add(RowFactory.create(rev, tempRev1, ts1, shareRow, tempId, null));
                            } else {
                                long shareAmount = Math.round(1.0 * a * shareDays / sumDays);
                                Row shareRow = RowFactory.create(fileName, "主营业务成本.管理费", shareAmount, null, null, null, ts0);
                                result.add(RowFactory.create(rev, tempRev1, ts1, shareRow, tempId, null));
                            }
                        }
                    }
                } else {
                    for (LocalDate start = startTime; start.compareTo(endTime) < 0; start = start.plusYears(1)) {
                        Timestamp ts0 = Timestamp.valueOf(start.atTime(0, 0));
                        LocalDate end = Stream.of(start.plusYears(1), endTime).min(LocalDate::compareTo).get();
                        Timestamp ts1 = Timestamp.valueOf(end.atTime(0, 0));
                        long tempTime1 = ts1.getTime() / 1000;
                        String tempRev1 = replaceIndex(0, rev, String.format("%10d", tempTime1));

                        long shareDays = start.until(end, ChronoUnit.DAYS);
                        long sumDays = start.until(start.plusYears(1), ChronoUnit.DAYS);
                        if (start.plusYears(1).compareTo(endTime) <= 0) {
                            Row shareRow = RowFactory.create(fileName, "主营业务成本.管理费", amount, null, null, null, ts0);
                            result.add(RowFactory.create(rev, tempRev1, ts1, shareRow, null, null));
                        } else {
                            long shareAmount = Math.round(1.0 * amount * shareDays / sumDays);
                            Row shareRow = RowFactory.create(fileName, "主营业务成本.管理费", shareAmount, null, null, null, ts0);
                            result.add(RowFactory.create(rev, tempRev1, ts1, shareRow, null, null));
                        }
                    }
                }
                break;

            case "分成":
                if (remarks.contains("优惠")) {
                    throw new UnsupportedOperationException("分成存在优惠, 编号为: " + no1);
                }
                if ((!Strings.isNullOrEmpty(storeId2) && storeId2.contains("/")) ||
                        (!Strings.isNullOrEmpty(bizId2) && bizId2.contains("/"))) {
                    throw new UnsupportedOperationException("分成存在门店编号或项目编号多个的情况, 编号为: " + no1);
                }

                for (LocalDate start = startTime; start.compareTo(endTime) < 0; start = start.plusYears(1)) {

                    Timestamp ts2 = Timestamp.valueOf(start.atTime(0, 0));
                    LocalDate end = Stream.of(start.plusYears(1), endTime).min(LocalDate::compareTo).get();
                    Timestamp endTs3 = Timestamp.valueOf(end.atTime(0, 0));
                    long tempTime2 = endTs3.getTime() / 1000;
                    String tempRev2 = replaceIndex(0, rev, String.format("%010d", tempTime2));

                    Row shareRow = RowFactory.create(fileName, "主营业务成本.管理费（分成费）", null,
                            noLiquidRate, liquidRate, null, ts2);

                    result.add(RowFactory.create(rev, tempRev2, endTs3, shareRow, bizId2, storeId2));
                }
                break;

            case "固定年租金+分成":
            case "台年费+分成":

                if (remarks.contains("优惠")) {
                    throw new UnsupportedOperationException("固定金额+分成存在优惠, 编号为: " + no1);
                }
                if ((!Strings.isNullOrEmpty(storeId2) && storeId2.contains("/")) ||
                        (!Strings.isNullOrEmpty(bizId2) && bizId2.contains("/"))) {
                    throw new UnsupportedOperationException("固定金额+分成存在门店编号或项目编号多个的情况, 编号为: " + no1);
                }

                long amount1 = Strings.isNullOrEmpty(fixedAmountStr) ? annualFee : fixedAmount;
                for (LocalDate start = startTime; start.compareTo(endTime) < 0; start = start.plusYears(1)) {

                    Timestamp ts3 = Timestamp.valueOf(start.atTime(0, 0));
                    LocalDate end = Stream.of(start.plusYears(1), endTime).min(LocalDate::compareTo).get();
                    Timestamp endTs4 = Timestamp.valueOf(end.atTime(0, 0));
                    long tempTime3 = endTs4.getTime() / 1000;
                    String tempRev3 = replaceIndex(0, rev, String.format("%010d", tempTime3));

                    long shareDays = start.until(end, ChronoUnit.DAYS);
                    long sumDays = start.until(start.plusYears(1), ChronoUnit.DAYS);

                    Row shareRatio = RowFactory.create(fileName, "主营业务成本.管理费（分成费）", null,
                            noLiquidRate, liquidRate, null, ts3);
                    if (start.plusYears(1).compareTo(endTime) <= 0) {
                        Row shareRow = RowFactory.create(fileName, "主营业务成本.管理费", amount1, null, null, null, ts3);
                        result.add(RowFactory.create(rev, tempRev3, endTs4, shareRow, bizId2, storeId2));
                        result.add(RowFactory.create(rev, tempRev3, endTs4, shareRatio, bizId2, storeId2));
                    } else {
                        long shareAmount = Math.round(1.0 * amount1 * shareDays / sumDays);
                        Row shareRow = RowFactory.create(fileName, "主营业务成本.管理费", shareAmount, null, null, null, ts3);

                        result.add(RowFactory.create(rev, tempRev3, endTs4, shareRow, bizId2, storeId2));
                        result.add(RowFactory.create(rev, tempRev3, endTs4, shareRatio, bizId2, storeId2));
                    }
                }
                break;
            case "台年费与分成取高":

                if (remarks.contains("优惠")) {
                    throw new UnsupportedOperationException("台年费与分成取高存在优惠, 编号为: " + no1);
                }
                if ((!Strings.isNullOrEmpty(storeId2) && storeId2.contains("/")) ||
                        (!Strings.isNullOrEmpty(bizId2) && bizId2.contains("/"))) {
                    throw new UnsupportedOperationException("台年费与分成取高存在门店编号或项目编号多个的情况, 编号为: " + no1);
                }

                long amount2 = Strings.isNullOrEmpty(fixedAmountStr) ? annualFee : fixedAmount;
                for (LocalDate start = startTime; start.compareTo(endTime) < 0; start = start.plusYears(1)) {

                    Timestamp ts4 = Timestamp.valueOf(start.atTime(0, 0));
                    LocalDate end = Stream.of(start.plusYears(1), endTime).min(LocalDate::compareTo).get();
                    Timestamp endTs5 = Timestamp.valueOf(end.atTime(0, 0));
                    long tempTime4 = endTs5.getTime() / 1000;
                    String tempRev4 = replaceIndex(0, rev, String.format("%010d", tempTime4));

                    long shareDays = start.until(end, ChronoUnit.DAYS);
                    long sumDays = start.until(start.plusYears(1), ChronoUnit.DAYS);

                    if (start.plusYears(1).compareTo(endTime) <= 0) {
                        Row shareRow = RowFactory.create(fileName, "主营业务成本.管理费", amount2, noLiquidRate, liquidRate, null, ts4);
                        result.add(RowFactory.create(rev, tempRev4, endTs5, shareRow, bizId2, storeId2));
                    } else {
                        long shareAmount = Math.round(1.0 * amount2 * shareDays / sumDays);
                        Row shareRow = RowFactory.create(fileName, "主营业务成本.管理费", shareAmount, noLiquidRate, liquidRate, null, ts4);
                        result.add(RowFactory.create(rev, tempRev4, endTs5, shareRow, bizId2, storeId2));
                    }
                }
                break;
            default:
                throw new UnsupportedOperationException("管理费结算方式不确定, 编号为: " + no1);
        }

        return modifyRev(result);
    }

    // 付款申请审批
    public static Row[] fukuan_cost_data(JsonNode curr, String rev) {

        if (curr.size() == 0) {
            return null;
        }

        String paymentTime = curr.at("/打款日期").asText();
        if (Strings.isNullOrEmpty(paymentTime)) {
            return null;
        }

        Timestamp ts = Timestamp.valueOf(LocalDate.parse(paymentTime).atTime(0, 0));
        long tempTime = ts.getTime() / 1000;
        String tempRev = replaceIndex(0, rev, String.format("%010d", tempTime));

        String no1 = curr.at("/编号").asText();
        String accountName = curr.at("/收款户名").asText();
        String account = curr.at("/收款账号").asText();
        long allAmount = Math.round(curr.at("/总付款金额").asDouble() * 100);
        JsonNode dept = curr.at("/所在部门");

        Set<String> deptSet = Sets.newHashSet();
        boolean isCollectionDept = false;
        for (JsonNode deptNode : dept) {
            String deptName = deptNode.at("/name").asText();
            deptSet.add(deptName);
        }

        if (deptSet.contains("采供部")) {
            isCollectionDept = true;
        }

        List<Row> result = Lists.newArrayList();
        Row cost = RowFactory.create(no1, null);

        long checkAmount = 0L;
        for (JsonNode i : curr.at("/付款明细")) {

            String storeId = i.at("/门店编号").asText();
            String bizId = i.at("/运营体编号").asText();

            if (storeId.equals("0")) {
                storeId = null;
            }
            if (bizId.equals("0")) {
                bizId = null;
            }

            String paymentCategory = i.at("/付款类别").asText();
            String subject = paymentCategory.replaceAll("^\\d+\\.\\d+([^\\-]+)\\-(.*)", "$1.$2");
            long lineAmount = Math.round(i.at("/付款金额").asDouble() * 100);

            boolean b1 = paymentCategory.equals("1.10销售费用-低值易耗品（门店运营耗材）") ||
                    paymentCategory.equals("4.07固定资产-运营电器") ||
                    paymentCategory.equals("4.01固定资产-主洗") ||
                    paymentCategory.equals("4.09固定资产-办公电器") ||
                    paymentCategory.equals("4.02固定资产-安卓屏") ||
                    paymentCategory.equals("4.10固定资产-洗鞋机") ||
                    paymentCategory.equals("4.03固定资产-加液器") ||
                    paymentCategory.equals("4.11固定资产-开水器") ||
                    paymentCategory.equals("4.04固定资产-监控") ||
                    paymentCategory.equals("4.05固定资产-柜体") ||
                    paymentCategory.equals("4.06固定资产-维保工具") ||
                    paymentCategory.equals("5.01库存商品-低值易耗品");

            //如果是不排除的数据, 输出
            if (!(isCollectionDept && b1)) {
                Row payment = RowFactory.create(ts, subject, accountName, account, (-1) * lineAmount, null);
                result.add(RowFactory.create(
                        rev, tempRev, ts,
                        new Row[]{payment}, cost,
                        storeId, bizId
                ));
            }
            checkAmount += lineAmount;
        }

        Preconditions.checkArgument(checkAmount == allAmount, "金额不等!! 编号为: " + no1);
        return modifyRev(result);
    }

    // 日常报销审批
    public static Row[] baoxiao_cost_data(JsonNode curr, String rev) {

        if (curr.size() == 0) {
            return null;
        }

        String paymentTime = curr.at("/打款日期").asText();
        if (Strings.isNullOrEmpty(paymentTime)) {
            return null;
        }

        Timestamp ts = Timestamp.valueOf(LocalDate.parse(paymentTime).atTime(0, 0));
        long tempTime = ts.getTime() / 1000;
        String tempRev = replaceIndex(0, rev, String.format("%010d", tempTime));

        String no1 = curr.at("/编号").asText();
        long allAmount = Math.round(curr.at("/总报销金额").asDouble() * 100);
        JsonNode dept = curr.at("/所在部门");

        Set<String> deptSet = Sets.newHashSet();
        boolean isCollectionDept = false;
        for (JsonNode deptNode : dept) {
            String deptName = deptNode.at("/name").asText();
            deptSet.add(deptName);
        }

        if (deptSet.contains("采供部")) {
            isCollectionDept = true;
        }

        List<Row> result = Lists.newArrayList();
        Row cost = RowFactory.create(no1, null);

        long checkAmount = 0L;
        for (JsonNode i : curr.at("/报销明细")) {

            String storeId = i.at("/门店ID").asText();
            String bizId = i.at("/运营体编号").asText();

            if (storeId.equals("0")) {
                storeId = null;
            }
            if (bizId.equals("0")) {
                bizId = null;
            }

            String paymentCategory = i.at("/报销类别").asText();
            String subject = paymentCategory.replaceAll("^\\d+\\.\\d+([^\\-]+)\\-(.*)", "$1.$2");
            long lineAmount = Math.round(i.at("/报销金额").asDouble() * 100);

            boolean b1 = paymentCategory.equals("1.10销售费用-低值易耗品（门店运营耗材）") ||
                    paymentCategory.equals("4.07固定资产-运营电器") ||
                    paymentCategory.equals("4.01固定资产-主洗") ||
                    paymentCategory.equals("4.09固定资产-办公电器") ||
                    paymentCategory.equals("4.02固定资产-安卓屏") ||
                    paymentCategory.equals("4.10固定资产-洗鞋机") ||
                    paymentCategory.equals("4.03固定资产-加液器") ||
                    paymentCategory.equals("4.11固定资产-开水器") ||
                    paymentCategory.equals("4.04固定资产-监控") ||
                    paymentCategory.equals("4.05固定资产-柜体") ||
                    paymentCategory.equals("4.06固定资产-维保工具") ||
                    paymentCategory.equals("5.01库存商品-低值易耗品");

            //如果是不排除的数据, 输出
            if (!(isCollectionDept && b1)) {
                Row payment = RowFactory.create(ts, subject, null, null, -lineAmount, null);
                result.add(RowFactory.create(
                        rev, tempRev, ts,
                        new Row[]{payment}, cost,
                        storeId, bizId
                ));
            }
            checkAmount += lineAmount;
        }

        Preconditions.checkArgument(checkAmount == allAmount, "金额不等!! 编号为: " + no1);
        return modifyRev(result);
    }


    private static Column rev(String prefix, String tsExpr, String revExpr) {
        return udf((Timestamp ts, String rev) -> {
            return String.format("%010dQ%s%s", ts.getTime() / 1000, prefix, rev);
        }, StringType$.MODULE$).apply(expr(tsExpr), expr(revExpr));
    }

    private static Dataset<Row> load(Dataset<Row> qingliu, String prefix) {
        String deletePatch = "[{\"op\":\"replace\",\"path\":\"\",\"value\":{}}]";
        Dataset<Row> r = qingliu
                .withColumn("id", expr("concat(app_id,':',apply_id)"))
                .withColumn("id_rev", rev(prefix, "audit_time", "concat(app_id,':',apply_id,':',log_id)"))
                .withColumn("patch", My.answer2value.apply(col("after")))
                .withColumn("patch", My.jsonReplace("patch", "if(audit_name = '领用结果抄送', concat('\"',audit_name,'\"'), null) ", "/audit_name"))
                .withColumn("patch", when(col("active"), json2patch("patch")).otherwise(deletePatch))
                .selectExpr("id", "patch", "id_rev");
        return r;
    }

    public static Long takeAmount(String str, Long defaultValue) {
        String subString;

        if (Strings.isNullOrEmpty(str)) {
            return defaultValue;
        }

        if (str.contains("本次付款周期期间应付金额：")) {
            int index = str.indexOf("本次付款周期期间应付金额：");
            String subAftStr = str.substring(index + 13);
            int index1 = subAftStr.indexOf("=");
            String subPreStr = subAftStr.substring(0, index1 - 1);
            subString = subPreStr;
        } else {
            String lastLineStr = str.substring(str.lastIndexOf("\n") + 1);
            if (lastLineStr.contains("=")) {
                subString = lastLineStr.substring(0, lastLineStr.indexOf("=") - 1);
            } else {
                return defaultValue;
            }
        }

        if (!subString.substring(0, 1).matches("^-?|[1-9]")) {
            return defaultValue;
        }

        if (subString.matches("^-?\\s+[0-9]+.[0-9]+")) {
            subString = subString.replaceAll(" ", "");
        }

        Double tempAmount = Double.parseDouble(subString);
        long amount = Math.round(tempAmount * 100);
        return amount;
    }

    private static Iterable<Long> shareAmount(Long amount, int length) {

        long remainder = Math.abs(amount % length);
        long avg = amount / length;
        long d = avg < 0 ? -1 : 1;

        List<Long> list = new ArrayList<>();
        for (int i = 0; i < length - remainder; i++) {
            list.add(avg);
        }
        for (int j = length - (int) remainder; j < length; j++) {
            list.add(avg + d);
        }

        return list;
    }

    private static String replaceIndex(int index, String res, String str) {
        return res.substring(0, index) + str + res.substring(index + str.length());
    }

    private static Row[] modifyRev(Collection<Row> rows) {

        // 总数
        int count = rows.size();
        int len = ("" + count).length();    // 确定补位

        // 构建一个stream 后缀字符串stream
        Stream<String> streamSuffix = Stream.iterate(0, i -> ++i).limit(count)
                .map(i -> String.format("%" + len + "d", i));

        // zip 两个stream
        Stream<Row> result = Streams.zip(rows.stream(), streamSuffix, (a, b) -> {
            List<Object> objects = JavaConverters.seqAsJavaListConverter(a.toSeq()).asJava();
            Object[] tempArray = objects.toArray();
            tempArray[1] = tempArray[1] + ":" + b;
            return RowFactory.create(tempArray);
        });
        return result.toArray(Row[]::new);
    }

    public static Timestamp ts(String rev) {
        long time = Long.parseLong(rev.substring(0, 10));
        return Timestamp.from(Instant.ofEpochSecond(time));
    }

}

