package boluo.work;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.google.common.io.CharSource;
import com.google.common.io.Resources;
import io.delta.tables.DeltaTable;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.*;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.*;

public class Base2OrderCheck {

	private static final Logger logger = LoggerFactory.getLogger(Base2OrderCheck.class);
	private static final ObjectMapper mapper = new ObjectMapper();
	private static final boolean TEST = false;

	/*
-i1=file:///D:/data/base2_linked
-i2=file:///D:/data
-d=file:///D:/data/check_bill_diff
-u=https://oapi.dingtalk.com/robot/send?access_token=5b87
 * */
	public static void main(String[] args) throws ParseException, IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {

		// 1. 时间问题, 取本地上次对账后, 最早修改时间文件对应的时间 和 支付账单文件上次对账后, 最早修改时间文件对应的时间, 两个时间取较早的一个, 作为本地和微信的开始时间
		// 2. 对账问题, 本地数据(订单号, 金额, 账户, 以订单号分组求和后 join (wechat以订单号分组求和)), 对账户开窗, 统计 wechat.金额 - 本地.金额 != 0的

		SparkSession spark = SparkSession.builder()
				.getOrCreate();

		CommandLine cli = new GnuParser().parse(new Options()
				.addOption("i1", "base_linked", true, "本地账单")
				.addOption("i2", "pay_bills", true, "支付订单信息")
				.addOption("d", "check_bill_diff", true, "上次对账差异部分")
				.addOption("u", "notify_url", true, "报告发送地址")
				.addOption("p", "", true, "对账时间间隔"), args);

		// 参数
		String base2_linked = cli.getOptionValue("i1");
		String pathBills = cli.getOptionValue("i2");
		String check_bill_diff = cli.getOptionValue("d");
		String notifyUri = cli.getOptionValue("u");
		String period = "P" + cli.getOptionValue("p", "24M");

		// 取上次对账时间
		Instant now = Instant.now();
		Map<String, String> metaTs = null; // Outputs.meta(check_bill_diff);
		Instant lastTs = Instant.parse(metaTs.getOrDefault("check", "2000-01-01T00:00:00Z"));

		// 取上次对账时间后linked最早的修改时间
		String linkedHash = getLeadPartition(base2_linked, "hash", lastTs);
		LocalDate lastUpdate1 = Strings.isNullOrEmpty(linkedHash)
				? LocalDateTime.ofInstant(lastTs, ZoneId.systemDefault()).toLocalDate()
				: LocalDateTime.ofInstant(Instant.ofEpochMilli(Integer.parseInt(linkedHash) * 1000000000L), ZoneId.systemDefault()).toLocalDate();

		// 取对账单中上次对账后最早修改时间文件的对应时间
		Optional<LocalDate> payBillLastUpdate = getPayBillLastUpdate(pathBills, lastTs);
		LocalDate lastUpdate2 = payBillLastUpdate.orElseGet(() -> LocalDateTime.ofInstant(lastTs, ZoneId.systemDefault()).toLocalDate());

		lastUpdate1 = Stream.of(lastUpdate1, LocalDate.of(2019, 10, 1)).max(LocalDate::compareTo).get();
		lastUpdate2 = Stream.of(lastUpdate2, LocalDate.of(2019, 10, 1)).max(LocalDate::compareTo).get();

		// 取上次对账后, 支付账单最早修改时间文件的对应时间, 和本地上次对账后最早修改时间取较早的, 作为本次对账时间
		LocalDate startTime = Stream.of(lastUpdate1, lastUpdate2).min(LocalDate::compareTo).get();
		LocalDate endTime = Stream.of(startTime.plus(Period.parse(period)), LocalDate.now()).min(LocalDate::compareTo).get();

		// 加载本地数据及对账单数据, 并且格式化
		int hash = Strings.isNullOrEmpty(linkedHash) ? 0 : Integer.parseInt(linkedHash) - 1;
		Dataset<Row> localCheckDs = loadLocal(base2_linked, hash, startTime, endTime);
		Dataset<Row> payCheckDs = loadBill(pathBills, startTime, endTime);
		Dataset<Row> formatResult = formatData(localCheckDs, payCheckDs).cache();

		Dataset<Row> result = saveResult(formatResult, check_bill_diff, startTime).cache();
		report(formatResult, result, notifyUri, now, startTime, endTime);

		metaTs.put("check", String.valueOf(now));
		// Outputs.meta(check_bill_diff, metaTs);
	}

	private static Dataset<Row> loadLocal(String path, int hash, LocalDate start, LocalDate end) {

		Dataset<Row> df1 = SparkSession.active().read().format("delta").load(path);    // base2_linked

		return df1.withColumn("payment", expr("explode(payment)"))
				.where(String.format("payment.`时间` >= '%s' and payment.`时间` < '%s'", start, end))
				.where(String.format("hash > '%s'", hash))
				.select(
						expr("to_timestamp(ts, 'yyyy-MM-dd HH:mm:ss') time"),
						expr("payment.`第三方流水号`").as("订单号"),
						expr("order.`订单号`").as("订单号2"),
						expr("payment.`金额`").as("金额"),
						expr("payment.`账户`").as("账户")
				);
	}

	private static Dataset<Row> loadBill(String path, LocalDate start, LocalDate end) {

		Dataset<Row> wechat = SparkSession.active().read()
				.option("sep", ",")
				.option("inferSchema", "false")
				.option("header", "true")
				.csv(path + "/wechat")
				.withColumn("amt", expr("ifnull(floor(round(substring(`订单金额`,2)*100,0)),0)"))
				.withColumn("amt_ex", expr("ifnull(-floor(round(substring(`申请退款金额`,2)*100,0)),0)"))
				.withColumn("fee", expr("ifnull(-floor(round(substring(`手续费`,2)*100,0)),0)"))
				.withColumn("金额", col("amt").plus(col("amt_ex")).plus(col("fee")))
				.withColumn("pay_金额", col("金额").multiply(-1))
				.select(
						expr("to_timestamp(substring(`交易时间`,2), 'yyyy-MM-dd HH:mm:ss') pay_time"),
						expr("ifnull(substring(`商户订单号`,2),'') `pay_订单号`"),
						expr("`pay_金额`")
				);

		Dataset<Row> wechat2 = SparkSession.active().read()
				.option("sep", ",")
				.option("inferSchema", "false")
				.option("header", "true")
				.csv(path + "/wechat2")
				.withColumn("amt", expr("if(`业务类型`='`交易',floor(round(substring(`收支金额(元)`,2)*100,0)),0)"))
				.withColumn("amt_ex", expr("if(`业务类型`='`退款',-floor(round(substring(`收支金额(元)`,2)*100,0)),0)"))
				.withColumn("fee", expr("if(`业务类型`='`扣除交易手续费',-floor(round(substring(`收支金额(元)`,2)*100,0)),0)"))
				.withColumn("金额", col("amt").plus(col("amt_ex")).plus(col("fee")))
				.withColumn("pay_金额", col("金额").multiply(-1))
				.select(
						expr("to_timestamp(substring(`记账时间`,2), 'yyyy-MM-dd HH:mm:ss') pay_time"),
						expr("ifnull(substring(`资金流水单号`,2),'') `pay_订单号`"),
						expr("`pay_金额`")
				);

		Dataset<Row> alipay2 = SparkSession.active().read()
				.option("sep", ",")
				.option("comment", "#")
				.option("inferSchema", "false")
				.option("header", "true")
				.option("charset", "GB2312")
				.csv(path + "/alipay2/*/*")
				.withColumn("amt", expr("ifnull(floor(if(`业务类型` like '交易%',round(`订单金额（元）`*100,0),0)),0)"))
				.withColumn("amt_ex", expr("ifnull(floor(if(`业务类型` like '退款%',round(`订单金额（元）`*100,0),0)),0)"))
				.withColumn("fee", expr("ifnull(floor(round(`服务费（元）`*100,0)),0)"))
				.withColumn("金额", col("amt").plus(col("amt_ex")).plus(col("fee")))
				.withColumn("pay_金额", col("金额").multiply(-1))
				.select(
						expr("ifnull(to_timestamp(replace(`完成时间`,'\\t','')),cast(2000-01-01 as timestamp)) pay_time"),
						expr("ifnull(replace(`支付宝交易号`,'\t',''),'') `pay_订单号`"),
						expr("`pay_金额`")
				);

		Dataset<Row> res = Stream.of(wechat, wechat2, alipay2).reduce(Dataset::unionAll).get();
		return res.where(String.format("pay_time >= '%s' and pay_time < '%s'", start, end));
	}

	private static Dataset<Row> formatData(Dataset<Row> localCheckDs, Dataset<Row> payCheckDs) {

		// 本地数据和微信数据先分别对订单号分组求和
		localCheckDs = localCheckDs.groupBy("`订单号`", "`订单号2`")
				.agg(sum("`金额`").as("金额"), max("`账户`").as("账户"), max("time").as("local_time"));
		payCheckDs = payCheckDs.groupBy("`pay_订单号`")
				.agg(sum("`pay_金额`").as("pay_金额"), max("pay_time").as("time"));

		// 聚合本地订单和支付订单
		Dataset<Row> totalDs = localCheckDs.as("s").join(
				payCheckDs.as("t"),
				expr("s.`订单号` = t.`pay_订单号`"),
				"outer"
		);

		// 对账户进行开窗, 只留下支付订单中有订单号的对应账户
		totalDs = totalDs
				.withColumn("flag", expr("if(`pay_订单号` is null, 0, 1)"))
				.withColumn("account_num", expr("sum(flag) over (partition by `账户`)"))
				.where("account_num > 0")
		;
		return totalDs;
	}

	// 对账求差异数据并存储
	private static Dataset<Row> saveResult(Dataset<Row> totalDs, String path, LocalDate startTime) {

		// 相加结果不为0的即为有差异的
		Dataset<Row> currCheckDs = totalDs
				.withColumn("res", expr("ifnull(`金额`,0)+ifnull(`pay_金额`,0)"))
				.where("res != 0")
				.select(
						expr("ifnull(`订单号`,`pay_订单号`)").as("订单号"),
						expr("ifnull(`订单号2`,'')").as("订单号2"),
						expr("res").as("金额"),
						expr("ifnull(time,local_time)").as("time"),
						expr("`账户`").as("账户")
				);

		// 上次对账差异
		Dataset<Row> lastCheckDs = DeltaTable.isDeltaTable(path)
				? SparkSession.active().read().format("delta").load(path)
				: null;

		Dataset<Row> result;
		if (Objects.nonNull(lastCheckDs) && !lastCheckDs.schema().isEmpty()) {

			// 合并两次的差异结果, 但是要先把上次对账结果中, 大于等于本次开始对账时间的部分去掉, 再合并
			lastCheckDs = lastCheckDs.where(String.format("time < '%s'", startTime));
			result = lastCheckDs.union(currCheckDs);

			// 对本次和上次的差异再次分组求和
			result = result.groupBy("`订单号`", "`订单号2`")
					.agg(sum("`金额`").as("金额"), max("time").as("time"))
					.filter((FilterFunction<Row>) row -> !Objects.equals(row.getAs("金额"), 0L));
		} else {
			result = currCheckDs;
		}

		result.write().format("delta")
				.mode("overwrite")
				.save(path);

		return result;
	}

	private static void report(Dataset<Row> formatDs, Dataset<Row> ds, String notifyUri, Instant startClock, LocalDate start, LocalDate end) throws IOException {

		List<Row> account = formatDs.withColumn("账户订单号", expr("ifnull(`订单号`, `pay_订单号`)"))
				.withColumn("账户时间", expr("ifnull(local_time, time)"))
				.groupBy(expr("`账户`").as("accountNo"))
				.agg(count("`账户订单号`").as("count"),
						max("`账户时间`").as("max_time"),
						min("`账户时间`").as("min_time")
				)
				.withColumn("accountName", expr("concat(left(accountNo,3),'×××',right(accountNo,4))"))
				.withColumn("days", datediff(col("max_time"), col("min_time")))
				.where("`账户` is not null")
				.collectAsList();

		List<Row> dsByDay = ds.groupBy(date_format(col("time"), "yyyy年MM月dd日").as("date"))
				.agg(count("`订单号`").as("order"),
						sum("`金额`").as("sumAmount")
				)
				.withColumn("amount", expr("round(sumAmount/100,2)"))
				.orderBy(desc("date"))
				.collectAsList();

		List<Row> res = ds.select(
				expr("`订单号`").as("orderid"),
				expr("concat(`订单号2`,'\n')").as("orderid2"),
				expr("round(`金额`/100,2)").as("delta")
		).limit(100).orderBy(desc("orderid")).collectAsList();

		long num = res.size();
		boolean br = !res.isEmpty();

		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy年MM月dd日");
		String title = (start.plusDays(1).equals(end) ? formatter.format(start) : (formatter.format(start) + "至" + formatter.format(end)))
				+ "对账报告";
		String info = TEST ? "测试数据, 请勿当真!!!" : "这不是演习！这不是演习！！";

		CharSource charSource = Resources.asCharSource(
				Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource("base2_check_report.md")),
				Charset.defaultCharset()
		);
		ST md = new ST(charSource.read());
		md.add("title", title);
		md.add("info", info);
		md.add("num", num);
		md.add("clock", startClock.until(Instant.now(), ChronoUnit.SECONDS));
		md.add("stopDate", DateTimeFormatter.ofPattern("MM月dd日").format(end.minusDays(1)));
		md.add("result", res.stream().map(Base2OrderCheck::toMap).collect(Collectors.toList()));
		md.add("resByDay", dsByDay.stream().map(Base2OrderCheck::toMap).collect(Collectors.toList()));
		md.add("account", account.stream().map(Base2OrderCheck::toMap).collect(Collectors.toList()));
		md.add("has3", br);
		logger.info("\n{}", md.render());

		// 发送
		ObjectNode param = mapper.createObjectNode()
				.put("msgtype", "markdown");
		param.with("markdown")
				.put("title", title)
				.put("text", md.render());

		// Reports.dingtalk(notifyUri, param.toString());
	}

	private static Map<String, Object> toMap(Row row) {
		return Maps.toMap(
				Arrays.asList(row.schema().fieldNames()),
				row::getAs
		);
	}

	private static String getLeadPartition(String path, String part, Instant ts) throws IOException {

		List<String> results = Lists.newArrayList();
		Path parentPath = new Path(path);

		FileSystem fs = FileSystem.get(parentPath.toUri(), SparkSession.active().sparkContext().hadoopConfiguration());
		FileStatus[] fsParentFiles = fs.listStatus(parentPath);
		List<FileStatus> fsParentLists = Arrays.asList(fsParentFiles);
		Collections.sort(fsParentLists);
		for (FileStatus fileParent : fsParentLists) {

			if (fileParent.getPath().toString().contains(part + "=__HIVE_DEFAULT_PARTITION__")) {
				continue;
			}
			int parentNameIndex = fileParent.getPath().toString().lastIndexOf("/");
			String parentName = fileParent.getPath().toString().substring(parentNameIndex + 1);

			if (parentName.startsWith(part + "=")) {

				// 遍历目录下每个文件
				FileStatus[] fsFiles = fs.listStatus(fileParent.getPath());
				for (FileStatus fileChild : fsFiles) {

					// 如果找到文件修改时间大于传入时间的情况
					if (ts.compareTo(Instant.ofEpochMilli(fileChild.getModificationTime())) < 0) {
						int index = fileParent.getPath().toString().indexOf("=");
						String pathName = fileParent.getPath().toString().substring(index + 1);
						results.add(pathName);
						break;
					}
				}
			}
		}

		if (results.size() == 0) {
			return null;
		} else {
			// 字符串排序, 返回第一个
			Collections.sort(results);
			return results.get(0);
		}
	}

	private static Optional<LocalDate> getPayBillLastUpdate(String path, Instant ts) throws IOException {
		String wechat = path + "/wechat";
		String wechat2 = path + "/wechat2";
		String alipay2 = path + "/alipay2";

		Optional<LocalDate> wechatLastUpdate = getWeChatLastUpdate(wechat, ts, "All");
		Optional<LocalDate> wechat2LastUpdate = getWeChatLastUpdate(wechat2, ts, "资金账单");
		Optional<LocalDate> alipay2LastUpdate = getAliPayLastUpdate(alipay2, ts);

		return Streams.concat(Streams.stream(wechatLastUpdate),
				Streams.stream(wechat2LastUpdate), Streams.stream(alipay2LastUpdate)).min(LocalDate::compareTo);
	}

	// 取微信文件上次对账后, 最早修改时间文件对应的时间
	private static Optional<LocalDate> getWeChatLastUpdate(String path, Instant ts, String str) throws IOException {
		List<LocalDate> results = Lists.newArrayList();
		Path wechatPath = new Path(path);

		FileSystem fs = FileSystem.get(wechatPath.toUri(), SparkSession.active().sparkContext().hadoopConfiguration());
		FileStatus[] files = fs.listStatus(wechatPath);

		for (FileStatus file : files) {
			if (!file.getPath().toString().endsWith(".csv")) {
				continue;
			}
			// 留下csv文件
			if (ts.compareTo(Instant.ofEpochMilli(file.getModificationTime())) < 0) {
				String fileName = file.getPath().getName();
				int idx1 = fileName.indexOf(str) + str.length();
				String fileDate = fileName.substring(idx1, idx1 + 10);
				results.add(LocalDate.parse(fileDate));
			}
		}

		return results.stream().min(LocalDate::compareTo);
	}

	// 取阿里文件上次对账后, 最早修改时间文件对应的时间
	private static Optional<LocalDate> getAliPayLastUpdate(String path, Instant ts) throws IOException {
		List<LocalDate> results = Lists.newArrayList();
		Path aliPath = new Path(path);

		FileSystem fs = FileSystem.get(aliPath.toUri(), SparkSession.active().sparkContext().hadoopConfiguration());
		RemoteIterator<LocatedFileStatus> it = fs.listFiles(aliPath, true);

		while (it.hasNext()) {
			LocatedFileStatus file = it.next();
			if (!file.getPath().toString().endsWith(".csv")) {
				continue;
			}
			if (ts.compareTo(Instant.ofEpochMilli(file.getModificationTime())) < 0) {
				String fileName = file.getPath().getName();
				String str1 = fileName.substring(fileName.indexOf("_") + 1);
				String str2 = str1.substring(0, str1.indexOf("_"));
				DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyyMMdd");
				results.add(LocalDate.parse(str2, format));
			}
		}
		return results.stream().min(LocalDate::compareTo);
	}


}
