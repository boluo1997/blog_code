package boluo.work;


import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Function1;

import java.io.IOException;
import java.time.*;
import java.util.*;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.*;

public class CheckBill {

	public static void main(String[] args) throws ParseException, IOException {

		// 涉及到三个数据库, mongo wechat lastCheckResult
		// 1. 取mongo最小修改hash之后的文件进行对账, 数据存入checkDs
		// 2. 取wechat对应的部分进行对账, 数据存入checkDs
		// 3. 取出两部分结果的差异部分
		// 4. 本次差异 union 上次差异 = 本次差异

		// 1. 时间问题, 取本地上次对账后, 最早修改时间文件对应的时间
		// 取微信文件上次对账后, 最早修改时间文件对应的时间
		// 两个时间取较早的一个, 作为本地和微信的开始时间

		// 2. 对账问题, 本地数据增加账户列,
		// 本地数据(订单号, 金额, 账户, 以订单号分组求和后 join (wechat以订单号分组求和))
		// 对账户开窗, 统计 wechat.金额 - 本地.金额 != 0的

		SparkSession spark = SparkSession.builder()
				.getOrCreate();

		CommandLine cli = new GnuParser().parse(new Options()
				.addOption("b", "base_linked", true, "本地账单")
				.addOption("w", "wechat", true, "微信账单")
				.addOption("d", "check_bill_diff", true, "上次对账差异部分")
				.addOption("p", "", true, "对账时间间隔"), args);

		// 参数
		String base2_linked = cli.getOptionValue("b");
		String wechat = cli.getOptionValue("w");
		String check_bill_diff = cli.getOptionValue("d");
		String period = "P" + cli.getOptionValue("p", "24M");

		// 1. 取本地差异部分文件
		Instant now = Instant.now();
		// Outputs.meta(base2_linked);
		Map<String, String> metaTs = Maps.newHashMap();
		Instant lastTs = Instant.parse(metaTs.getOrDefault("check", "2000-01-01T00:00:00Z"));

		// 假设 hash: 1507 , 1507000...对应的时刻为2017-10-03 11:06:40
		// 也就是说2017-10-03 00:00:00 - 11:06:40 部分的数据在 hash - 1中
		String linkedHash = getLeadPartition(base2_linked, "hash", lastTs);
		int hash = Strings.isNullOrEmpty(linkedHash)
				? 0
				: Integer.parseInt(linkedHash) - 1;

		// 15070000...对应的时刻, 及相对应的时间: 2017-10-03 00:00:00
		long hash_unix_timestamp = (hash + 1) * 1000000000L;
		LocalDate lastUpdate = LocalDateTime.ofInstant(Instant.ofEpochMilli(hash_unix_timestamp), ZoneId.systemDefault()).toLocalDate();
		LocalDate localLastUpdate = lastUpdate.compareTo(LocalDate.of(2019, 10, 1)) < 0 ? LocalDate.of(2019, 10, 1) : lastUpdate;

		// 取微信上次对账后, 最早修改时间文件的对应时间
		Optional<LocalDate> wechatLastUpdate = getWeChatLastUpdate(wechat, lastTs);
		LocalDate startTime = localLastUpdate;
		if (wechatLastUpdate.isPresent()) {
			startTime = Stream.of(localLastUpdate, wechatLastUpdate.get()).min(LocalDate::compareTo).get();
		}
		LocalDate endTime = startTime.plus(Period.parse(period));

		// 本地订单号及金额
		Dataset<Row> df1 = spark.read().format("delta").load(base2_linked);    // base2_linked
		Dataset<Row> localCheckDs = df1.withColumn("payment", expr("explode(payment)"))
				.where(String.format("payment.`时间` >= '%s' and payment.`时间` < '%s'", startTime, endTime))
				.where(String.format("hash > '%s'", hash))
				// .where("payment.`第三方流水号` = '20191002152505905002566714'")
				.select(
						expr("to_timestamp(ts, 'yyyy-MM-dd HH:mm:ss') time"),
						expr("payment.`第三方流水号`").as("订单号"),
						expr("payment.`金额`").as("金额"),
						expr("payment.`账户`").as("账户")
				);

		localCheckDs.show(false);
		// 取微信差异部分数据, 微信订单号及金额
		Dataset<Row> df2 = spark.read().option("header", true).format("csv").load(wechat);        // wechat
		Dataset<Row> wechatCheckDs = df2
				.select(
						expr("to_timestamp(substring(`交易时间`,2), 'yyyy-MM-dd HH:mm:ss')").as("time"),
						expr("ifnull(substring(`商户订单号`,2),'')").as("we_订单号"),
						expr("ifnull(-floor(round(substring(`订单金额`,2)*100,0)),0)").as("订单金额"),
						expr("ifnull(floor(round(substring(`手续费`,2)*100,0)),0)").as("手续费"),
						expr("ifnull(floor(round(substring(`退款金额`,2)*100,0)),0)").as("退款")
				);

		// 微信账单也取和本地差异部分时间相对应的部分
		wechatCheckDs = wechatCheckDs.where(String.format("time >= '%s' and time < '%s'", startTime, endTime));
		// wechatCheckDs.where("`we_订单号` = '20191002152505905002566714'").show(false);
		wechatCheckDs = wechatCheckDs.withColumn("金额", col("`订单金额`").plus(col("`手续费`")).plus(col("`退款`")));

		// 本地数据和微信数据先分别对订单号分组求和
		localCheckDs = localCheckDs.groupBy("`订单号`")
				.agg(sum("`金额`").as("金额"), max("`账户`").as("账户"), max("time").as("time"));
		wechatCheckDs = wechatCheckDs.groupBy("`we_订单号`")
				.agg(sum("`金额`").as("we_金额"), max("time").as("we_time"));

		// 聚合本地订单和微信订单
		// Dataset<Row> totalDs = localCheckDs.union(wechatCheckDs);
		Dataset<Row> totalDs = localCheckDs.as("s").join(
				wechatCheckDs.as("t"),
				expr("s.`订单号` = t.`we_订单号`"),
				"outer"
		);

		totalDs.orderBy(desc("we_time")).show();
		// 对账户进行开窗, 只留下微信中有订单号的对应账户
		totalDs = totalDs
				.withColumn("flag", expr("if(`we_订单号` is null, 0, 1)"))
				.withColumn("account_num", expr("sum(flag) over (partition by `账户`)"))
				.where("account_num > 0")
		;

		// 相加结果不为0的即为有差异的
		Dataset<Row> currCheckDs = totalDs
				.withColumn("res", col("`金额`").plus(col("`we_金额`")))
				.where("res != 0")
				.select(
						expr("`订单号`"),
						expr("res").as("金额"),
						expr("time")
				);

		currCheckDs.orderBy(desc("time")).show(false);
		// 上次对账差异
		Dataset<Row> lastCheckDs = spark.read().format("delta").load(check_bill_diff);

		// 合并两次的差异结果, 但是要先把上次对账结果中, 大于本次开始对账时间的部分去掉, 再合并
		Dataset<Row> result;
		if (lastCheckDs.schema().isEmpty()) {
			result = currCheckDs;
		} else {
			lastCheckDs = lastCheckDs.where(String.format("time <= '%s'", startTime));
			result = lastCheckDs.union(currCheckDs);
			// 对本次和上次的差异再次分组求和
			result = result.groupBy(expr("`订单号`"))
					.agg(sum("`金额`").as("金额"), max("time").as("time"))
					.filter((FilterFunction<Row>) row -> !Objects.equals(row.getAs("金额"), 0L));
		}

		// result.where("`订单号` = '20190916104101624005124955'").show(false);
		// result.orderBy(expr("`订单号`")).show(false);
		metaTs.put("check", String.valueOf(now));
		// 存储本次 +上次的总差异
		result.write().format("delta")
				.mode("overwrite")
				.save(check_bill_diff);

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

	// 取微信文件上次对账后, 最早修改时间文件对应的时间
	private static Optional<LocalDate> getWeChatLastUpdate(String path, Instant ts) throws IOException {
		List<LocalDate> results = Lists.newArrayList();
		Path wechatPath = new Path(path);

		FileSystem fs = FileSystem.get(wechatPath.toUri(), SparkSession.active().sparkContext().hadoopConfiguration());
		FileStatus[] files = fs.listStatus(wechatPath);

		for (FileStatus file : files) {
			if (file.getPath().toString().endsWith(".crc")) {
				continue;
			}
			// 留下csv文件

			if (ts.compareTo(Instant.ofEpochMilli(file.getModificationTime())) < 0) {
				String fileName = file.getPath().getName();
				int idx1 = fileName.indexOf("All") + 3;
				String fileDate = fileName.substring(idx1, idx1 + 10);
				results.add(LocalDate.parse(fileDate));
			}
		}

		return results.stream().min(LocalDate::compareTo);
	}

}