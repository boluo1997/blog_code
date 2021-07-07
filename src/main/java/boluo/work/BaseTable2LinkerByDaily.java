package boluo.work;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

public class BaseTable2LinkerByDaily {

	public static void main(String[] args) throws Exception {

		SparkSession spark = SparkSession.builder()
				.getOrCreate();

		CommandLine cli = new GnuParser().parse(new Options()
				.addOption("i", "input", true, "")
				.addOption("o", "output", true, ""), args);
		// 参数
		String inPath = cli.getOptionValue("i");
		String outPath = cli.getOptionValue("o");

		/**
		 * 1.取当前时间
		 */
		Instant now = Instant.now();

		/**
		 * 2.meta ts
		 */
		Map<String, String> metaTs = Maps.newHashMap(); // Outputs.meta(outPath);
		Instant lastTs = Instant.parse(metaTs.getOrDefault("last", "2000-01-01T00:00:00Z"));

		/**
		 * 3.读取 linked hash && hash = hash - 1
		 *
		 */
		int hash = 0;
		if (!metaTs.isEmpty()) {
			String linkedHash = My.getLeadPartition(inPath, "hash", lastTs);
			if (Strings.isNullOrEmpty(linkedHash)) {
				return;
			} else {
				hash = Integer.parseInt(linkedHash) - 1;
			}
		}
		long hash_unix_timestamp = (hash + 1) * 1000000000L;
		LocalDate hashDate = LocalDateTime.ofInstant(Instant.ofEpochMilli(hash_unix_timestamp), ZoneId.systemDefault()).toLocalDate();
		/**
		 * 4.读取 linked where hash = ? && date >= hash_date
		 *   按日分组统计, (舍弃第三方流水号, 按其余分组)
		 */

		Dataset<Row> df = spark.read().format("delta").load(inPath);
		Dataset<Row> res = df.where("hash >= " + hash)
				.where("payment is not null")
				.withColumn("payment", explode(expr("payment")))
				.withColumn("date", date_format(expr("payment.`时间`"), "yyyy-MM-dd"))
				.groupBy(expr("date"), expr("payment.`科目`"), expr("payment.`收款方`"), expr("payment.`账户`"), expr("biz"), expr("store"))
				.agg(sum(expr("payment.`金额`")).as("金额"), count("payment.`金额`").as("笔数"))
				.where("date >= '" + hashDate + "'")
				.orderBy(expr("date"));

		/**
		 * 5.overwrite
		 * 6.存储 meta ts -> now ts
		 */
		res.write().format("delta")
				.mode("overwrite")
				.partitionBy("date")
				.option("replaceWhere", "date>='" + hashDate + "'")
				.save(outPath);

		metaTs.put("last", now.toString());
		// Outputs.meta(outPath, metaTs);

	}

}
