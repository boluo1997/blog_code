package boluo.work;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.CharMatcher;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.compress.utils.Charsets;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.*;

/**
 *	主要思想:
 *	将数据分为 全量 和 差异 两部分
 * 	每次拉取数据, 取当前的全量, 和上一次的全量做比较, 比较出今天的差异部分数据
 * 	然后将本次全量, 本次差异存储, 将上一次的全量删除
 */
public class FromHaierDevice {

	private static final Logger logger = LoggerFactory.getLogger(FromHaier2.class);
	private static final ObjectMapper mapper = new ObjectMapper();
	private static final CloseableHttpClient http;
	private static final String pageMatcher = "\\s*共(\\d+)条记录\\s*当前页\\s*(\\d+)/(\\d+)";
	private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

	static {
		PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
		cm.setMaxTotal(3);
		cm.setDefaultMaxPerRoute(3);
		http = HttpClients.custom()
				.setConnectionManager(cm)
				.build();
	}

	/*
-user=
-pwd=
-o=file:///D:/data/haier_device
	 */
	public static void main(String[] args) throws ParseException, IOException {

		SparkSession spark = SparkSession.builder()
				.getOrCreate();

		CommandLine cli = new GnuParser().parse(new Options()
				.addOption("o", "output", true, "")
				.addOption("user", "user", true, "")
				.addOption("pwd", "pwd", true, ""), args);

		String outputUri = cli.getOptionValue("o");
		String user = cli.getOptionValue("user", "18557523125");
		String pwd = cli.getOptionValue("pwd", "xl123456");
		String loginData = String.format("j_username=%s&j_password=%s", user, pwd);

		LocalDateTime now = LocalDateTime.now();

		// 调用: 商家信息管理, 取商家名称
		HttpClientContext httpContext = HttpClientContext.create();
		login(loginData, httpContext);

		ObjectNode merchantInfo = getMerchantInfo(httpContext);
		String merchantId = merchantInfo.at("/商家ID").asText();
		String merchantName = merchantInfo.at("/商家名称").asText();

		// 调用: 机器使用次数
		Dataset<Row> deviceDs = machineUsage(httpContext, merchantId, now.toLocalDate().minusDays(1));

		// 调用: 设备管理		空闲:1	运行:3	故障:4	游离:5	掉线:6	维护:7	上线:8
		Dataset<Row> currDeviceDs = deviceManage(httpContext, merchantId, String.valueOf(1));
		String[] status = new String[]{"3", "4", "6", "7", "8"};
		for (String s : status) {
			Dataset<Row> otherDeviceManage = deviceManage(httpContext, merchantId, s);
			currDeviceDs = otherDeviceManage.schema().isEmpty() ? currDeviceDs : currDeviceDs.union(otherDeviceManage);
		}

		// 取设备管理接口中有, 机器使用次数接口中没有的数据
		List<Row> otherDevices = currDeviceDs.selectExpr("key", "`设备识别码`").as("a").join(
				deviceDs.as("b"),
				expr("a.`设备识别码` = b.`设备识别码`"),
				"leftanti"
		).collectAsList().stream()
				.map(row -> {
					String key = row.getAs("key");
					String deviceNo = row.getAs("设备识别码");
					String deviceType = getDeviceType(httpContext, key);
					return RowFactory.create(deviceNo, deviceType);
				}).collect(Collectors.toList());

		// 机器使用次数接口中取不到设备型号的设备信息
		Dataset<Row> otherDeviceDs = spark.createDataFrame(otherDevices, new StructType()
				.add("设备识别码", "string")
				.add("设备型号", "string"));

		currDeviceDs = currDeviceDs.as("a").join(
				deviceDs.union(otherDeviceDs).as("b"),
				JavaConverters.asScalaBufferConverter(Collections.singletonList("设备识别码")).asScala(),
				"left"
		);

		// TODO 取上一次修改时间的分区
		Dataset<Row> timeRow = getPartition(outputUri, null, null);
		Date hashDate = Date.valueOf("2020-01-01");

		// 数据查询: 本地海尔设备表
		Dataset<Row> lastAllDeviceDs = spark.read().format("delta").load(outputUri).cache();

		Dataset<Row> allDeviceDs;
		if (lastAllDeviceDs.schema().isEmpty()) {
			allDeviceDs = currDeviceDs.withColumn("flag", expr("0"));
		} else {

			Dataset<Row> lastDeviceDs = lastAllDeviceDs.where("flag = 0");
			Dataset<Row> lastUpdateDs = lastAllDeviceDs.where("flag = 1");

			// 本次修改, 删除, 添加的数据

		}


	}

	private static void login(String loginData, HttpClientContext httpContext) throws IOException {
		HttpUriRequest req = RequestBuilder.post()
				.setUri("http://www.saywash.com/saywash/WashCallManager/j_spring_security_check")
				.setHeader("Content-Type", "application/x-www-form-urlencoded")
				.setHeader("Referer", "http://www.saywash.com/saywash/WashCallManager/login/login.do")
				.setHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36 Edg/80.0.361.69")
				.setEntity(new StringEntity(loginData, Charsets.UTF_8))
				.build();
		try (CloseableHttpResponse response = http.execute(req, httpContext)) {
			logger.info("{}", req.getURI());
			Preconditions.checkArgument(response.getStatusLine().getStatusCode() == 302, response);
			Header[] locations = response.getHeaders("Location");
			Preconditions.checkArgument(locations.length == 1 && !locations[0].getValue().contains("Failed"), response);
		}
	}

	private static Document executor(HttpUriRequest req, HttpClientContext httpContext, String body) {
		try (CloseableHttpResponse response = http.execute(req, httpContext)) {
			logger.info("{}, args: {}", req.getURI(), body);
			Preconditions.checkArgument(response.getStatusLine().getStatusCode() == 200, response);
			Thread.sleep(100);
			try (InputStream is = response.getEntity().getContent()) {
				return Jsoup.parse(is, "utf-8", "");
			}
		} catch (IOException | InterruptedException e) {
			throw new RuntimeException();
		}
	}

	private static ObjectNode getMerchantInfo(HttpClientContext httpContext) {

		String url = "http://www.saywash.com/saywash/WashCallManager/merchant/merchant/search.do";
		String args = "";

		Document d1 = getMerchantInfoRequest(httpContext, url, args);

		List<ObjectNode> result = Stream.of(d1).flatMap(doc -> {

			Element head = doc.selectFirst("table>thead tr");
			List<String> colName = head.select("td").stream()
					.map(i -> i.text().trim())
					.collect(Collectors.toList());

			Elements body = doc.select("table>tbody tr");
			return body.stream()
					.map(tr -> {
						Elements td = tr.select("td");
						Preconditions.checkArgument(td.size() == colName.size(), td);
						ObjectNode r = mapper.createObjectNode();
						for (int i = 0; i < colName.size(); i++) {
							r.put(colName.get(i), td.get(i).text());
						}
						return r;
					});
		}).collect(Collectors.toList());

		Preconditions.checkArgument(result.size() == 1, "该账号下有0个或多个商户信息");
		return result.get(0);
	}

	private static Document getMerchantInfoRequest(HttpClientContext httpContext, String url, String body) {

		HttpUriRequest req = RequestBuilder.post()
				.setUri(url)
				.setHeader("Referer", "http://www.saywash.com/saywash/WashCallManager/merchant/merchant/index.do?meun=3")
				.setHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36")
				.setEntity(new StringEntity(body, Charsets.UTF_8))
				.build();

		return executor(req, httpContext, body);
	}

	private static Dataset<Row> machineUsage(HttpClientContext httpContext, String merchantId, LocalDate date) {

		DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy/MM/dd");
		String url = "http://www.saywash.com/saywash/WashCallManager/report/deviceuseCount/search.do";
		String args = "type=3&provinceId=&cityId=&districtId=&laundryUnit=&laundryName=" +
				"&merchantId=" + merchantId +
				"&faultStartTime=" + date.format(dateTimeFormatter) +
				"&faultEndTime=" + date.format(dateTimeFormatter) +
				"&deviceTypeId=";

		Document d1 = machineUsageRequest(httpContext, url, args);
		Element pager = d1.selectFirst(".paging_container>div");
		if (Objects.isNull(pager)) {
			Preconditions.checkArgument(d1.select("table>tbody").isEmpty(), "机器使用次数接口页数显示与真实数量不一致");
		}

		Matcher matcher = Pattern.compile(pageMatcher).matcher(pager.text());
		Preconditions.checkArgument(matcher.matches(), "分页提取失败!");

		int total = Integer.parseInt(matcher.group(1));
		int curr = Integer.parseInt(matcher.group(2));
		int last = Integer.parseInt(matcher.group(3));

		List<ObjectNode> result = Stream.concat(Stream.of(d1),
				Stream.iterate(1, i -> i + 1).limit(last - 1)
						.map(i -> machineUsageRequest(httpContext, url, args + "&pageIndex=" + i)))
				.flatMap(doc -> {
					Element head = doc.selectFirst("table>thead tr");
					List<String> colName = head.select("td").stream()
							.map(i -> i.text().trim())
							.collect(Collectors.toList());

					Elements body = doc.select("table>tbody tr");
					return body.stream()
							.map(tr -> {
								Elements td = tr.select("td");
								Preconditions.checkArgument(td.size() == colName.size(), td);
								ObjectNode r = mapper.createObjectNode();
								for (int i = 0; i < colName.size(); i++) {
									r.put(colName.get(i), td.get(i).text());
								}
								return r;
							});
				}).collect(Collectors.toList());

		Preconditions.checkArgument(result.size() == total, "接口返回数量与显示数量不一致, 接口数量: " + result.size());
		Dataset<String> dsJson = SparkSession.active()
				.createDataset(result, Encoders.kryo(ObjectNode.class))
				.map((MapFunction<ObjectNode, String>) mapper::writeValueAsString, Encoders.STRING());

		return SparkSession.active().read().json(dsJson)
				.select(
						expr("`设备识别码`"),
						expr("`设备编号`").as("设备型号")
				)
				.coalesce(1);
	}

	private static Document machineUsageRequest(HttpClientContext httpContext, String url, String body) {

		HttpUriRequest req = RequestBuilder.post()
				.setUri(url)
				.setHeader("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
				.setHeader("Referer", "http://www.saywash.com/saywash/WashCallManager/report/device/index.do?meun=1")
				.setHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36")
				.setEntity(new StringEntity(body, Charsets.UTF_8))
				.build();

		return executor(req, httpContext, body);
	}

	private static Dataset<Row> deviceManage(HttpClientContext httpContext, String merchantId, String statusId) {

		String url = "http://www.saywash.com/saywash/WashCallManager/device/device/search.do";
		String args = "laundryInfo=&deviceId=&qrCode=&categoryId=" +
				"&merchantId=" + merchantId +
				"&statusId=" + statusId +
				"&iccid=&moduleType=&autoCoupon=&refund=&reservest=";

		Document d1 = deviceManageRequest(httpContext, url, args);
		Element pager = d1.selectFirst(".paging_container>div");
		if (Objects.isNull(pager)) {
			Preconditions.checkArgument(d1.select("table>tbody").isEmpty(), "设备管理接口页数显示与真实数量不一致");
		}

		Matcher matcher = Pattern.compile(pageMatcher).matcher(pager.text());
		Preconditions.checkArgument(matcher.matches(), "分页提取失败!");

		int total = Integer.parseInt(matcher.group(1));
		int curr = Integer.parseInt(matcher.group(2));
		int last = Integer.parseInt(matcher.group(3)) == 0
				? 1
				: Integer.parseInt(matcher.group(3));

		List<ObjectNode> result = Stream.concat(Stream.of(d1),
				Stream.iterate(1, i -> i + 1).limit(last - 1)
						.map(i -> deviceManageRequest(httpContext, url, args + "&pageIndex=" + i)))
				.flatMap(doc -> {

					Element head = doc.selectFirst("table>thead tr");
					List<String> colName = head.select("td:not([style=display:none;])").stream()
							.map(i -> i.text().trim())
							.collect(Collectors.toList());

					Elements body = doc.select("table>tbody tr");
					return body.stream()
							.map(tr -> {
								String key = tr.attr("data-key");
								Elements td = tr.select("td");
								Preconditions.checkArgument(td.size() == colName.size(), td);
								Preconditions.checkArgument(td.size() == 14, "设备管理列数不为14列, 列数为: " + td.size());
								ObjectNode r = mapper.createObjectNode()
										.put("key", key);
								for (int i = 0; i < colName.size(); i++) {
									r.put(colName.get(i), td.get(i).text());
								}

								// 故障返券, 预约模式, 退款开关等三个开关按钮
								Boolean faultRebate = null;
								if (td.get(10).select("span.switch-on").size() > 0) {
									faultRebate = true;
								} else if (td.get(10).select("span.switch-off").size() > 0) {
									faultRebate = false;
								}

								Boolean appointment = td.get(11).select("span.switch-on").isEmpty()
										? td.get(11).select("span.switch-off").isEmpty()
										? null
										: (Boolean) false
										: (Boolean) true;

								Boolean refund = td.get(12).select("span.switch-on").isEmpty()
										? td.get(12).select("span.switch-off").isEmpty()
										? null
										: (Boolean) false
										: (Boolean) true;

								r.put("故障返券", faultRebate);
								r.put("预约模式", appointment);
								r.put("退款开关", refund);

								return r;
							});
				}).collect(Collectors.toList());

		// Preconditions.checkArgument(result.size() == total, "接口返回数量与分页显示数量不一致, 接口数量: " + result.size());
		Dataset<String> dsJson = SparkSession.active()
				.createDataset(result, Encoders.kryo(ObjectNode.class))
				.map((MapFunction<ObjectNode, String>) mapper::writeValueAsString, Encoders.STRING());

		return SparkSession.active().read().json(dsJson)
				.drop("操作")
				.coalesce(1);

	}

	private static Document deviceManageRequest(HttpClientContext httpContext, String url, String body) {

		HttpUriRequest req = RequestBuilder.post()
				.setUri(url)
				.setHeader("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
				.setHeader("Referer", "http://www.saywash.com/saywash/WashCallManager/device/device/index.do?meun=6")
				.setHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36")
				.setEntity(new StringEntity(body, Charsets.UTF_8))
				.build();

		return executor(req, httpContext, body);
	}

	private static String getDeviceType(HttpClientContext httpContext, String deviceId) {

		String url = "http://www.saywash.com/saywash/WashCallManager/device/device/edit.do";
		String args = "deviceId=" + deviceId;

		HttpUriRequest req = RequestBuilder.post()
				.setUri(url)
				.setHeader("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
				.setHeader("Referer", "http://www.saywash.com/saywash/WashCallManager/device/device/index.do?meun=6")
				.setHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36")
				.setEntity(new StringEntity(args, Charsets.UTF_8))
				.build();

		Document d1 = executor(req, httpContext, args);
		Elements body = d1.select("table>tbody>tr:eq(2) td");
		return body.text();
	}

	private static Dataset<Row> getPartition(String path, Instant min, Instant max) throws IOException {

		min = MoreObjects.firstNonNull(min, Timestamp.valueOf(LocalDateTime.of(2000, 1, 1, 0, 0)).toInstant());
		max = MoreObjects.firstNonNull(max, Timestamp.valueOf(LocalDateTime.of(2100, 1, 1, 0, 0)).toInstant());

		Path parentPath = new Path(path);
		FileSystem fs = FileSystem.get(parentPath.toUri(), SparkSession.active().sparkContext().hadoopConfiguration());
		fs.mkdirs(parentPath);

		Dataset<Row> pathDs = SparkSession.active().read().format("delta").load(path);
		Pattern pattern = Pattern.compile("\\S+=\\S+");

		// 是否分区及各分区字段
		LogicalRelation logicalPlan = (LogicalRelation) pathDs.logicalPlan();
		HadoopFsRelation fr = (HadoopFsRelation) logicalPlan.relation();
		StructType types = fr.partitionSchema();

		String[] partitionName = types.fieldNames();
		StructType structType = new StructType();

		for (String name : partitionName) {
			structType = structType.add(name, "string");
		}
		structType = structType.add("time", "timestamp");
		List<Row> results = Lists.newArrayList();

		// 有效文件路径
		String[] paths = pathDs.inputFiles();
		for (String s : paths) {

			Object[] values = new Object[structType.size()];
			FileStatus file = fs.getFileStatus(new Path(s));
			long ins = file.getModificationTime();

			Iterator<String> it = Arrays.stream(String.valueOf(file.getPath().getParent()).split("/"))
					.filter(i -> pattern.matcher(i).matches()).iterator();

			for (int i = 0; i < partitionName.length; i++) {
				String value = it.next();
				value = value.substring(value.indexOf("=") + 1);
				values[i] = value;
			}
			values[values.length - 1] = new Timestamp(ins);
			results.add(RowFactory.create(values));
		}

		Dataset<Row> resDs = SparkSession.active().createDataFrame(results, structType);
		for (int i = 0; i < types.size(); i++) {
			String name = types.apply(i).name();
			String type = types.apply(i).dataType().typeName();
			resDs = resDs.withColumn(name, expr(String.format("cast(`%s` as %s)", name, type)));
		}

		Column[] cs = Arrays.stream(partitionName).map(functions::col).toArray(Column[]::new);
		return resDs.where(String.format("time >= '%s' and time < '%s'",
				LocalDateTime.ofInstant(min, ZoneId.systemDefault()).format(formatter),
				LocalDateTime.ofInstant(max, ZoneId.systemDefault()).format(formatter)))
				.groupBy(cs)
				.agg(expr("max(time)").as("max_time"), expr("min(time)").as("min_time"))
				.where("max_time is not null and min_time is not null")
				;

	}

}
