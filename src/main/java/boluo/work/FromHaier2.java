package boluo.work;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.CharMatcher;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.*;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.compress.utils.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.*;
import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.*;

public class FromHaier2 {

	private static final Logger logger = LoggerFactory.getLogger(FromHaier2.class);
	private static final ObjectMapper mapper = new ObjectMapper();
	private static final CloseableHttpClient http;
	private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
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
-user=188
-pwd=666
-o=file:///D:/data/haier2
	 */
	public static void main(String[] args) throws Exception {

		SparkSession spark = SparkSession.builder()
				.getOrCreate();

		CommandLine cli = new GnuParser().parse(new Options()
				.addOption("o", "output", true, "")
				.addOption("user", "user", true, "")
				.addOption("pwd", "pwd", true, ""), args);

		String outputUri = cli.getOptionValue("o");
		String user = cli.getOptionValue("user", "188");
		String pwd = cli.getOptionValue("pwd", "666");
		String loginData = String.format("j_username=%s&j_password=%s", user, pwd);
		outputUri = CharMatcher.anyOf("/").trimTrailingFrom(outputUri) + "/order";

		Path path = new Path(outputUri);
		FileSystem fs = FileSystem.get(path.toUri(), spark.sparkContext().hadoopConfiguration());
		fs.mkdirs(path);

		// 调用接口: 商家信息管理, 取商家名称
		HttpClientContext httpContext = HttpClientContext.create();
		login(loginData, httpContext);
		String merchantName = getMerchantName(httpContext);

		// 上次存储的结束时间
		Dataset<Row> timeRow = getPartition(outputUri, null, null);

		Date date = timeRow.isEmpty()
				? Date.valueOf("2020-01-01")
				: timeRow.where(String.format("`商家名称` = '%s'", merchantName))
				.select(expr("ifnull(max(date),to_date('2020-01-01'))")).first().getAs(0);

		Dataset<Row> haier2 = spark.read().format("delta").load(outputUri);
		LocalDateTime max_u_ts = haier2.schema().isEmpty()
				? LocalDateTime.of(2020, 1, 1, 0, 0)
				: haier2.where(String.format("date>='%s' and `商家名称` = '%s'", date, merchantName))
				.select(expr("ifnull(max(u_ts),to_date('2020-01-01'))"))
				.first()
				.<Timestamp>getAs(0).toLocalDateTime();

		LocalDateTime now = LocalDateTime.now();
		LocalDate nowDate = now.toLocalDate();
		LocalDateTime curr = max_u_ts;

		while (curr.compareTo(now) < 0) {

			curr = Stream.of(curr.toLocalDate().plusDays(1).atStartOfDay(), now).min(LocalDateTime::compareTo).get();
			Long insert_u_ts = curr.atZone(ZoneId.systemDefault()).toEpochSecond();
			Stream<ObjectNode> str = Stream.of();

			if (curr.compareTo(nowDate.minusDays(1).atStartOfDay()) < 0) {
				// 历史接口, 前两天数据
				for (LocalDate start = curr.toLocalDate().minusDays(2); start.compareTo(curr.toLocalDate()) < 0; start = start.plusDays(1)) {
					str = Stream.concat(str, historySearch(httpContext, start).stream());
				}
			} else {
				// 实时接口, 前两天数据
				for (LocalDate start = curr.toLocalDate().minusDays(2); start.compareTo(curr.toLocalDate()) < 0; start = start.plusDays(1)) {
					str = Stream.concat(str, currSearch(httpContext, start).stream());
				}

				// 当天数据
				if (curr.isEqual(now)) {
					str = Stream.concat(str, currSearch(httpContext, curr.toLocalDate()).stream());
				}
			}

			List<ObjectNode> resultOrderList = str.collect(Collectors.toList());
			if (Iterables.isEmpty(resultOrderList)) {
				logger.info("skip {}", curr);
				continue;
			}
			Dataset<String> dsJson = spark
					.createDataset(resultOrderList, Encoders.kryo(ObjectNode.class))
					.map((MapFunction<ObjectNode, String>) mapper::writeValueAsString, Encoders.STRING());

			Dataset<Row> ds = spark.read().json(dsJson)
					.withColumn("u_ts", to_timestamp(from_unixtime(expr(String.valueOf(insert_u_ts)))))
					.withColumn("date", to_date(col("u_ts")))
					.coalesce(1);

			ds.write()
					.format("delta")
					.partitionBy("date", "商家名称")
					.mode("overwrite")
					.option("replaceWhere", String.format("date = '%s' and `商家名称` = '%s'", curr.toLocalDate(), merchantName))
					.save(outputUri);

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

	private static String getMerchantName(HttpClientContext httpContext) {

		String url = "http://www.saywash.com/saywash/WashCallManager/merchant/merchant/search.do";
		String body = "";

		HttpUriRequest req = RequestBuilder.post()
				.setUri(url)
				.setHeader("Referer", "http://www.saywash.com/saywash/WashCallManager/merchant/merchant/index.do?meun=3")
				.setHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36")
				.setEntity(new StringEntity(body, Charsets.UTF_8))
				.build();

		try (CloseableHttpResponse response = http.execute(req, httpContext)) {
			logger.info("{}, args: {}", req.getURI(), body);
			Preconditions.checkArgument(response.getStatusLine().getStatusCode() == 200, response);
			Thread.sleep(100);
			try (InputStream is = response.getEntity().getContent()) {
				Document doc = Jsoup.parse(is, "utf-8", "");
				Elements names = doc.select("table>tbody>tr");
				Preconditions.checkArgument(names.size() == 1, "该账户中有多个商家名称");
				return names.select("td.detail").first().text();
			}
		} catch (IOException | InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	private static List<ObjectNode> historySearch(HttpClientContext httpContext, LocalDate date) throws IOException {
		DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy/MM/dd");
		String url = "http://www.saywash.com/saywash/WashCallManager/merchant/order/historyorder/search.do";
		String args = "type=1&merchantId=3914&orderCode=&phone=&status=&payTypeId=" +
				"&years=" + date.getYear() +
				"&startDate=" + date.format(dateTimeFormatter) +
				"&endDate=" + date.format(dateTimeFormatter) +
				"&couponType=&orderesource=";

		Document d1 = historyRequest(httpContext, url, args);

		Element pager = d1.selectFirst(".paging_container>div");
		if (Objects.isNull(pager)) {
			Preconditions.checkArgument(d1.select("table>tbody").isEmpty());
			return ImmutableList.of();
		}
		Matcher matcher = Pattern.compile("\\s*共(\\d+)条记录\\s*当前页\\s*(\\d+)/(\\d+)").matcher(pager.text());
		Preconditions.checkArgument(matcher.matches(), "提取分页失败");
		int total = Integer.parseInt(matcher.group(1));
		int curr = Integer.parseInt(matcher.group(2));
		int last = Integer.parseInt(matcher.group(3));

		List<ObjectNode> res = Stream.concat(Stream.of(d1),
				Stream.iterate(1, i -> i + 1).limit(last - 1)
						.map(i -> historyRequest(httpContext, url, args + "&pageIndex=" + i))
		).flatMap(doc -> {
			Element head = doc.selectFirst("table>thead tr");
			List<String> colName = head.select("td").stream()
					.map(i -> i.text().trim())
					.collect(Collectors.toList());
			Elements body = doc.select("table>tbody tr");
			return body.stream()
					.map(tr -> {
						String key = tr.attr("data-key");
						Elements td = tr.select("td");
						Preconditions.checkArgument(td.size() == colName.size(), td);
						ObjectNode r = mapper.createObjectNode()
								.put("key", key);
						for (int i = 0; i < colName.size(); ++i) {
							r.put(formatKey(colName.get(i)), td.get(i).text());
						}
						return r;
					});
		}).peek(i -> {
			LocalDateTime datetime = formatDatetime(i.at("/订单时间").textValue());
			ObjectNode detail = getHistoryOrderDetail(httpContext, i.at("/key").textValue(), datetime.getYear());
			i.setAll(detail);
			LocalDateTime updateTime = formatDatetime(i.at("/最新状态时间").textValue());
			if (Objects.nonNull(updateTime)) {
				i.put("最新状态时间", Timestamp.valueOf(updateTime).toString());
			}
			i.put("订单时间", Timestamp.valueOf(datetime).toString());
			i.remove("序号");
		}).collect(Collectors.toList());
		Preconditions.checkArgument(res.size() == total, "总数检查失败");
		return res;
	}

	private static List<ObjectNode> currSearch(HttpClientContext httpContext, LocalDate date) {
		DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy/MM/dd");
		String url = "http://www.saywash.com/saywash/WashCallManager/merchant/order/search.do";
		String args = "type=1&merchantId=3914&orderCode=&phone=&status=&payTypeId=&couponType=" +
				"&startDate=" + date.format(dateTimeFormatter) +
				"&endDate=" + date.format(dateTimeFormatter) +
				"&orderesource=";

		Document d1 = currRequest(httpContext, url, args);

		Element pager = d1.selectFirst(".paging_container>div");
		if (Objects.isNull(pager)) {
			Preconditions.checkArgument(d1.select("table>tbody").isEmpty());
			return ImmutableList.of();
		}
		Matcher matcher = Pattern.compile("\\s*共(\\d+)条记录\\s*当前页\\s*(\\d+)/(\\d+)").matcher(pager.text());
		Preconditions.checkArgument(matcher.matches(), "提取分页失败");
		int total = Integer.parseInt(matcher.group(1));
		int curr = Integer.parseInt(matcher.group(2));
		int last = Integer.parseInt(matcher.group(3));

		List<ObjectNode> res = Stream.concat(Stream.of(d1),
				Stream.iterate(1, i -> i + 1).limit(last - 1)
						.map(i -> currRequest(httpContext, url, args + "&pageIndex=" + i))
		).flatMap(doc -> {
			Element head = doc.selectFirst("table>thead tr");
			List<String> colName = head.select("td").stream()
					.map(i -> i.text().trim())
					.collect(Collectors.toList());
			Elements body = doc.select("table>tbody tr");
			return body.stream()
					.map(tr -> {
						String key = tr.attr("data-key");
						Elements td = tr.select("td");
						Preconditions.checkArgument(td.size() == colName.size(), td);
						ObjectNode r = mapper.createObjectNode()
								.put("key", key);
						for (int i = 0; i < colName.size(); ++i) {
							r.put(formatKey(colName.get(i)), td.get(i).text());
						}
						return r;
					});
		}).peek(i -> {
			LocalDateTime datetime = formatDatetime(i.at("/订单时间").textValue());
			ObjectNode detail = getOrderDetail(httpContext, i.at("/key").textValue());
			i.setAll(detail);
			LocalDateTime updateTime = formatDatetime(i.at("/最新状态时间").textValue());
			if (Objects.nonNull(updateTime)) {
				i.put("最新状态时间", Timestamp.valueOf(updateTime).toString());
			}
			i.put("订单时间", Timestamp.valueOf(datetime).toString());
			i.remove("序号");
			i.remove("操作");
		}).collect(Collectors.toList());
		Preconditions.checkArgument(res.size() == total, "总数检查失败");
		return res;
	}

	private static ObjectNode getHistoryOrderDetail(HttpClientContext httpContext, String key, int years) {
		String url = "http://www.saywash.com/saywash/WashCallManager/merchant/order/getHistoryOrderDetail.do";
		String args = String.format("orderId=%s&isOrderSearch=0&years=%d", key, years);
		Document doc = historyRequest(httpContext, url, args);
		ObjectNode res = mapper.createObjectNode();
		Elements trList = doc.select("tbody>tr");
		trList.forEach(tr -> {
			Elements title = tr.select("th label");
			Elements value = tr.select("td");
			res.put(formatKey(title.text()), value.text());
		});
		Preconditions.checkArgument(res.size() > 0, "读取history detail失败");
		return res;
	}

	private static ObjectNode getOrderDetail(HttpClientContext httpContext, String key) {
		String url = "http://www.saywash.com/saywash/WashCallManager/merchant/order/getOrderDetail.do";
		String args = String.format("orderId=%s&isOrderSearch=0", key);
		Document doc = currRequest(httpContext, url, args);
		ObjectNode res = mapper.createObjectNode();
		Elements trList = doc.select("tbody>tr");
		trList.forEach(tr -> {
			Elements title = tr.select("th label");
			Elements value = tr.select("td");
			res.put(formatKey(title.text()), value.text());
		});
		Preconditions.checkArgument(res.size() > 0, "读取detail失败");
		return res;
	}

	private static Document historyRequest(HttpClientContext httpContext, String url, String body) {
		HttpUriRequest req = RequestBuilder.post()
				.setUri(url)
				.setHeader("Content-Type", "application/x-www-form-urlencoded")
				.setHeader("Referer", "http://www.saywash.com/saywash/WashCallManager/merchant/order/historyorder/index.do")
				.setHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36 Edg/80.0.361.69")
				.setEntity(new StringEntity(body, Charsets.UTF_8))
				.build();
		try (CloseableHttpResponse response = http.execute(req, httpContext)) {
			logger.info("{}, args: {}", req.getURI(), body);
			Preconditions.checkArgument(response.getStatusLine().getStatusCode() == 200, response);
			Thread.sleep(100);
			try (InputStream is = response.getEntity().getContent()) {
				return Jsoup.parse(is, "utf-8", "");
			}
		} catch (IOException | InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	private static Document currRequest(HttpClientContext httpContext, String url, String body) {
		HttpUriRequest req = RequestBuilder.post()
				.setUri(url)
				.setHeader("Content-Type", "application/x-www-form-urlencoded")
				.setHeader("Referer", "http://www.saywash.com/saywash/WashCallManager/merchant/order/index.do")
				.setHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36 Edg/80.0.361.69")
				.setEntity(new StringEntity(body, Charsets.UTF_8))
				.build();
		try (CloseableHttpResponse response = http.execute(req, httpContext)) {
			logger.info("{}, args: {}", req.getURI(), body);
			Preconditions.checkArgument(response.getStatusLine().getStatusCode() == 200, response);
			Thread.sleep(100);
			try (InputStream is = response.getEntity().getContent()) {
				return Jsoup.parse(is, "utf-8", "");
			}
		} catch (IOException | InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	private static LocalDateTime formatDatetime(String str) {
		try {
			return LocalDateTime.parse(str, dateTimeFormatter);
		} catch (DateTimeParseException ex) {
			return null;
		}
	}

	private static String formatKey(String str) {
		return str.replaceAll("\\(元\\)$", "");
	}

	private static Dataset<Row> getPartition(String path, Instant min, Instant max) throws IOException {

		min = MoreObjects.firstNonNull(min, Timestamp.valueOf(LocalDateTime.of(2000, 1, 1, 0, 0)).toInstant());
		max = MoreObjects.firstNonNull(max, Timestamp.valueOf(LocalDateTime.of(2100, 1, 1, 0, 0)).toInstant());

		Dataset<Row> pathDs = SparkSession.active().read().format("delta").load(path);
		Pattern pattern = Pattern.compile("\\S+=\\S+");

		Path parentPath = new Path(path);
		FileSystem fs = FileSystem.get(parentPath.toUri(), SparkSession.active().sparkContext().hadoopConfiguration());

		// 是否分区及各分区字段
		LogicalRelation logicalPlan = (LogicalRelation) pathDs.logicalPlan();
		HadoopFsRelation fr = (HadoopFsRelation) logicalPlan.relation();
		StructType types = fr.partitionSchema();

		String[] partitionName = types.fieldNames();
		StructType structType = new StructType();

		// use stream
		structType = Stream.of(partitionName).reduce(structType, (a, name) -> a.add(name, "string"), (a, b) -> a);

//		for (String name : partitionName) {
//			structType = structType.add(name, "string");
//		}

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
