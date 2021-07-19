package boluo.work;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.CharMatcher;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
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
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
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

	static {
		PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
		cm.setMaxTotal(3);
		cm.setDefaultMaxPerRoute(3);
		http = HttpClients.custom()
				.setConnectionManager(cm)
				.build();
	}

	public static void main(String[] args) throws Exception {
		SparkSession spark = SparkSession.builder()
				.getOrCreate();
		CommandLine cli = new GnuParser().parse(new Options()
				.addOption("o", "output", true, "")
				.addOption("user", "user", true, "")
				.addOption("pwd", "pwd", true, ""), args);

		String outputUri = cli.getOptionValue("o");
		String user = cli.getOptionValue("user", "18866668888");
		String pwd = cli.getOptionValue("pwd", "******");
		String loginData = String.format("j_username=%s&j_password=%s", user, pwd);
		outputUri = CharMatcher.anyOf("/").trimTrailingFrom(outputUri) + "/haier2/order";

		Path path = new Path(outputUri);
		FileSystem fs = FileSystem.get(path.toUri(), spark.sparkContext().hadoopConfiguration());
		if (!fs.exists(path)) {
			// 创建文件夹
			fs.mkdirs(path);
		}

		// 1. 查询本地海尔表 max(u_ts)
		String haierHash = getLagPartition(outputUri, "date");
		String date = Strings.isNullOrEmpty(haierHash)
				? "2019-08-19"
				: haierHash;

		Dataset<Row> haier = spark.read().format("delta").load(outputUri);
		LocalDateTime max_u_ts = haier.schema().isEmpty()
				? LocalDateTime.of(2020, 1, 1, 0, 0)
				: ((Timestamp) haier.where(String.format("date>='%s'", date)).select(max("u_ts")).first().getAs(0)).toLocalDateTime();

		LocalDateTime now = LocalDateTime.now();
		LocalDate nowDate = now.toLocalDate();
		LocalDateTime curr = max_u_ts;

		while (curr.compareTo(now) < 0) {

			HttpClientContext httpContext = HttpClientContext.create();
			login(loginData, httpContext);

			curr = Stream.of(curr.toLocalDate().plusDays(1).atStartOfDay(), now).min(LocalDateTime::compareTo).get();
			String insert_u_ts = String.valueOf(curr.atZone(ZoneId.systemDefault()).toEpochSecond());
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
					.withColumn("u_ts", to_timestamp(from_unixtime(expr(insert_u_ts))))
					.withColumn("date", to_date(col("`u_ts`")))
					.coalesce(1);

			ds.write()
					.format("delta")
					.partitionBy("date")
					.mode("overwrite")
					.option("replaceWhere", String.format("date = '%s'", curr.toLocalDate()))
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
			logger.info("{}", req);
			Preconditions.checkArgument(response.getStatusLine().getStatusCode() == 302, response);
			Header[] locations = response.getHeaders("Location");
			Preconditions.checkArgument(locations.length == 1 && !locations[0].getValue().contains("Failed"), response);
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

		return search(httpContext, url, args, 1);
	}

	private static List<ObjectNode> currSearch(HttpClientContext httpContext, LocalDate date) {
		DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy/MM/dd");
		String url = "http://www.saywash.com/saywash/WashCallManager/merchant/order/search.do";
		String args = "type=1&merchantId=3914&orderCode=&phone=&status=&payTypeId=&couponType=" +
				"&startDate=" + date.format(dateTimeFormatter) +
				"&endDate=" + date.format(dateTimeFormatter) +
				"&orderesource=";

		return search(httpContext, url, args, 2);
	}

	private static List<ObjectNode> search(HttpClientContext httpContext, String url, String args, int tag) {
		Document d1 = request(httpContext, url, args);

		Element pager = d1.select(".paging_container>div").first();
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
						.map(i -> request(httpContext, url, args + "&pageIndex=" + i))
		).flatMap(doc -> {
			Element head = doc.select("table>thead tr").first();
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
			ObjectNode detail = tag == 1
					? getHistoryOrderDetail(httpContext, i.at("/key").textValue(), datetime.getYear())
					: getOrderDetail(httpContext, i.at("/key").textValue());
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


	private static ObjectNode getHistoryOrderDetail(HttpClientContext httpContext, String key, int years) {
		String url = "http://www.saywash.com/saywash/WashCallManager/merchant/order/getHistoryOrderDetail.do";
		String args = String.format("orderId=%s&isOrderSearch=0&years=%d", key, years);
		Document doc = request(httpContext, url, args);
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
		Document doc = request(httpContext, url, args);
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

	private static Document request(HttpClientContext httpContext, String url, String body) {
		HttpUriRequest req = RequestBuilder.post()
				.setUri(url)
				.setHeader("Content-Type", "application/x-www-form-urlencoded")
				.setHeader("Referer", "http://www.saywash.com/saywash/WashCallManager/merchant/order/historyorder/index.do")
				.setHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36 Edg/80.0.361.69")
				.setEntity(new StringEntity(body, Charsets.UTF_8))
				.build();
		try (CloseableHttpResponse response = http.execute(req, httpContext)) {
			logger.info("{}", req);
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

	public static String getLagPartition(String path, String part) throws IOException {

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
				int index = fileParent.getPath().toString().indexOf("=");
				String pathName = fileParent.getPath().toString().substring(index + 1);
				results.add(pathName);
			}
		}

		if (results.isEmpty()) {
			return null;
		} else {
			// 字符串排序, 返回第一个
			results.sort(Comparator.reverseOrder());
			return results.get(0);
		}
	}
}
