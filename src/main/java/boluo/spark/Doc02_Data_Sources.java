package boluo.spark;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class Doc02_Data_Sources {

	static SparkSession spark = SparkSession
			.builder()
			.appName("Java Spark SQL basic example")
			.master("local[*]")
			.config("spark.some.config.option", "some-value")
			.getOrCreate();

	public static void main(String[] args) {

//		Dataset<Row> userDs = spark.read().load("examples/src/main/resources/users.parquet");
//		userDs.select("name", "favorite_color").write().save("nameAndFavColors.parquet");

		Dataset<Row> peopleDs = spark.read().format("json").load("examples/src/main/resources/people.json");
		peopleDs.show(false);
		peopleDs.write().mode("ignore").save("examples/src/main/resources/people.parquet");

		// load CSV
		Dataset<Row> peopleDsCSV = spark.read().format("csv")
				.option("sep", ",")
				.option("comment", "#")
				.option("charset", "GB2312")
				.option("inferSchema", "true")
				.option("header", "true")
				.load("examples/src/main/resources/people.csv");
		peopleDsCSV.show(false);

		// apache ORC
		peopleDs.write().format("orc")
				.option("orc.bloom.filter.columns", "favorite_color")
				.option("orc.dictionary.key.threshold", "1.0")
				.option("orc.column.encoding.direct", "name")
				.mode(SaveMode.Ignore)
				.save("examples/src/main/resources/users_with_options.orc");

		// run SQL on files directly
		Dataset<Row> sqlDs =
				spark.sql("select * from parquet.`examples/src/main/resources/people.parquet`");
		sqlDs.show(false);

		// 目前在使用 bucketBy 的时候，必须和 sortBy，saveAsTable 一起使用
		peopleDs.write()
				.bucketBy(42, "name")
				.sortBy("age")
				.mode("ignore")
				.saveAsTable("people_bucketed");

		peopleDs.write()
				.partitionBy("favorite_color")
				.format("parquet")
				.save("examples/src/main/resources/people.parquet");

		peopleDs.write()
				.partitionBy("favorite_color")
				.bucketBy(42, "name")
				.saveAsTable("users_partitioned_bucketed");
	}

	@Test
	public void func1() {

		// load parquet
		Dataset<Row> peopleDs = SparkSession.active().read().parquet("examples/src/main/resources/people.parquet");
		peopleDs.registerTempTable("parquetFile");
		Dataset<Row> namesDf = SparkSession.active().sql("select name from parquetFile where age between 13 and 19");
		Dataset<String> namesDs = namesDf.map(
				(MapFunction<Row, String>) row -> "Name: " + row.getString(0),
				Encoders.STRING()
		);
		namesDs.show();
	}

	@Test
	public void func2() {

		List<String> jsonData = Arrays.asList(
				"{\"name\":\"Yin\",\"address\":{\"city\":\"Columbus\",\"state\":\"Ohio\"}}");
		Dataset<String> anotherPeopleDataset = spark.createDataset(jsonData, Encoders.STRING());
		anotherPeopleDataset.show(false);
	}

	@Test
	public void func3() {

		// loading JDBC source
		Dataset<Row> jdbcDs = SparkSession.active().read()
				.format("jdbc")
				.option("url", "127.0.0.1:3306")
				.option("dbtable", "japan_video")
				.option("user", "root")
				.option("password", "root")
				.load();
		jdbcDs.show(false);
	}

}




