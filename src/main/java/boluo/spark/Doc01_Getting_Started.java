package boluo.spark;

import org.apache.spark.sql.SparkSession;

public class Doc01_Getting_Started {
	public static void main(String[] args) {

		SparkSession spark = SparkSession
				.builder()
				.appName("Java Spark SQL basic example")
				.config("spark.some.config.option", "some-value")
				.getOrCreate();

	}
}
