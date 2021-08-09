package boluo.delta;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

public class Doc01_Quickstart {

	public static void main(String[] args) {
		//
		SparkSession spark = SparkSession
				.builder()
				.appName("Java Spark SQL basic example")
				.getOrCreate();

		Dataset<Long> data = spark.range(0, 5);
		data.write().format("delta").save("/tmp/delta-table");
	}
}
