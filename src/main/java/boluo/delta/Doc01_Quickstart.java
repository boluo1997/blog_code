package boluo.delta;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

public class Doc01_Quickstart {

	private static final SparkSession spark = SparkSession
			.builder()
			.appName("Java Spark SQL basic example")
			.master("local[*]")
			.getOrCreate();

	public static void main(String[] args) {

		Dataset<Long> data = SparkSession.active().range(0, 5);
		data.write()
				.format("delta")
				.mode("overwrite")
				.save("./examples/delta-table");
	}

	@Test
	public void func1() {
		Dataset<Row> ds = SparkSession.active().read().format("delta").load("./examples/delta-table");
		ds.show(false);
		System.out.println("---");
	}

}
