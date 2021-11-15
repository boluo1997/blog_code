package boluo.delta;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.util.HashMap;

import static org.apache.spark.sql.functions.*;

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

	@Test
	public void func2() {
		// Delta Lake provides programmatic APIs to conditional update, delete and merge(upsert) data into tables
		DeltaTable deltaTable = DeltaTable.forPath("./examples/delta-table");

		// Update every even value by adding 100 to it
		deltaTable.update(
				expr("id % 2 == 0"),
				new HashMap<String, Column>() {{
					put("id", expr("id + 100"));
				}}
		);

		// Delete every even value
		deltaTable.delete(expr("id % 2 == 0"));

		// Upsert (merge) new data
		Dataset<Row> newData = spark.range(0, 20).toDF();
		deltaTable.as("oldData")
				.merge(
						newData.as("newData"),
						"oldData.id = newData.id"
				)
				.whenMatched()
				.update(
						new HashMap<String, Column>() {{
							put("id", expr("newData.id"));
						}})
				.whenNotMatched()
				.insertAll()
				.execute();
	}

	@Test
	public void func3() {

		// read older versions of data using time travel
		Dataset<Row> ds = SparkSession.active().read().format("delta")
				.option("versionAsOf", 0)
				.load("/tmp/delta-table");
	}

	@Test
	public void func4() {

		// write a stream of data to a table
	}

}




