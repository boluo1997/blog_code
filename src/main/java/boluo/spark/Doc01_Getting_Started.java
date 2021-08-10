package boluo.spark;

import boluo.model.Person;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Doc01_Getting_Started {
	public static void main(String[] args) {

		SparkSession spark = SparkSession
				.builder()
				.appName("Java Spark SQL basic example")
				.master("local[*]")
				.config("spark.some.config.option", "some-value")
				.getOrCreate();

		// Creating Datasets

		// Create an instance of a Bean class
		Person person = new Person();
		person.setName("Andy");
		person.setAge(30);

		// Encoders are created for Java beans
		Encoder<Person> personEncoder = Encoders.bean(Person.class);
		Dataset<Person> javaBeanDs = spark.createDataset(
				Collections.singletonList(person),
				personEncoder
		);
		javaBeanDs.show();

		// Encoders for most common types are provided in class Encoders
		Encoder<Long> longEncoder = Encoders.LONG();
		Dataset<Long> primitiveDs = spark.createDataset(Arrays.asList(1L, 2L, 3L), longEncoder);
		Dataset<Long> transformedDs = primitiveDs.map(
				(MapFunction<Long, Long>) value -> value + 1L,
				longEncoder
		);
		transformedDs.collect();

		// DataFrames can be converted to a Dataset by providing a class. Mapping based on name
		String path = "examples/src/main/resources/people.json";
		Dataset<Person> peopleDs = spark.read().json(path).as(personEncoder);
		peopleDs.show();


		/**
		 * Spark SQL supports two different methods for converting existing RDDs into Datasets. The first
		 * method uses reflection to infer the schema of an RDD that contains specific types of objects.
		 * This reflection-based approach leads to more concise code and works well when you already know
		 * the schema while writing your Spark application.
		 *
		 * The second method for creating Datasets is through a programmatic interface that allows you to
		 * construct a schema and then apply it to an existing RDD. While this method is more verbose, it
		 * allows you to construct Datasets when the columns and their types are not known until runtime.
		 */

		JavaRDD<Person> peopleRDD = spark.read()
				.textFile("examples/src/main/resources/people.txt")
				.javaRDD()
				.map(line -> {
					String[] parts = line.split(",");
					Person person_ = new Person();
					person_.setName(parts[0]);
					person_.setAge(Integer.parseInt(parts[1].trim()));
					return person_;
				});

		// Apply a schema to an RDD of JavaBeans to get a DataFrame
		Dataset<Row> peopleDs_ = spark.createDataFrame(peopleRDD, Person.class);

		// Register the DataFrame as a temporary view
		peopleDs_.registerTempTable("people");

		Dataset<Row> teenagers = spark.sql("select name from people where age between 13 and 19");
		teenagers.show();

		// The columns of a row in the result can be accessed by field index
		Encoder<String> stringEncoder = Encoders.STRING();
		Dataset<String> teenagerNamesByIndexDs = teenagers.map(
				(MapFunction<Row, String>) row -> "Name: " + row.getString(0),
				stringEncoder
		);
		teenagerNamesByIndexDs.show();

		// or by field name
		Dataset<String> teenagerNamesByFieldDs = teenagers.map(
				(MapFunction<Row, String>) row -> row.<String>getAs("name"),
				stringEncoder
		);
		teenagerNamesByFieldDs.show();

		/**
		 * When JavaBean classes cannot be defined ahead of time (for example, the structure of records is encoded
		 * in a string, or a text dataset will be parsed and fields will be projected differently for different users),
		 * a Dataset<Row> can be created programmatically with three steps.
		 *
		 * 1. Create an RDD of Rows from the original RDD;
		 * 2. Create the schema represented by a StructType matching the structure of Rows in the RDD created in Step 1.
		 * 3. Apply the schema to the RDD of Rows via createDataFrame method provided by SparkSession.
		 */

		JavaRDD<String> peopleRDD_ = spark.sparkContext()
				.textFile("examples/src/main/resources/people.txt", 1)
				.toJavaRDD();

		// The schema is encoded in a string
		String schemaString = "name age";

		// Generate the schema based on the string of schema
		List<StructField> fields = new ArrayList<>();
		for (String fieldName : schemaString.split(" ")) {
			StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
			fields.add(field);
		}
		StructType schema = DataTypes.createStructType(fields);

		// Convert records of the RDD (people) to Rows
		JavaRDD<Row> rowRDD = peopleRDD_.map((Function<String, Row>) record -> {
			String[] attributes = record.split(",");
			return RowFactory.create(attributes[0], attributes[1].trim());
		});


	}


}
