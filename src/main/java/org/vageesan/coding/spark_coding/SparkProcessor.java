package org.vageesan.coding.spark_coding;

import java.io.File;
import java.net.URL;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class SparkProcessor {
	public static void main(String[] args) {
		SparkProcessor main = new SparkProcessor();
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("JD Word Counter");
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

		SparkSession spark = SparkSession.builder().appName(sparkConf.get("spark.app.name")).getOrCreate();

		SparkUDFProcessor.registerUDFs(spark);

		// Merge To Tree json transformed to Dataset
		Dataset<Row> mttDS = spark.read().format("json").option("header", true)
				.load(main.getFileFromResources("merge_to_tree.json").getAbsolutePath());
		// Search Log json transformed to Dataset
		Dataset<Row> slDS = spark.read().format("json").option("header", true)
				.load(main.getFileFromResources("search_log.json").getAbsolutePath());
		// Search Results Click json transformed to Dataset
		Dataset<Row> srcDS = spark.read().format("json").option("header", true)
				.load(main.getFileFromResources("search_results_click.json").getAbsolutePath());

		// Adding LabelCode based on the conditions
		Dataset<Row> mttDS1 = mttDS.withColumn("LabelCode", functions.callUDF("geLabelCode", mttDS.col("EventName"),
				mttDS.col("ObjectType"), mttDS.col("SuccessSourceType")));

		//Creating a tuple of Timestamp/LabelCode
		Dataset<Row> mttDS2 = mttDS1.select(mttDS1.col("UserId"), mttDS1.col("RecordId"), mttDS1.col("DatabaseId"),
				mttDS1.col("TimeStamp"), mttDS1.col("LabelCode")).withColumn("TSLabelCode",
						functions.callUDF("createTuple", mttDS1.col("TimeStamp"), mttDS1.col("LabelCode")));

		//Aggregating the tuple into a list for the same UserId/RecordId/DatabaseId
		Dataset<Row> mttDS3 = mttDS2.drop(mttDS2.col("LabelCode")).drop(mttDS2.col("TimeStamp"))
				.groupBy(mttDS2.col("UserId").as("UserId"), mttDS2.col("RecordId").as("RecordId"),
						mttDS2.col("DataBaseId").as("DatabaseId"))
				.agg(functions.collect_set(mttDS2.col("TSLabelCode")).as("Labels"))
				.withColumnRenamed("UserId", "UserId1").withColumnRenamed("RecordId", "RecordId1")
				.withColumnRenamed("DatabaseId", "DatabaseId1");

		// Joining search results click with the transformed merge to tree events based on UserId RecordId and DatabaseId
		Dataset<Row> user_eventsDS = srcDS
				.select(srcDS.col("QueryId"), srcDS.col("UserId"), srcDS.col("RecordId"), srcDS.col("DatabaseId"))
				.join(mttDS3,
						srcDS.col("UserId").equalTo(mttDS3.col("UserId1"))
								.and(srcDS.col("RecordId").equalTo(mttDS3.col("RecordId1")))
								.and(srcDS.col("DatabaseId").equalTo(mttDS3.col("DatabaseId1"))));
		
		//Dropping the joined fields from merge to tree table
		Dataset<Row> user_eventsDS1 = user_eventsDS.drop(user_eventsDS.col("UserId1")).drop(user_eventsDS.col("RecordId1"))
				.drop(user_eventsDS.col("DatabaseId1"));

		// Extracting the Query Reply Id with max score
		Dataset<Row> slDS1 = slDS
				.select(slDS.col("QueryId"),
						functions.callUDF("getIdforMaxScore", slDS.col("QueryReply.ids"),
								slDS.col("QueryReply.numFound"), slDS.col("QueryReply.scores")))
				.toDF("QueryId", "QueryReplyId").drop(slDS.col("QueryReply"));

		
		// Joining with the search log with Query id to get the result DS
		Dataset<Row> resultDS = user_eventsDS1.join(slDS1, "QueryId");

		// Bonus problem
		// Getting a column tuple with min and max scores from search log for each queryid
		Column col = functions.callUDF("getMinMaxScore", slDS.col("QueryReply.scores"));
		
		// adding MinMax column to search log DS
		Dataset<Row> slDS2 = slDS.withColumn("MinMax", col);
		
		// extracting Min and Max score as seperate columns
		Dataset<Row> slDS3 = slDS2.withColumn("MinScore", slDS2.col("MinMax.MinScore"))
				.withColumn("MaxScore", slDS2.col("MinMax.MaxScore")).drop(slDS2.col("QueryReply"))
				.drop(slDS2.col("MinMax"));
		
		// Joining the transformed user_events with search logs to get the result of the bonus question
		Dataset<Row> bonus_resultDS = user_eventsDS1.join(slDS3, "QueryId");

		resultDS.show(20, false);
		bonus_resultDS.show(20, false);

	}

	// get file from classpath, resources folder
	public File getFileFromResources(String fileName) {

		ClassLoader classLoader = getClass().getClassLoader();

		URL resource = classLoader.getResource(fileName);
		if (resource == null) {
			throw new IllegalArgumentException("file is not found!");
		} else {
			return new File(resource.getFile());
		}

	}
};