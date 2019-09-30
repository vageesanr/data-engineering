package org.vageesan.coding.spark_coding;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import scala.Tuple2;
import scala.collection.mutable.WrappedArray;

public class SparkUDFProcessor {

	public static void registerUDFs(SparkSession spark) {
		
		
		UDF3 geLabelCode = new UDF3<String, String, String, Integer>() {
			public Integer call(String eventName, String objectType, String successSourceType) {
				
				if(eventName.equals("MergeToTree") && objectType.equals("Person") && successSourceType.equals("Hint")) {
					return 1;
				}else if(eventName.equals("MergeToTree") && objectType.equals("Person") && successSourceType.equals("Search")) {
					return 2;
				}else if(eventName.equals("MergeToTree") && objectType.equals("Record") && successSourceType.equals("Hint")) {
					return 3;
				}else if(eventName.equals("MergeToTree") && objectType.equals("Record") && successSourceType.equals("Search")) {
					return 4;
				}else {
					return 0;
				}
			}
		};
		spark.udf().register("geLabelCode", geLabelCode, DataTypes.IntegerType);
		
		UDF3 getIdforMaxScore = new UDF3<WrappedArray<String>, Long, WrappedArray<Double>, String>() {
			public String call(WrappedArray<String> ids, Long size, WrappedArray<Double> scores) {
				double x=0.0;
				String maxId="";
				for(int i=0; i<scores.length(); i++) {
					if(scores.apply(i)>x) {
						x=scores.apply(i);
						maxId=ids.apply(i);
					}
				}
				return maxId;
			}
			
		};
		
		spark.udf().register("getIdforMaxScore", getIdforMaxScore, DataTypes.StringType);
		
		
		List<StructField> fields = new ArrayList<>();
		fields.add(DataTypes.createStructField("MinScore", DataTypes.DoubleType, false));
		fields.add(DataTypes.createStructField("MaxScore", DataTypes.DoubleType, false));
		DataType schema = DataTypes.createStructType(fields);
		
		UDF1 getMinMaxScore = new UDF1<WrappedArray<Double>, Tuple2<Double, Double>>() {
			public Tuple2<Double, Double> call(WrappedArray<Double> scores) {
				Double x= Double.MIN_VALUE;
				Double y = Double.MAX_VALUE;
				for(int i=0; i<scores.length(); i++) {
					if(scores.apply(i)>x) {
						x=scores.apply(i);
					}
					if(scores.apply(i)<y) {
						y=scores.apply(i);
					}
				}
				return Tuple2.apply(y, x);
			}
			
		};
		
		spark.udf().register("getMinMaxScore", getMinMaxScore, schema);
		
		fields = new ArrayList<>();
		fields.add(DataTypes.createStructField("TimeStamp", DataTypes.StringType, false));
		fields.add(DataTypes.createStructField("LabelCode", DataTypes.IntegerType, false));
		schema = DataTypes.createStructType(fields);
		
		UDF2 createTuple = new UDF2<String, Integer, Tuple2<String, Integer>>() {
			public Tuple2<String, Integer> call(String timeStamp, Integer labelCode) {
				return Tuple2.apply(timeStamp, labelCode);
				
			}
		};
		
		spark.udf().register("createTuple", createTuple, schema);
	}
}
