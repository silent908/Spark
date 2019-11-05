package main.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkApp {
	@SuppressWarnings("all")
	public static void main(String args[]){
		/*String logFile = "/spark-2.4.4-bin-hadoop2.7/README.md"; // Should be some file on your system
	    SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
	    Dataset<String> logData = spark.read().textFile(logFile).cache();

	    long numAs = logData.filter(s -> s.contains("a")).count();
	    long numBs = logData.filter(s -> s.contains("b")).count();

	    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

	    spark.stop();*/
		
		String logFile = "/home/spark-2.4.4-bin-hadoop2.7/README.md";
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Simple Application");
	    conf.set("spark.testing.memory", "2147480000");//后面的值大于512m即可
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile(logFile);
		long numAs = lines.filter(s -> s.contains("A")).count();
		long numBs = lines.filter(s -> s.contains("B")).count();
		System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
		
		sc.stop();
	}
}
