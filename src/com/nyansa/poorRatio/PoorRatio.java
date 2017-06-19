package com.nyansa.poorRatio;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

/**
 * 
 * @author vikas
 *
 */

public class PoorRatio {

	public static void main(String[] args) {

		if (args.length < 1) {
			System.out.println("please enter the file path with file name....");
			System.out.println("file name missing...");
			System.out.println("please try again...");
			return;
		} else if (args.length > 1) {
			System.out.println("I only need the file name with the file path.. it looks like you have added "
					+ "more than 1 argument");
			System.out.println("please try again...");
			return;
		}

		String appName = "nyansaApp";
		String sparkMaster = "local[2]";

		SparkConf conf = new SparkConf().setAppName(appName).setMaster(sparkMaster);

		JavaSparkContext context = new JavaSparkContext(conf);

		// JavaRDD<String> rdd = context.textFile("data/mapReduce.csv");
		JavaRDD<String> rdd = context.textFile(args[0]);

		// Removing the header from the dataset

		String header = rdd.first();
		JavaRDD<String> dataRDD = rdd.filter(s -> !s.contains(header));

		// Creating a PairRDD and mapping the key value pair
		JavaPairRDD<String, Integer[]> mapDeviceType = dataRDD.mapToPair(new GetDeviceType());

		System.out.println("key value rdd raw data ...");
		printStringRDD(mapDeviceType, (int) mapDeviceType.count());

		//Adding the total score using reduceByKey method.
		JavaPairRDD<String, Integer[]> sumScore = mapDeviceType.reduceByKey(new ComputeTotalScore());

		System.out.println();
		System.out.println();
		System.out.println("***************************************");
		System.out.println("total sum value :- ");
		printStringRDD(sumScore, (int) sumScore.count());

		// Calculating average score using mapValues().
		JavaPairRDD<String, Integer> avgScore = sumScore.mapValues(x -> x[0] / x[1]);

		System.out.println();
		System.out.println();
		System.out.println("****************************************");
		System.out.println("average score");
		printRDDValue(avgScore, (int) avgScore.count());


		JavaRDD<Object> testMap = avgScore.map(s -> s);
		System.out.println("testMap count is :- " + testMap.count());

		System.out.println("value :- ");
		printRDD(testMap, (int) testMap.count());

		// Mapping the device_type with the average score using mapToPair()
		JavaPairRDD<String, Integer[]> ratioRDD = testMap.mapToPair(new RatioValue());
		System.out.println("pair rdd now ...");
		printStringRDD(ratioRDD, (int) ratioRDD.count());

		// Calculating number of poor ratio based on the key using reduceByKey
		JavaPairRDD<String, Integer[]> findRatioCount = ratioRDD.reduceByKey(new FindRatio());
		System.out.println("ratio count...");
		printStringRDD(findRatioCount, (int) findRatioCount.count());

		// Calculating the ratio of number of device which is poor by calculating number of 
		// devices that are poor to the number of devices of that device_type
		System.out.println("calculating poor ratio ...");
		JavaPairRDD<String, Double> poorRatio = findRatioCount.mapValues(new Function<Integer[], Double>() {

			/*
			 * (non-Javadoc)
			 * @see org.apache.spark.api.java.function.Function#call(java.lang.Object)
			 * 
			 * Here I am calculating the poor ratio. I have passed an Integer array and then casting it into
			 * a double. Then return Double.
			 */
						
			@Override
			public Double call(Integer[] v1) throws Exception {

				Integer x = v1[0];
				Integer y = v1[1];

				Double xx = (double) x;
				Double yy = (double) y;

				return xx / yy;
			}
		});
		
		printPoorRatio(poorRatio, (int) poorRatio.count());

		// sorting the key in descending order for highest poor ratio
		JavaPairRDD<String, Double> outputRDD = poorRatio.sortByKey(false);
		
		// taking the first element from descending order for maximum
		List<Tuple2<String, Double>> highestPoorRatio = outputRDD.take(1);

		System.out.println("highest poor ratio is :- " + highestPoorRatio.get(0)._1);

	}

	/**
	 * 
	 * @param key  JavaPairRDD, having String as Key and Double as Value
	 * @param count this is RDD.count() value.
	 */
	
	public static void printPoorRatio(JavaPairRDD<String, Double> key, int count) {

		for (Tuple2<String, Double> s : key.take(count)) {
			System.out.println(s);
		}
	}

	/**
	 * 
	 * @param rdd
	 * @param count
	 */
	
	private static void printRDD(JavaRDD<Object> rdd, int count) {

		for (Object s : rdd.take(count)) {
			System.out.println(s);
		}
	}

	/**
	 * 
	 * @param key
	 * @param value
	 */
	
	private static void printRDDValue(JavaPairRDD<String, Integer> key, int value) {

		for (Tuple2<String, Integer> list : key.take(value)) {
			System.out.println(list);
		}
	}
	
	/**
	 * 
	 * @param mapDeviceType
	 * @param count
	 */

	private static void printStringRDD(JavaPairRDD<String, Integer[]> mapDeviceType, int count) {

		for (Tuple2<String, Integer[]> s : mapDeviceType.take(count)) {
			System.out.println(s._1 + " - " + s._2[0] + " , " + s._2[1]);
		}

	}

}
