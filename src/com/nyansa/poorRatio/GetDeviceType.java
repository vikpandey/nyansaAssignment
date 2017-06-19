package com.nyansa.poorRatio;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * 
 * @author vikas
 *
 */

public class GetDeviceType implements PairFunction<String, String, Integer[]> {

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.spark.api.java.function.PairFunction#call(java.lang.Object)
	 * 
	 * In this call method, String is a passed as parameter and I am creating a key based on id and device_type.
	 * Return a Tuple<String, Integer[]>
	 * 
	 */

	@Override
	public Tuple2<String, Integer[]> call(String t) throws Exception {

		String[] arr = t.split(",");

		String key = arr[0] + "-" + arr[1];
		Integer[] value = { Integer.parseInt(arr[2]), 1 };

		return new Tuple2<String, Integer[]>(key, value);
	}

}
