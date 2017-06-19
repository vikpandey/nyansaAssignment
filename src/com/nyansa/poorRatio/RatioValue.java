package com.nyansa.poorRatio;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * 
 * @author vikas
 *
 */

public class RatioValue implements PairFunction<Object, String, Integer[]> {

	/*
	 * (non-Javadoc)
	 * @see org.apache.spark.api.java.function.PairFunction#call(java.lang.Object)
	 */
	
	@Override
	public Tuple2<String, Integer[]> call(Object t) throws Exception {

		String s = t.toString();
		String[] arr = s.split("[, - ( )]+");
		String[] key = arr[1].split("-");
		String k = key[1];
		Integer[] value = { Integer.parseInt(arr[2]), 1 };

		return new Tuple2<String, Integer[]>(k, value);
	}
}
