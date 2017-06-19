package com.nyansa.poorRatio;

import org.apache.spark.api.java.function.Function2;

/**
 * 
 * @author vikas
 *
 */

public class ComputeTotalScore implements Function2<Integer[], Integer[], Integer[]> {

	/*
	 * (non-Javadoc)
	 * @see org.apache.spark.api.java.function.Function2#call(java.lang.Object, java.lang.Object)
	 */
	
	@Override
	public Integer[] call(Integer[] v1, Integer[] v2) throws Exception {

		Integer[] value = { v1[0] + v2[0], v1[1] + v2[1] };

		return value;
	}
}
