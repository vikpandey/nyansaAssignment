package com.nyansa.poorRatio;

import org.apache.spark.api.java.function.Function2;

/**
 * 
 * @author vikas
 *
 */

public class FindRatio implements Function2<Integer[], Integer[], Integer[]> {

	/*
	 * (non-Javadoc)
	 * @see org.apache.spark.api.java.function.Function2#call(java.lang.Object, java.lang.Object)
	 */
	
	@Override
	public Integer[] call(Integer[] v1, Integer[] v2) throws Exception {

		int counter = 0;

		if (v1[0] < 50) {
			++counter;
		}
		if (v2[0] < 50) {
			++counter;
		}
		Integer[] value = { counter, v1[1] + v2[1] };

		return value;
	}

}
