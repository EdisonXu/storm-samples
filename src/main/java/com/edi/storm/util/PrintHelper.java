package com.edi.storm.util;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author Edison Xu
 *
 * Jan 2, 2014
 */
public class PrintHelper {

	private static SimpleDateFormat sf = new SimpleDateFormat("mm:ss:SSS");
	
	public static void print(String out){
		System.err.println(sf.format(new Date()) + " [" + Thread.currentThread().getName() + "] " + out);
	}
	
}
