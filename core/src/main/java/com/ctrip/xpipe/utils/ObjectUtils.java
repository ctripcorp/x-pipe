package com.ctrip.xpipe.utils;

import java.lang.reflect.Method;
import java.util.Arrays;

/**
 * @author wenchao.meng
 *
 * Jun 14, 2016
 */
public class ObjectUtils {
	
	
	public static int hashCode(Object ...objects){
		return Arrays.hashCode(objects);
	}
	
	public static boolean equals(Object obj1, Object obj2){
		
		if(obj1 == obj2){
			return true;
		}
		
		if(obj1 == null || obj2 == null){
			return false;
		}
		
		return obj1.equals(obj2);
	}
	
	public static <T>  boolean equals(T obj1, T obj2, EqualFunction<T> equalFunction){

		if(obj1 == obj2){
			return true;
		}
		
		if(obj1 == null || obj2 == null){
			return false;
		}
		
		return equalFunction.equals(obj1, obj2);
	}
	
	public static interface EqualFunction<T>{
		
		boolean equals(T obj1, T obj2);
		
	}
	
	public static Method getMethod(String methodName, Class<?> clazz) {
		
		for(Method method : clazz.getMethods()){
			if(method.getName().equals(methodName)){
				return method;
			}
		}
		return null;
	}


}
