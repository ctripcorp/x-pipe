package com.ctrip.xpipe.utils;

/**
 * @author wenchao.meng
 *
 * 2016年3月28日 下午6:46:26
 */
public class StringUtil {
	
	public static String join(String split, String ...args){
		
		int i = 0;
		StringBuilder sb = new StringBuilder();
		for(String arg : args){
			sb.append(arg);
			i++;
			if( i < args.length){
				sb.append(split);
			}
		}
		return sb.toString();
	}

}
