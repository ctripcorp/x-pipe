package com.ctrip.xpipe.utils;

/**
 * @author wenchao.meng
 *
 * 2016年3月29日 下午3:33:17
 */
public class CpuUtils {
	
	private static final int CPU_COUNT;
			
	static{
		
		String cpuCount = System.getProperty("CPU_COUNT"); 
		if( cpuCount != null){
			CPU_COUNT = Integer.parseInt(cpuCount);
		}else{
			CPU_COUNT = Runtime.getRuntime().availableProcessors();
		}
	}
			
	public static int getCpuCount(){
		return CPU_COUNT;
	}

}
