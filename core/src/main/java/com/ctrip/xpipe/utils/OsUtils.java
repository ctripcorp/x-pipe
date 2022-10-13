package com.ctrip.xpipe.utils;

import java.util.Properties;

/**
 * @author wenchao.meng
 *
 * 2016年3月29日 下午3:33:17
 */
public class OsUtils {
	
	private static final int CPU_COUNT;
	private static long startTime = System.currentTimeMillis();
	public  static int 	APPROXIMATE__RESTART_TIME_MILLI = Integer.parseInt(System.getProperty("APPROXIMATE__RESTART_TIME_MILLI", "60000"));
	public static final String LINE_SEPARATOR = System.getProperty("line.separator", "\n");
			
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

	public static int defaultMaxCoreThreadCount(){
		return getMultiCpuOrMax(100, 200);
	}


	public static int getMultiCpuOrMax(int multi, int max){
		return Math.min(getCpuCount() * multi, max);
	}



	public static String osInfo(){
		
		Properties props=System.getProperties();   
		String osName = props.getProperty("os.name");   
		String osArch = props.getProperty("os.arch");   
		String osVersion = props.getProperty("os.version");
		
		return String.format("%s %s %s", osName, osVersion, osArch);
	}

	
	/**
	 * time should be larger than system starttime
	 * @param strTime
	 * @return
	 */
	public static long getCorrentTime(String strTime){
		
		try{
			Long time = Long.parseLong(strTime);
			if(time < startTime || time > System.currentTimeMillis()){
				return -1;
			}
			return time;
		}catch(Exception e){
			return -1L;
		}
	}
}
