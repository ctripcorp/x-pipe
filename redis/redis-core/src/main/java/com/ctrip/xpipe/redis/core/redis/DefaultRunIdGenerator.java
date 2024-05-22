package com.ctrip.xpipe.redis.core.redis;

import java.util.Random;

/**
 * @author wenchao.meng
 *
 * Aug 19, 2016
 */
public class DefaultRunIdGenerator implements RunidGenerator{
	
	private final int length = 40;
	
	private static final char[] hex = new char[]{
			'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};

	@Override
	public String generateRunid() {
		
		StringBuilder sb = new StringBuilder(length);
		Random random = new Random();
		
		for(int i=0;i<length;i++){
			int index = random.nextInt(16);
			sb.append(hex[index]);
		}
		return sb.toString();
	}

}
