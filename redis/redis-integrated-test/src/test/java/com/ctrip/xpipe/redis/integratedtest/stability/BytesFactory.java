package com.ctrip.xpipe.redis.integratedtest.stability;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;

/**
 * @author wenchao.meng
 *
 * Nov 25, 2016
 */
public class BytesFactory extends BasePooledObjectFactory<byte[]> implements PooledObjectFactory<byte[]>{
	
	private int length;
	private int activeLength = 20;
	
	public BytesFactory(int length){
		this.length = length;
	}

	@Override
	public void activateObject(PooledObject<byte[]> p) throws Exception {
		byte[]data = p.getObject();
		for(int i=0; i < activeLength;i++){
			data[i] = 'c';
		}
	}
	@Override
	public byte[] create() throws Exception {
		
		return randomString(length).getBytes();
	}

	@Override
	public PooledObject<byte[]> wrap(byte[] obj) {
		
		return new DefaultPooledObject<byte[]>(obj);
	}

	
	public static String randomString(int length) {

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < length; i++) {
			sb.append((char) ('a' + (int) (26 * Math.random())));
		}

		return sb.toString();

	}

}
