package com.ctrip.xpipe.redis.integratedtest.stability;

/**
 * @author wenchao.meng
 *
 * Nov 25, 2016
 */
public class UnsignedLongByte {
	
	private byte []dat = new byte[30];
	
	private int len = 0;

	public UnsignedLongByte from(long data){
		
		len = 0;
		
		while(data != 0){
			
			int current = (int) (data%10);
			dat[len++] = (byte) ('0' + current & 0XFF);
			data = data/10;
		}
		reverse(dat, 0, len);
		return this;
	}
	
	private void reverse(byte[] data, int begin, int end) {
		
		for(int i=begin;i<= (begin + end - 1)/2;i++){
			
			byte tmp = data[i];
			int endIndex = end -(i-begin) -1;
			
			data[i] = data[endIndex];  
			data[endIndex] = tmp;
		}
		
	}

	public int put(byte []dst){
		return put(dst, 0);
	}

	public int put(byte []dst, int pos){
		
		for(int i=0;i<len;i++){
			dst[i + pos] = dat[i];
		}
		return len;
	}
	
	@Override
	public String toString(){
		return new String(dat, 0, len);
	}
	
	public byte[] getBytes(){
		
		byte []ret = new byte[len];
		System.arraycopy(dat, 0, ret, 0, len);
		return ret;
	}
	
	public static void main(String []argc){
		
		UnsignedLongByte unsignedLongByte = new UnsignedLongByte();
		unsignedLongByte.from(Long.MAX_VALUE);
		
		System.out.println(unsignedLongByte.toString());
		System.out.println(Long.MAX_VALUE);
		
		byte []dst = new byte[100];
		
		int len = unsignedLongByte.put(dst);
		
		System.out.println(new String(dst, 0, len));
		
	}
}
