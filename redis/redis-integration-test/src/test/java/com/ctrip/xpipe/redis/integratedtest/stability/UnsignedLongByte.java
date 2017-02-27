package com.ctrip.xpipe.redis.integratedtest.stability;

import org.junit.Assert;

/**
 * @author wenchao.meng
 *
 *         Nov 25, 2016
 */
public class UnsignedLongByte {

	private byte[] dat = new byte[30];

	private int len = 0;

	public UnsignedLongByte from(long data) {

		len = 0;

		do{

			int current = (int) (data % 10);
			dat[len++] = (byte) ('0' + current & 0XFF);
			data = data / 10;
		}while (data != 0);
		reverse(dat, 0, len);
		return this;
	}

	private void reverse(byte[] data, int begin, int end) {

		for (int i = begin; i <= (begin + end - 1) / 2; i++) {

			byte tmp = data[i];
			int endIndex = end - (i - begin) - 1;

			data[i] = data[endIndex];
			data[endIndex] = tmp;
		}

	}

	public int put(byte[] dst) {
		return put(dst, 0);
	}

	public int put(byte[] dst, int pos) {

		for (int i = 0; i < len; i++) {
			dst[i + pos] = dat[i];
		}
		return len;
	}

	@Override
	public String toString() {
		return new String(dat, 0, len);
	}

	public byte[] getBytes() {

		byte[] ret = new byte[len];
		System.arraycopy(dat, 0, ret, 0, len);
		return ret;
	}

	public static void main(String[] argc) {


		UnsignedLongByte test = new UnsignedLongByte();
		System.out.println(test.from(1230));

		for(long i = Long.MAX_VALUE; i >= Long.MAX_VALUE - 10000;i--){
			
			UnsignedLongByte unsignedLongByte = new UnsignedLongByte();
			unsignedLongByte.from(i);
			
			Assert.assertEquals(i, Long.parseLong(unsignedLongByte.toString()));

			byte[] dst = new byte[100];
			int len = unsignedLongByte.put(dst);
			
			Assert.assertEquals(i, Long.parseLong(new String(dst, 0, len)));
		}

		for(long i = 0; i <= 100000;i++){
			
			UnsignedLongByte unsignedLongByte = new UnsignedLongByte();
			unsignedLongByte.from(i);
			
			Assert.assertEquals(i, Long.parseLong(unsignedLongByte.toString()));

			byte[] dst = new byte[100];
			int len = unsignedLongByte.put(dst);
			
			Assert.assertEquals(i, Long.parseLong(new String(dst, 0, len)));
		}

	}
}
