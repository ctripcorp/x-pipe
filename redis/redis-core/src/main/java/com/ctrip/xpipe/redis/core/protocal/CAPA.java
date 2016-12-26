package com.ctrip.xpipe.redis.core.protocal;

/**
 * @author wenchao.meng
 *
 *         Dec 25, 2016
 */
public enum CAPA {
	
	EOF;

	public static CAPA of(String capaString) {

		if ("eof".equalsIgnoreCase(capaString)) {
			return EOF;
		}
		throw new IllegalArgumentException("unsupported capa type:" + capaString);
	}
	
	@Override
	public String toString() {
		return super.toString().toLowerCase();
	}
}
