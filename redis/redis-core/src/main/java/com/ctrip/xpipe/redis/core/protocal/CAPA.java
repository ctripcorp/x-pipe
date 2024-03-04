package com.ctrip.xpipe.redis.core.protocal;

/**
 * @author wenchao.meng
 *
 *         Dec 25, 2016
 */
public enum CAPA {
	
	EOF,
	PSYNC2,
	RORDB;
	
	public static CAPA of(String capaString) {
		
		for(CAPA capa : CAPA.values()){
			if(capa.toString().equalsIgnoreCase(capaString)){
				return capa;
			}
		}
		throw new IllegalArgumentException("unsupported capa type:" + capaString);
	}
	
	@Override
	public String toString() {
		return super.toString().toLowerCase();
	}
}
