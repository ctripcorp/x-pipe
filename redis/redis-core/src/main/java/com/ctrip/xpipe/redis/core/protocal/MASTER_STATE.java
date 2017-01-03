package com.ctrip.xpipe.redis.core.protocal;

/**
 * @author wenchao.meng
 *
 * Sep 14, 2016
 */
public enum MASTER_STATE {
	
	REDIS_REPL_NONE("none"),
	REDIS_REPL_CONNECT("connect"),
	REDIS_REPL_CONNECTING("connecting"),
	REDIS_REPL_TRANSFER("sync"),
	REDIS_REPL_CONNECTED("connected"),
	
	//NOT IMPLEMENTED IN KEEPER
	REDIS_REPL_HANDSHAKE("handshake"),
	REDIS_REPL_UNKNOWN("unknown");
	

	private String desc;
	MASTER_STATE(String desc){
		this.desc = desc;
	}
	
	public String getDesc() {
		return desc;
	}
	
	public static MASTER_STATE fromDesc(String desc){
		for(MASTER_STATE state : MASTER_STATE.values()){
			if(state.getDesc().equals(desc)){
				return state;
			}
		}
		throw new IllegalStateException("unknown desc:" + desc);
	}

}
