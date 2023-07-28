package com.ctrip.xpipe.redis.keeper;

/**
 * @author wenchao.meng
 *
 * Aug 25, 2016
 */
public enum SLAVE_STATE {

	REDIS_REPL_WAIT_RDB_DUMPING("wait_rdb_dumping"),
	REDIS_REPL_WAIT_RDB_GTIDSET("wait_rdb_gtidset"),
	REDIS_REPL_WAIT_SEQ_FSYNC("wait_seq_fsync"),
	REDIS_REPL_SEND_BULK("send_bulk"),
	REDIS_REPL_ONLINE("online");
	
	private String desc;
	SLAVE_STATE(String desc){
		this.desc = desc;
	}
	public String getDesc() {
		return desc;
	}

}
