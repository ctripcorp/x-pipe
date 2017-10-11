package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.keeper.exception.RedisKeeperRuntimeException;

import java.io.File;

/**
 * @author wenchao.meng
 *
 * Aug 24, 2016
 */
public class RdbStoreExeption extends RedisKeeperRuntimeException{

	private static final long serialVersionUID = 1L;

	public RdbStoreExeption(EofType eofType, File rdbFile) {
		super(String.format("eofType:%s, file:%s, len:%d", eofType, rdbFile, rdbFile.length()));
	}

}
