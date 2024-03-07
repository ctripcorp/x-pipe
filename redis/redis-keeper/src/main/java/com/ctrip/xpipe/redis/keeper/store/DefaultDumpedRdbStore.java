package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.store.DumpedRdbStore;

import java.io.File;
import java.io.IOException;


/**
 * @author wenchao.meng
 *
 * Aug 26, 2016
 */
public class DefaultDumpedRdbStore extends DefaultRdbStore implements DumpedRdbStore{
	
	public DefaultDumpedRdbStore(File file) throws IOException {
		super(file, null, -1, null);
	}

	@Override
	public void setReplId(String replId) {
		this.replId = replId;
	}

	
	@Override
	public void setEofType(EofType eofType) {
		this.eofType = eofType;
	}

	@Override
	public void setRdbOffset(long rdbOffset){
		this.rdbOffset = rdbOffset;
	}
}
