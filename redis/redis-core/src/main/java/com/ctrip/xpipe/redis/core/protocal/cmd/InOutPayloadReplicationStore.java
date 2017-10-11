package com.ctrip.xpipe.redis.core.protocal.cmd;


import com.ctrip.xpipe.api.payload.InOutPayload;
import com.ctrip.xpipe.payload.AbstractInOutPayload;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author wenchao.meng
 *
 * 2016年4月20日 下午5:19:55
 */
public class InOutPayloadReplicationStore extends AbstractInOutPayload implements InOutPayload {

	public  RdbStore rdbStore;
	
	private AtomicBoolean stop = new AtomicBoolean(false);
	
	public InOutPayloadReplicationStore() {
	}
	
	public void setRdbStore(RdbStore rdbStore) {
		this.rdbStore = rdbStore;
	}

	@Override
	public int doIn(ByteBuf byteBuf) throws IOException {
		
		if(logger.isDebugEnabled()){
			logger.debug("[doIn]" + byteBuf.readableBytes());
		}
		return rdbStore.writeRdb(byteBuf);
	}

	@Override
	public void endInputTruncate(int reduceLen) throws IOException {
		rdbStore.truncateEndRdb(reduceLen);
	}
	
	@Override
	protected void doEndInput() throws IOException {
		rdbStore.endRdb();
		super.doEndInput();
	}
	@Override
	public void doStartOutput() throws IOException {
		
	}

	@Override
	public long doOut(WritableByteChannel writableByteChannel) throws IOException {
		throw new UnsupportedOperationException("Should not call doOut");
	}

	@Override
	public void doEndOutput() {
		stop.set(true);
	}

	@Override
	protected void doTruncate(int reduceLen) throws IOException {
		
	}

}
