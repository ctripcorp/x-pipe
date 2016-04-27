package com.ctrip.xpipe.redis.protocal.cmd;


import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

import com.ctrip.xpipe.api.payload.InOutPayload;
import com.ctrip.xpipe.payload.AbstractInOutPayload;
import com.ctrip.xpipe.redis.keeper.RdbFile;
import com.ctrip.xpipe.redis.keeper.ReplicationStore;

import io.netty.buffer.ByteBuf;

/**
 * @author wenchao.meng
 *
 * 2016年4月20日 下午5:19:55
 */
public class InOutPayloadReplicationStore extends AbstractInOutPayload implements InOutPayload{

	public  ReplicationStore replicationStore;
	
	private RdbFile rdbFile;
	
	public InOutPayloadReplicationStore(ReplicationStore replicationStore) {
		this.replicationStore = replicationStore;
	}

	
	@Override
	public int doIn(ByteBuf byteBuf) throws IOException {
		
		if(logger.isDebugEnabled()){
			logger.debug("[doIn]" + byteBuf.readableBytes());
		}
		
		return replicationStore.writeRdb(byteBuf);
	}

	@Override
	public void doStartOutput() throws IOException {
		
		rdbFile = replicationStore.getRdbFile();
	}

	@Override
	public long doOut(WritableByteChannel writableByteChannel) throws IOException {
		
		FileChannel fileChannel = rdbFile.getRdbFile();
		long n = rdbFile.getRdbFile().transferTo(0, fileChannel.size(), writableByteChannel);
		return n;
	}


	@Override
	public void doEndOutput() {
		
		if(rdbFile.getRdbFile() != null){
			try {
				rdbFile.getRdbFile().close();
			} catch (IOException e) {
				logger.error("[doEndOutput]" + rdbFile.getRdbFile(), e);
			}
		}
	}

}
