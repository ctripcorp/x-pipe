package com.ctrip.xpipe.redis.protocal.cmd;


import java.io.IOException;
import java.nio.channels.WritableByteChannel;

import com.ctrip.xpipe.api.payload.InOutPayload;
import com.ctrip.xpipe.redis.keeper.ReplicationStore;

import io.netty.buffer.ByteBuf;
import io.netty.channel.FileRegion;

/**
 * @author wenchao.meng
 *
 * 2016年4月20日 下午5:19:55
 */
public class InOutPayloadReplicationStore implements InOutPayload{

	public  ReplicationStore replicationStore;
	
	private FileRegion fileRegion;
	private long position;
	
	public InOutPayloadReplicationStore(ReplicationStore replicationStore) {
		this.replicationStore = replicationStore;
	}

	

	
	@Override
	public void startInput() {
	}


	@Override
	public int in(ByteBuf byteBuf) throws IOException {
		
		return replicationStore.writeRdb(byteBuf);
	}

	@Override
	public void endInput() {
		
	}

	
	@Override
	public void startOutput() {
		fileRegion = replicationStore.getRdbFile();
	}

	@Override
	public long out(WritableByteChannel writableByteChannel) throws IOException {
		
		long n = fileRegion.transferTo(writableByteChannel, position);
		position += n;
		if(fileRegion.transfered() >=  fileRegion.count()){
			fileRegion.release();
		}
		return n;
	}


	@Override
	public void endOutput() {
		
		if(fileRegion.refCnt() >= 0){
			fileRegion.release(fileRegion.refCnt());
		}
	}

}
