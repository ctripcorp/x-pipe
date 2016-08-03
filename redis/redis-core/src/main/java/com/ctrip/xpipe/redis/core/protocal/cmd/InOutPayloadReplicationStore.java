package com.ctrip.xpipe.redis.core.protocal.cmd;


import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import com.ctrip.xpipe.api.payload.InOutPayload;
import com.ctrip.xpipe.payload.AbstractInOutPayload;
import com.ctrip.xpipe.redis.core.store.RdbFileListener;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;

import io.netty.buffer.ByteBuf;

/**
 * @author wenchao.meng
 *
 * 2016年4月20日 下午5:19:55
 */
public class InOutPayloadReplicationStore extends AbstractInOutPayload implements InOutPayload, RdbFileListener{

	public  ReplicationStore replicationStore;
	
	private WritableByteChannel writableByteChannel;
	
	private AtomicBoolean stop = new AtomicBoolean(false);
	
	public InOutPayloadReplicationStore(ReplicationStore replicationStore) {
		this.replicationStore = replicationStore;
	}

	
	@Override
	public int doIn(ByteBuf byteBuf) throws IOException {
		
		if(logger.isDebugEnabled()){
			logger.debug("[doIn]" + byteBuf.readableBytes());
		}
		
		return replicationStore.getRdbStore().writeRdb(byteBuf);
	}

	@Override
	public void doStartOutput() throws IOException {
		
	}

	@Override
	public long doOut(WritableByteChannel writableByteChannel) throws IOException {
		
		this.writableByteChannel = writableByteChannel;
		replicationStore.getRdbStore().readRdbFile(this);
		return 0;
	}


	@Override
	public void doEndOutput() {
		stop.set(true);
	}


	@Override
	public void onFileData(FileChannel fileChannel, long pos, long len) throws IOException {
		
		if(len >= 0){
			fileChannel.transferTo(pos, len, writableByteChannel);
		}else{
			writableByteChannel.close();
		}
	}

	@Override
	public void exception(Exception e) {
		
		logger.error("[exception]", e);
		
		if(writableByteChannel != null){
			
			if(logger.isInfoEnabled()){
				logger.info("[exception][close write channel]" + writableByteChannel);
			}
			
			try {
				writableByteChannel.close();
			} catch (IOException e1) {
				logger.error("[exception]", e1);
			}
		}
	}

	@Override
	public void setRdbFileInfo(long rdbFileSize, long rdbFileOffset) {
		
	}


	@Override
   public boolean isStop() {
	   return stop.get();
   }


	@Override
	public void beforeFileData() {
	}


}
