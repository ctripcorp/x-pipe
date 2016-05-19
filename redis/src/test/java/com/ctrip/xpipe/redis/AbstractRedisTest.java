package com.ctrip.xpipe.redis;




import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.payload.ByteArrayWritableByteChannel;
import com.ctrip.xpipe.redis.keeper.CommandsListener;
import com.ctrip.xpipe.redis.keeper.RdbFileListener;
import com.ctrip.xpipe.redis.keeper.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.impl.DefaultReplicationStore;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

/**
 * @author wenchao.meng
 *
 * 2016年3月28日 下午5:54:09
 */
public abstract class AbstractRedisTest extends AbstractTest{

	protected ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;

	protected static final int runidLength = 40;

	protected ReplicationStore createReplicationStore() throws IOException{
		
		String tmpDir = getTestFileDir();
		
		if(logger.isInfoEnabled()){
			logger.info("[createReplicationStore]" + tmpDir);
		}
		ReplicationStore replicationStore = new DefaultReplicationStore(new File(tmpDir), 1024 * 1024);
		replicationStore.setMasterAddress(getMasterEndPoint());
		return replicationStore;
	}
	
	private Endpoint getMasterEndPoint() {
		return new DefaultEndPoint("redis://127.0.0.1:6379");
	}

	protected String readRdbFileTilEnd(ReplicationStore replicationStore) throws IOException, InterruptedException {
		
		final ByteArrayWritableByteChannel bachannel = new ByteArrayWritableByteChannel();
		final CountDownLatch latch = new CountDownLatch(1);
				
		
		replicationStore.readRdbFile(new RdbFileListener() {
			
			@Override
			public void setRdbFileInfo(long rdbFileSize, long rdbFileOffset) {
				
			}
			
			@Override
			public void onFileData(FileChannel fileChannel, long pos, long len) throws IOException {
				if(len == -1){
					latch.countDown();
					return;
				}
				fileChannel.transferTo(pos, len, bachannel);
			}
			
			@Override
			public boolean isStop() {
				return false;
			}
			
			@Override
			public void exception(Exception e) {
				latch.countDown();
			}
		});
		
		latch.await();
		return new String(bachannel.getResult());
	}
	
	public String readCommandFileTilEnd(ReplicationStore replicationStore) throws IOException {
		
		final List<ByteBuf>  buffs = new LinkedList<>();
		final AtomicInteger size = new AtomicInteger();
		
		replicationStore.addCommandsListener(replicationStore.beginOffset(), new CommandsListener() {
			
			@Override
			public void onCommand(ByteBuf byteBuf) {
				
				buffs.add(byteBuf);
				size.addAndGet(byteBuf.readableBytes());
			}
		});
		
		int lastSize = buffs.size();
		long equalCount = 0;
		while(true){
			
			int currentSize = buffs.size();
			if(currentSize != lastSize){
				lastSize = currentSize;
				equalCount = 0;
			}else{
				equalCount++;
			}
			if(equalCount > 10){
				break;
			}
			sleep(10);
		}
	
		byte []result = new byte[size.get()];
		int destIndex = 0;
		for(ByteBuf byteBuf : buffs){
			int readable = byteBuf.readableBytes();
			byteBuf.readBytes(result, destIndex, readable);
			Assert.assertEquals(0, byteBuf.readableBytes());
			destIndex += readable;
		}
		
		
		return new String(result);
	}
	
	protected String readLine(InputStream ins) throws IOException {
		
		StringBuilder sb = new StringBuilder();
		int last = 0;
		while(true){
			
			int data = ins.read();
			if(data == -1){
				return null;
			}
			sb.append((char)data);
			if(data == '\n' && last == '\r'){
				break;
			}
			last = data;
		}
		
		return sb.toString();
	}

	

}
