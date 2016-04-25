package com.ctrip.xpipe.redis.tools;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ctrip.xpipe.redis.keeper.CommandsListener;
import com.ctrip.xpipe.redis.keeper.RdbFile;
import com.ctrip.xpipe.redis.keeper.ReplicationStore;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * just for unit test, write only
 * @author wenchao.meng
 *
 * 2016年4月21日 上午11:03:53
 */
public class SimpleFileReplicationStore implements ReplicationStore{
	
	private Logger logger = LogManager.getLogger(getClass());
	
	public String  rdbFile;
	private String commandsFile;
	
	private FileChannel rdbFileChannel, commandsFileChannel;
	
	private String masterRunId;
	private long masterOffset;
	
	private ExecutorService executors = Executors.newCachedThreadPool();
	
	private Map<CommandsListener, CommandsTask>  listeners = new ConcurrentHashMap<CommandsListener, CommandsTask>();
	
	public SimpleFileReplicationStore(String rdbFile, String commandsFile) {
		this.rdbFile = rdbFile;
		this.commandsFile = commandsFile;
	}

	@Override
	public void close() throws IOException {
		
	}

	@Override
	public void beginRdb(String masterRunid, long masterOffset) {
		
		this.masterRunId = masterRunid;
		this.masterOffset = masterOffset;
		
	}

	@Override
	public int writeRdb(ByteBuf byteBuf) throws IOException {
		
		createRdbFileChannel();
		
		return write(rdbFileChannel, byteBuf);
	}

	private void createRdbFileChannel() {
		
		if(rdbFileChannel == null){
			synchronized (this) {
				if(rdbFileChannel == null){
					rdbFileChannel = createFileChannel(rdbFile);
				}
			}
		}
	}

	private void createCommandsFileChannel() {
		
		if(commandsFileChannel == null){
			synchronized (this) {
				if(commandsFileChannel == null){
					commandsFileChannel = createFileChannel(commandsFile);
				}
			}
		}
	}

	@SuppressWarnings("resource")
	private FileChannel createFileChannel(String file) {
		try {
			return new RandomAccessFile(new File(file), "rw").getChannel();
		} catch (FileNotFoundException e) {
			throw new IllegalStateException("[beginRdb]" + rdbFile, e);
		} 
	}

	@Override
	public void endRdb() {
		try {
			this.rdbFileChannel.close();
		} catch (IOException e) {
			logger.error("[endRdb]" + rdbFile, e);
		}
	}

	@Override
	public String getMasterRunid() {
		return masterRunId;
	}

	@Override
	public RdbFile getRdbFile() {
		return new RdbFile() {
			
			@Override
			public long getRdboffset() {
				return masterOffset;
			}
			
			@SuppressWarnings("resource")
			@Override
			public FileChannel getRdbFile() {
				
				try {
					return new RandomAccessFile(new File(rdbFile), "rw").getChannel();
				} catch (FileNotFoundException e) {
					logger.error("[getRdbFile]" + rdbFile, e);
					throw new RuntimeException("[getRdbFile]" + rdbFile, e);
				}
			}
		};
	}

	@Override
	public int appendCommands(ByteBuf byteBuf) throws IOException {
		
		createCommandsFileChannel();
		return write(commandsFileChannel, byteBuf);
	}

	private int write(FileChannel fileChannel, ByteBuf byteBuf) throws IOException {

		int readerIndex = byteBuf.readerIndex();
		int n = fileChannel.write(byteBuf.nioBuffer());
		byteBuf.readerIndex(readerIndex + n);
		return n;
	}

	@Override
	public void addCommandsListener(long offset, CommandsListener commandsListener) {
		
		CommandsTask task = new CommandsTask(offset, commandsListener);
		listeners.put(commandsListener, task);
		executors.execute(task);
	}

	@Override
	public void removeCommandsListener(CommandsListener commandsListener) {
		
		CommandsTask task = listeners.get(commandsListener);
		task.setStop(true);
		listeners.remove(commandsListener);
	}

	@Override
	public long beginOffset() {
		return masterOffset + 1;
	}

	@Override
	public long endOffset() {
		return 0;
	}

	@Override
	public void delete() {
		
	}

	
	class CommandsTask implements Runnable{
		
		@SuppressWarnings("unused")
		private long offset;
		private CommandsListener commandsListener;
		
		private volatile boolean stop = false;
		
		public CommandsTask(long offset, CommandsListener commandsListener) {
			
			this.offset = offset;
			this.commandsListener = commandsListener;
					
		}

		public void setStop(boolean stop) {
			this.stop = stop;
		}

		@SuppressWarnings("resource")
		@Override
		public void run() {
			
			FileChannel fileChannel = null;
			try{
				
				ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
				fileChannel = new RandomAccessFile(commandsFile, "rw").getChannel();
				while(true){
					
					if(stop){
						if(logger.isInfoEnabled()){
							logger.info("[run][stop]");
						}
						break;
					}
					
					byteBuffer.clear();
					int size = fileChannel.read(byteBuffer);
					if(size == -1){
						break;
					}
					byteBuffer.flip();
					commandsListener.onCommand(Unpooled.wrappedBuffer(byteBuffer));
				}
				
			} catch (IOException e) {
				logger.error("[run]" + commandsFile, e);
			}finally{
				if( fileChannel != null ){
					try {
						fileChannel.close();
					} catch (IOException e) {
						logger.error("[run]" + commandsFile, e);
					}
				}
			}
		}
	} 
}
