package com.ctrip.xpipe.redis.server;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ctrip.xpipe.redis.keeper.CommandsListener;
import com.ctrip.xpipe.redis.keeper.ReplicationStore;

import io.netty.buffer.ByteBuf;
import io.netty.channel.FileRegion;

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
	public FileRegion getRdbFile() {
		return null;
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
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removeCommandsListener(CommandsListener commandsListener) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public long beginOffset() {
		return masterOffset + 1;
	}

	@Override
	public long endOffset() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void delete() {
		// TODO Auto-generated method stub
		
	}

}
