package com.ctrip.xpipe.payload;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

import io.netty.buffer.ByteBuf;


/**
 * @author wenchao.meng
 *
 * 2016年3月29日 下午4:33:20
 */
public class FileInOutPayload extends AbstractInOutPayload{
	
	public String fileName;
	private FileChannel inFileChannel; 
	private FileChannel outFileChannel; 
	
	public FileInOutPayload(String fileName) {
		this.fileName = fileName;
	}

	@SuppressWarnings("resource")
	@Override
	public void startInput() {
		
		try {
			inFileChannel = new RandomAccessFile(fileName, "rw").getChannel();
		} catch (FileNotFoundException e) {
			throw new IllegalStateException("file not found:" + fileName, e);
		}
	}

	@Override
	public int in(ByteBuf byteBuf) throws IOException {
		
		
		int readerIndex = byteBuf.readerIndex();
		int n = inFileChannel.write(byteBuf.nioBuffer());
		byteBuf.readerIndex(readerIndex + n);
		return n;
	}

	
	@Override
	public void endInput() {
		try {
			inFileChannel.close();
		} catch (IOException e) {
			logger.error("[error closing file]" + fileName, e);
		}
	}

	@SuppressWarnings("resource")
	@Override
	public void startOutput() {
		
		try {
			outFileChannel = new RandomAccessFile(fileName, "rw").getChannel();
		} catch (FileNotFoundException e) {
			throw new IllegalStateException("file not found:" + fileName, e);
		}
	}
	
	@Override
	public long out(WritableByteChannel writableByteChannel) throws IOException {
		
		return outFileChannel.transferTo(0, outFileChannel.size(), writableByteChannel);
	}

	@Override
	public void endOutput() {
		try {
			outFileChannel.close();
		} catch (IOException e) {
			logger.error("[error closing file]" + fileName, e);
		}
	}
}
