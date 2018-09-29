package com.ctrip.xpipe.payload;

import io.netty.buffer.ByteBuf;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;


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
	public void doStartInput() {
		
		try {
			inFileChannel = new RandomAccessFile(fileName, "rw").getChannel();
		} catch (FileNotFoundException e) {
			throw new IllegalStateException("file not found:" + fileName, e);
		}
	}

	@Override
	public int doIn(ByteBuf byteBuf) throws IOException {
		
		
		int readerIndex = byteBuf.readerIndex();
		int n = inFileChannel.write(byteBuf.nioBuffer());
		byteBuf.readerIndex(readerIndex + n);
		return n;
	}

	
	@Override
	public void doEndInput() {
		try {
			inFileChannel.close();
		} catch (IOException e) {
			logger.error("[error closing file]" + fileName, e);
		}
	}

	@SuppressWarnings("resource")
	@Override
	public void doStartOutput() {
		
		try {
			outFileChannel = new RandomAccessFile(fileName, "rw").getChannel();
		} catch (FileNotFoundException e) {
			throw new IllegalStateException("file not found:" + fileName, e);
		}
	}
	
	@Override
	public long doOut(WritableByteChannel writableByteChannel) throws IOException {
		
		return outFileChannel.transferTo(0, outFileChannel.size(), writableByteChannel);
	}

	@Override
	protected void doTruncate(int reduceLen) throws IOException {
		outFileChannel.truncate(outFileChannel.size() - reduceLen);
	}
	
	@Override
	public void doEndOutput() {
		try {
			outFileChannel.close();
		} catch (IOException e) {
			logger.error("[error closing file]" + fileName, e);
		}
	}

}
