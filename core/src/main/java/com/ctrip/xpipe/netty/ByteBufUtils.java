package com.ctrip.xpipe.netty;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.codec.Codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;

/**
 * @author wenchao.meng
 *
 * Aug 26, 2016
 */
public class ByteBufUtils {
	
	private static Logger logger = LoggerFactory.getLogger(ByteBufferUtils.class);
	
	public static byte[] readToBytes(ByteBuf byteBuf){

		
		if(byteBuf instanceof CompositeByteBuf){
			CompositeByteBuf compositeByteBuf = (CompositeByteBuf) byteBuf;
			ByteArrayOutputStream baous = new ByteArrayOutputStream();
			for(ByteBuf single : compositeByteBuf){
				try {
					baous.write(readToBytes(single));
				} catch (IOException e) {
					throw new IllegalStateException("write to ByteArrayOutputStream error", e);
				}
			}
			return baous.toByteArray();
		}else{
			byte []result = new byte[byteBuf.readableBytes()];
			byteBuf.readBytes(result);
			return result;
		}
	}

	public static String readToString(ByteBuf byteBuf){
		
		byte []result = readToBytes(byteBuf);
		return new String(result,Codec.defaultCharset);
	}
	
	
	public static int writeByteBufToFileChannel(ByteBuf byteBuf, FileChannel fileChannel) throws IOException{

		int wrote = 0;
		try{
			ByteBuffer buf = byteBuf.internalNioBuffer(byteBuf.readerIndex(), byteBuf.readableBytes());
			if(logger.isDebugEnabled()){
				logger.debug("[appendCommands]{}", ByteBufferUtils.readToString(buf.slice()));
			}
			wrote += fileChannel.write(buf);
		}catch(Exception e){
			
			logger.info("[appendCommands]", e);
			ByteBuffer[] buffers = byteBuf.nioBuffers();
			// TODO ensure all read
			if (buffers != null) {
				for (ByteBuffer buf : buffers) {
					if(logger.isDebugEnabled()){
						logger.debug("[appendCommands]{}", ByteBufferUtils.readToString(buf.slice()));
					}
					wrote += fileChannel.write(buf);
				}
			}
		}
		byteBuf.readerIndex(byteBuf.writerIndex());
		
		return wrote;
	}

}
