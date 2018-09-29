package com.ctrip.xpipe.api.payload;


import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

/**
 * @author wenchao.meng
 *
 * 2016年3月29日 下午4:30:45
 */
public interface InOutPayload {

	void startInput();
	
	long inputSize();
	
	int in(ByteBuf byteBuf) throws IOException;
	
	void endInput() throws IOException;

	void endInputTruncate(int reduceLen) throws IOException;

	void startOutput() throws IOException;
	
	long outSize();

	long out(WritableByteChannel writableByteChannel) throws IOException;
	
	void endOutput();

}
