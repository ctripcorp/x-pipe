package com.ctrip.xpipe.redis.keeper.store;


import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.redis.core.store.CommandsListener;
import com.ctrip.xpipe.utils.ThreadUtils;

import io.netty.buffer.Unpooled;

public class DefaultCommandNotifier implements CommandNotifier {
	
	private Logger logger = LoggerFactory.getLogger(getClass());

	private CommandsListener listener;

	private CommandReader cmdReader;
	
	public static final String THREAD_PREFIX = "COMMAND_NOTIFIER";

	@Override
	public void start(CommandStore store, long startOffset, CommandsListener listener) throws IOException {
		this.listener = listener;
		cmdReader = store.beginRead(startOffset);

		ThreadUtils.newThread(THREAD_PREFIX + listener, new NotifyTask()).start();
	}

	@Override
	public void close() throws IOException {
		cmdReader.close();
	}

	private class NotifyTask implements Runnable {
		
		public void run() {
			try{
				while (true) {
					int read = 0;
					ByteBuffer dst = ByteBuffer.allocate(4096);
					read = read(dst);
					if (read > 0) {
						dst.flip();
						listener.onCommand(Unpooled.wrappedBuffer(dst));
					} else {
						sleep();
					}
				}
			}catch(Throwable th){
				logger.error("[run][exit]" + listener, th);
			}

		}

		private int read(ByteBuffer dst) {
			int read = 0;
			try {
				read = cmdReader.read(dst);
			} catch (IOException e) {
				logger.error("[read]" + listener, e);
			}
			return read;
		}

		// TODO
		private void sleep() {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
	}

}
