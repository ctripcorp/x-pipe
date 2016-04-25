package com.ctrip.xpipe.redis.keeper.impl;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.ctrip.xpipe.redis.keeper.CommandsListener;

import io.netty.buffer.Unpooled;

public class DefaultCommandNotifier implements CommandNotifier {

	private CommandsListener listener;

	private CommandReader cmdReader;

	@Override
	public void start(CommandStore store, long startOffset, CommandsListener listener) throws IOException {
		this.listener = listener;
		cmdReader = store.beginRead(startOffset);

		// TODO thread factory
		new Thread(new NotifyTask()).start();
	}

	@Override
	public void close() throws IOException {
		cmdReader.close();
	}

	private class NotifyTask implements Runnable {
		public void run() {
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

		}

		private int read(ByteBuffer dst) {
			int read = 0;
			try {
				read = cmdReader.read(dst);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
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
