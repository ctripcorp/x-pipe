package com.ctrip.xpipe.simple;

import com.ctrip.xpipe.AbstractTest;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * @author wenchao.meng
 *
 *         Dec 6, 2016
 */
public class FileChannelTest extends AbstractTest {

	private RandomAccessFile file;

	@Test
	public void testChannelInterrupt() throws IOException {

		String testDir = getTestFileDir();
		file = new RandomAccessFile(new File(testDir, getTestName()), "rw");
		final FileChannel fileChannel = file.getChannel();

		Thread thread = new Thread(new Runnable() {

			@Override
			public void run() {
				while (true) {
					try {
						fileChannel.size();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		});

		thread.start();

		thread.interrupt();

		waitForAnyKey();
	}

	@After
	public void afterFileChannelTest() throws IOException {
		
		if (file != null) {
			file.close();
		}
	}
}
