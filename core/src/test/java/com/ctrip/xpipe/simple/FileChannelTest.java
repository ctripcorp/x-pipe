package com.ctrip.xpipe.simple;

import com.ctrip.xpipe.AbstractTest;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;

/**
 * @author wenchao.meng
 *
 *         Dec 6, 2016
 */
public class FileChannelTest extends AbstractTest {

	private RandomAccessFile file;
	private Thread testThread;

	@Test
	public void testChannelInterrupt() throws IOException, InterruptedException {

		String testDir = getTestFileDir();
		file = new RandomAccessFile(new File(testDir, getTestName()), "rw");
		final FileChannel fileChannel = file.getChannel();

		testThread = new Thread(new Runnable() {

			@Override
			public void run() {
				while (!Thread.currentThread().isInterrupted()) {
					try {
						fileChannel.size();
						// Small sleep to avoid busy loop
						Thread.sleep(10);
					} catch (ClosedChannelException e) {
						// Channel closed, exit loop
						break;
					} catch (InterruptedException e) {
						// Thread interrupted, exit loop
						Thread.currentThread().interrupt();
						break;
					} catch (IOException e) {
						// Other IO exceptions, continue but check interrupt
						if (Thread.currentThread().isInterrupted()) {
							break;
						}
					}
				}
			}
		});

		testThread.start();

		// Wait a bit to let thread start
		Thread.sleep(100);
		
		// Interrupt the thread
		testThread.interrupt();
		
		// Wait for thread to finish (with timeout)
		testThread.join(2000);
	}

	@After
	public void afterFileChannelTest() throws IOException {
		
		// Stop the test thread first
		if (testThread != null && testThread.isAlive()) {
			testThread.interrupt();
			try {
				testThread.join(1000); // Wait up to 1 second for thread to finish
			} catch (InterruptedException e) {
				// Ignore
			}
		}
		
		if (file != null) {
			file.close();
		}
	}
}
