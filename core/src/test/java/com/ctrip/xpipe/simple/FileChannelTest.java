package com.ctrip.xpipe.simple;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import org.junit.Test;

import com.ctrip.xpipe.AbstractTest;

/**
 * @author wenchao.meng
 *
 * Dec 6, 2016
 */
public class FileChannelTest extends AbstractTest{
	
	@Test
	public void testChannelInterrupt() throws IOException{
		
		String testDir = getTestFileDir();
		RandomAccessFile file = new RandomAccessFile(new File(testDir, getTestName()), "rw");
		final FileChannel fileChannel = file.getChannel();
		
		Thread thread = new Thread(new Runnable() {
			
			@Override
			public void run() {
				while(true){
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

}
