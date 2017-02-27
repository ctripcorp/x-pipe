package com.ctrip.xpipe.utils;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;

/**
 * @author wenchao.meng
 *
 * Jan 20, 2017
 */
public class DefaultControllableFileTest extends AbstractTest{

	private File file;
	private DefaultControllableFile controllableFile;

	@Before
	public void beforeDefaultControllableFileTest() throws IOException{
		file = new File(getTestFileDir() + "/" + getTestName());
		controllableFile = new DefaultControllableFile(file);

	}
	
	@Test
	public void testConcurrentRead() throws IOException, InterruptedException{
		
		int concurrent = 10;
		
		List<FileChannel> channels = new LinkedList<>();
		CountDownLatch latch = new CountDownLatch(concurrent);

		for(int i=0;i<concurrent;i++){
			
			executors.execute(new AbstractExceptionLogTask() {
				
				@Override
				protected void doRun() throws Exception {
					try{
						FileChannel fileChannel = controllableFile.getFileChannel();
						synchronized (channels) {
							channels.add(fileChannel);
						}
					}finally{
						latch.countDown();
					}
				}
			});
		}

		latch.await();
		
		logger.info("{}", channels.size());
		Assert.assertEquals(concurrent, channels.size());
		for(int i=1;i<channels.size();i++){
			Assert.assertEquals(channels.get(0), channels.get(i));
		}
		
	}

	
	@After
	public void afterDefaultControllableFileTest() throws IOException{
		controllableFile.close();
	}
}
