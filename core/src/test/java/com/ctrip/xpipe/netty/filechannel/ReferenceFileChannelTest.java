package com.ctrip.xpipe.netty.filechannel;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.utils.DefaultControllableFile;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *
 *         Nov 10, 2016
 */
public class ReferenceFileChannelTest extends AbstractTest {

	private ReferenceFileChannel referenceFileChannel;
	private int totalFileLen = 1 << 15;

	@Before
	public void beforeReferenceFileChannelTest() throws FileNotFoundException, IOException {

		String file = getTestFileDir() + "/" + getTestName();

		try (FileOutputStream ous = new FileOutputStream(new File(file))) {
			ous.write(randomString(totalFileLen).getBytes());
		}

		referenceFileChannel = new ReferenceFileChannel(new DefaultControllableFile(file));
	}

	@Test
	public void testCloseAfterRelease() throws IOException {

		DefaultReferenceFileRegion referenceFileRegion = referenceFileChannel.readTilEnd();

		referenceFileRegion.deallocate();
		Assert.assertFalse(referenceFileChannel.isFileChannelClosed());

		referenceFileChannel.close();
		Assert.assertTrue(referenceFileChannel.isFileChannelClosed());

	}

	@Test
	public void testCloseFirst() throws IOException {

		DefaultReferenceFileRegion referenceFileRegion = referenceFileChannel.readTilEnd();
		referenceFileChannel.close();

		Assert.assertFalse(referenceFileChannel.isFileChannelClosed());

		referenceFileRegion.deallocate();
		Assert.assertTrue(referenceFileChannel.isFileChannelClosed());
	}

	@Test
	public void testConcurrentRead() throws InterruptedException, IOException {

		int concurrentCount = 10;
		final LinkedBlockingQueue<DefaultReferenceFileRegion> fileRegions = new LinkedBlockingQueue<>();
		final CountDownLatch latch = new CountDownLatch(concurrentCount);
		

		for (int i = 0; i < concurrentCount; i++) {

			executors.execute(new AbstractExceptionLogTask() {

				private int count =0;
				@Override
				protected void doRun() throws Exception {
					
					try{
						while (true) {
							
							count++;
							DefaultReferenceFileRegion referenceFileRegion = referenceFileChannel.read(1);
							fileRegions.offer(referenceFileRegion);
							if(count > totalFileLen){
								logger.info("{}", referenceFileRegion);
							}
							if (referenceFileRegion.count() == 0) {
								break;
							}
							if (referenceFileRegion.count() < 0) {
								logger.error("{}", referenceFileRegion);
							}

						}
					}finally{
						latch.countDown();
					}
				}
			});
		}
		
		latch.await();
		referenceFileChannel.close();
		Assert.assertFalse(referenceFileChannel.isFileChannelClosed());

		long realTotalLen = 0;
		Set<Long> starts = new HashSet<>();
		while (true) {
			
			DefaultReferenceFileRegion referenceFileRegion = fileRegions.poll(100, TimeUnit.MILLISECONDS);
			if(referenceFileRegion == null){
				break;
			}
			
			if(referenceFileRegion.position() != totalFileLen){
				Assert.assertTrue(starts.add(referenceFileRegion.position()));
			}
			
			realTotalLen += referenceFileRegion.count();
			referenceFileRegion.release();
		}

		Assert.assertEquals(totalFileLen, realTotalLen);
		Assert.assertTrue(referenceFileChannel.isFileChannelClosed());

	}

}
