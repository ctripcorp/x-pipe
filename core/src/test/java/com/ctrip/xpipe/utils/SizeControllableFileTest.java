package com.ctrip.xpipe.utils;


import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.utils.FileSize;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author wenchao.meng
 *
 *         Jan 5, 2017
 */
public class SizeControllableFileTest extends AbstractTest {

	private File file;
	private SizeControllableFile sizeControllableFile;
	private AtomicInteger openCount = new AtomicInteger();
	private int testCount = 100;

	@Before
	public void beforeSizeControllableFileTest() {

		file = new File(String.format("%s/%s.data", getTestFileDir(), getTestName()));
	}

	@Test
	public void testSize() throws IOException {

		sizeControllableFile = new SizeControllableFile(file, new FileSize() {

			@Override
			public long getSize(LongSupplier realSizeProvider) {
				return realSizeProvider.getAsLong() - 100;
			}
		});

		long totalLen = 0;
		for (int i = 0; i < testCount; i++) {

			int dataLen = randomInt(1, 1024);
			sizeControllableFile.getFileChannel().write(ByteBuffer.wrap(randomString(dataLen).getBytes()));

			totalLen += dataLen;
			Assert.assertEquals(totalLen - 100, sizeControllableFile.size());
		}
	}

	@Test
	public void testLazyOpen() throws IOException {

		sizeControllableFile = new SizeControllableFile(file, new FileSize() {

			@Override
			public long getSize(LongSupplier realSizeProvider) {
				return realSizeProvider.getAsLong();
			}
		}) {

			@Override
			protected void doOpen() throws IOException {
				openCount.incrementAndGet();
				super.doOpen();
			}
		};

		Assert.assertEquals(0, openCount.get());

		for (int i = 0; i < testCount; i++) {

			sizeControllableFile.getFileChannel();
			sizeControllableFile.size();
			Assert.assertEquals(1, openCount.get());
		}

		sizeControllableFile.close();

		for (int i = 0; i < testCount; i++) {
			assertThrows(XpipeRuntimeException.class , () -> {
				sizeControllableFile.getFileChannel();
			});

			assertThrows(XpipeRuntimeException.class , () -> {
				sizeControllableFile.size();
			});


		}
	}

	@After
	public void afterSizeControllableFileTest() throws IOException {

		if (sizeControllableFile != null) {
			sizeControllableFile.close();
		}
	}
}
