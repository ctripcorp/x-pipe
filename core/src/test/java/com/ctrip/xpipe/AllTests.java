package com.ctrip.xpipe;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.ctrip.xpipe.endpoint.DefaultEndPointTest;
import com.ctrip.xpipe.endpoint.TestAbstractLifecycle;
import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayloadTest;
import com.ctrip.xpipe.payload.ByteArrayWritableByteChannelTest;

/**
 * @author wenchao.meng
 *
 * May 17, 2016 2:09:41 PM
 */
@RunWith(Suite.class)
@SuiteClasses({
	DefaultEndPointTest.class,
	ByteArrayOutputStreamPayloadTest.class,
	ByteArrayWritableByteChannelTest.class,
	TestAbstractLifecycle.class
})
public class AllTests {

}
