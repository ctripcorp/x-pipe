package com.ctrip.xpipe;

import com.ctrip.xpipe.api.sso.SsoConfigTest;
import com.ctrip.xpipe.concurrent.DefaultExecutorFactoryTest;
import com.ctrip.xpipe.concurrent.FinalStateSetterManagerTest;
import com.ctrip.xpipe.tuple.PairTest;
import com.ctrip.xpipe.utils.*;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.ctrip.xpipe.command.CommandRetryWrapperTest;
import com.ctrip.xpipe.command.DefaultCommandFutureTest;
import com.ctrip.xpipe.command.ParallelCommandChainTest;
import com.ctrip.xpipe.command.SequenceCommandChainTest;
import com.ctrip.xpipe.concurrent.KeyedOneThreadTaskExecutorTest;
import com.ctrip.xpipe.concurrent.OneThreadTaskExecutorTest;
import com.ctrip.xpipe.endpoint.DefaultEndPointTest;
import com.ctrip.xpipe.endpoint.TestAbstractLifecycle;
import com.ctrip.xpipe.lifecycle.CreatedComponentRedistryTest;
import com.ctrip.xpipe.lifecycle.DefaultLifecycleControllerTest;
import com.ctrip.xpipe.lifecycle.DefaultLifecycleStateTest;
import com.ctrip.xpipe.lifecycle.DefaultRegistryTest;
import com.ctrip.xpipe.lifecycle.SpringComponentRegistryTest;
import com.ctrip.xpipe.netty.commands.RequestResponseCommandTest;
import com.ctrip.xpipe.netty.filechannel.ReferenceFileChannelTest;
import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayloadTest;
import com.ctrip.xpipe.payload.ByteArrayWritableByteChannelTest;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPoolTest;
import com.ctrip.xpipe.pool.XpipeNettyClientPoolTest;
import com.ctrip.xpipe.zk.impl.TestZkClientTest;

/**
 * @author wenchao.meng
 *
 * May 17, 2016 2:09:41 PM
 */
@RunWith(Suite.class)
@SuiteClasses({
	SsoConfigTest.class,
	XpipeNettyClientPoolTest.class,
	XpipeNettyClientKeyedObjectPoolTest.class,
	DefaultCommandFutureTest.class,
	DefaultEndPointTest.class,
	ByteArrayOutputStreamPayloadTest.class,
	ByteArrayWritableByteChannelTest.class,
	TestAbstractLifecycle.class,
	DefaultLifecycleControllerTest.class,
	DefaultLifecycleStateTest.class,
	CreatedComponentRedistryTest.class,
	SpringComponentRegistryTest.class,
	DefaultRegistryTest.class,
	OffsetNotifierTest.class,
	RequestResponseCommandTest.class,
	CommandRetryWrapperTest.class,
	SequenceCommandChainTest.class,
	ParallelCommandChainTest.class,
	FileUtilsTest.class,
	MathUtilTest.class,
	FinalStateSetterManagerTest.class,
	OneThreadTaskExecutorTest.class,
	TestZkClientTest.class,
	StringUtilTest.class,
	ReferenceFileChannelTest.class,
	ChannelUtilTest.class,
	KeyedOneThreadTaskExecutorTest.class,
	DefaultControllableFileTest.class,
	SizeControllableFileTest.class,
	UrlUtilsTest.class,
	DefaultExecutorFactoryTest.class,
	PairTest.class,
})
public class AllTests {

}
