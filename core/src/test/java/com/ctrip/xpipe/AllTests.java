package com.ctrip.xpipe;

import com.ctrip.xpipe.api.sso.SsoConfigTest;
import com.ctrip.xpipe.command.*;
import com.ctrip.xpipe.concurrent.*;
import com.ctrip.xpipe.endpoint.ClusterShardHostPortTest;
import com.ctrip.xpipe.endpoint.DefaultEndPointTest;
import com.ctrip.xpipe.endpoint.TestAbstractLifecycle;
import com.ctrip.xpipe.gtid.GtidSetTest;
import com.ctrip.xpipe.lifecycle.*;
import com.ctrip.xpipe.netty.NettyPoolUtilTest;
import com.ctrip.xpipe.netty.NettyTimeoutTtlListenerTest;
import com.ctrip.xpipe.netty.TcpPortCheckCommandTest;
import com.ctrip.xpipe.netty.commands.DefaultNettyClientTest;
import com.ctrip.xpipe.netty.commands.NettyClientFactoryTest;
import com.ctrip.xpipe.netty.commands.RequestResponseCommandTest;
import com.ctrip.xpipe.netty.filechannel.ReferenceFileChannelTest;
import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayloadTest;
import com.ctrip.xpipe.payload.ByteArrayWritableByteChannelTest;
import com.ctrip.xpipe.payload.DirectByteBufInStringOutPayloadTest;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPoolTest;
import com.ctrip.xpipe.pool.XpipeNettyClientPoolTest;
import com.ctrip.xpipe.spring.DomainValidateFilterTest;
import com.ctrip.xpipe.spring.RestTemplateFactoryTest;
import com.ctrip.xpipe.tuple.PairTest;
import com.ctrip.xpipe.utils.*;
import com.ctrip.xpipe.utils.job.DynamicDelayPeriodTaskTest;
import com.ctrip.xpipe.utils.log.MdcUtilTest;
import com.ctrip.xpipe.zk.impl.DefaultZkConfigTest;
import com.ctrip.xpipe.zk.impl.TestZkClientTest;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * @author wenchao.meng
 * <p>
 * May 17, 2016 2:09:41 PM
 */
@RunWith(Suite.class)
@SuiteClasses({
        SsoConfigTest.class,
        XpipeNettyClientPoolTest.class,
        XpipeNettyClientKeyedObjectPoolTest.class,
        DefaultCommandFutureTest.class,
        DefaultEndPointTest.class,
        ClusterShardHostPortTest.class,
        ByteArrayOutputStreamPayloadTest.class,
        ByteArrayWritableByteChannelTest.class,
        TestAbstractLifecycle.class,
        DefaultLifecycleControllerTest.class,
        DefaultLifecycleStateTest.class,
        CreatedComponentRedistryTest.class,
        SpringComponentRegistryTest.class,
        DefaultRegistryTest.class,
        LifecycleObservableAbstractTest.class,
        OffsetNotifierTest.class,
        TcpPortCheckCommandTest.class,
        RequestResponseCommandTest.class,
        CommandRetryWrapperTest.class,
        DefaultRetryCommandFactoryTest.class,
        SequenceCommandChainTest.class,
        ParallelCommandChainTest.class,
        RestTemplateFactoryTest.class,
        ControllableFileAbstractTest.class,
        FileUtilsTest.class,
        GateTest.class,
        MathUtilTest.class,
        CloseStateTest.class,
        FinalStateSetterManagerTest.class,
        OneThreadTaskExecutorTest.class,
        TestZkClientTest.class,
        StringUtilTest.class,
        VersionUtilsTest.class,
        ReferenceFileChannelTest.class,
        ChannelUtilTest.class,
        KeyedOneThreadTaskExecutorTest.class,
        DefaultControllableFileTest.class,
        SizeControllableFileTest.class,
        UrlUtilsTest.class,
        DefaultExecutorFactoryTest.class,
        PairTest.class,
        DomainValidateFilterTest.class,
        CausalCommandTest.class,
        CausalChainTest.class,
        MutexableOneThreadTaskExecutorTest.class,
        KeyedOneThreadMutexableTaskExecutorTest.class,
        DirectByteBufInStringOutPayloadTest.class,
        NettyTimeoutTtlListenerTest.class,
        DefaultZkConfigTest.class,
        DefaultNettyClientTest.class,
        NettyClientFactoryTest.class,
        NettyPoolUtilTest.class,
        DynamicDelayPeriodTaskTest.class,
        MdcUtilTest.class,
        GtidSetTest.class,
})
public class AllTests {

    @BeforeClass
    public static void beforeAllTests() {
        System.setProperty("io.netty.allocator.useCacheForAllThreads", "false");
    }

}
