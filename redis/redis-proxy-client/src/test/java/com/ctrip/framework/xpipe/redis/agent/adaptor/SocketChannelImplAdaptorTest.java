package com.ctrip.framework.xpipe.redis.agent.adaptor;

import org.junit.Assert;
import org.junit.Before;
import org.objectweb.asm.MethodVisitor;


/**
 * @Author limingdong
 * @create 2021/4/23
 */
public class SocketChannelImplAdaptorTest extends AbstractSocketAdaptorTest {

    @Before
    public void setUp() {
        socketAdaptor = new SocketChannelImplAdaptor(classWriter);
    }

    protected void assertConnect(MethodVisitor methodVisitor) {
        Assert.assertTrue(methodVisitor instanceof SocketChannelImplAdaptor.ConnectMethodAdaptor);
    }
}