package com.ctrip.framework.xpipe.redis.instrument.adapter;

import com.alibaba.deps.org.objectweb.asm.MethodVisitor;
import org.junit.Assert;
import org.junit.Before;


/**
 * @Author limingdong
 * @create 2021/4/23
 */
public class SocketChannelImplAdapterTest extends AbstractSocketAdapterTest {

    @Before
    public void setUp() {
        socketAdaptor = new SocketChannelImplAdapter(classWriter);
    }

    protected void assertConnect(MethodVisitor methodVisitor) {
        Assert.assertTrue(methodVisitor instanceof SocketChannelImplAdapter.ConnectMethodAdapter);
    }
}