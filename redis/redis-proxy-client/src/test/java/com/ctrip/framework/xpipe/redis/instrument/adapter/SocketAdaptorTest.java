package com.ctrip.framework.xpipe.redis.instrument.adapter;

import com.ctrip.framework.xpipe.redis.asm.MethodVisitor;
import org.junit.Assert;
import org.junit.Before;


/**
 * @Author limingdong
 * @create 2021/4/22
 */
public class SocketAdaptorTest extends AbstractSocketAdaptorTest {

    @Before
    public void setUp() {
        socketAdaptor = new SocketAdapter(classWriter);
    }

    protected void assertConnect(MethodVisitor methodVisitor) {
        Assert.assertTrue(methodVisitor instanceof SocketAdapter.ConnectMethodAdaptor);
    }

}