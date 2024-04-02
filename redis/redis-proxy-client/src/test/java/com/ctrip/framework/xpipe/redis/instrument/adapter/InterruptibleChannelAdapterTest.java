package com.ctrip.framework.xpipe.redis.instrument.adapter;

import com.alibaba.deps.org.objectweb.asm.MethodVisitor;
import org.junit.Before;

/**
 * @author lishanglin
 * date 2022/2/11
 */
public class InterruptibleChannelAdapterTest extends AbstractSocketAdapterTest {

    @Before
    public void setUp() {
        socketAdaptor = new SocketAdapter(classWriter);
    }

    protected void assertConnect(MethodVisitor methodVisitor) {

    }

}
