package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import com.ctrip.xpipe.simpleserver.Server;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * Mar 22, 2018
 */
public class TestAbstractRedisCommandTest extends AbstractRedisTest {

    //manual test
    @Test
    public void testLogRequest() throws Exception {
        XpipeNettyClientKeyedObjectPool keyedObjectPool = getXpipeNettyClientKeyedObjectPool();
        SimpleObjectPool<NettyClient> clientPool = keyedObjectPool
                .getKeyPool(new DefaultEndPoint("127.0.0.1", 6379));
        ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1);
        InfoCommand infoCommand = new InfoCommand(clientPool, InfoCommand.INFO_TYPE.REPLICATION, scheduled);
        infoCommand.logResponse(false);
//        infoCommand.logRequest(false);
        infoCommand.execute().get();
    }

    @Test
    public void testDoReceiveResponse() throws Exception {
        AbstractRedisCommand<Object> redisCommand = new AbstractRedisCommand<Object>(
                getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", randomPort())), scheduled) {
            @Override
            protected Object format(Object payload) {
                return payload;
            }

            @Override
            public ByteBuf getRequest() {
                return new RequestStringParser(getName(), "test").format();
            }
        };

        redisCommand.doReceiveResponse(null, Unpooled.copiedBuffer(":1\r\n".getBytes()));
    }

    @Test
    public void testCommandTimeout() throws Exception {
        Server server = startServerWithFlexibleResult(new Callable<String>() {
            @Override
            public String call() throws Exception {
                sleep(50);
                return "+PONG\r\n";
            }
        });
        SimpleObjectPool<NettyClient> pool = getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint(getTimeoutIp(), 10010));
        pool = spy(pool);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Object result = invocation.callRealMethod();
                sleep(500);
                return result;
            }
        }).when(pool).borrowObject();
        PingCommand pingCommand = new PingCommand(pool, scheduled);
        pingCommand.execute();
        waitConditionUntilTimeOut(()->pingCommand.future().isDone(), 1000);
        verify(pool, times(1)).borrowObject();
    }

}
