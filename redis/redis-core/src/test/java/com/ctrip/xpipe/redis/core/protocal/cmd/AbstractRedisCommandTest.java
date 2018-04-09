package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Mar 22, 2018
 */
public class AbstractRedisCommandTest extends AbstractRedisTest {

    //manual test
    @Test
    public void testLogRequest() throws Exception {
        XpipeNettyClientKeyedObjectPool keyedObjectPool = getXpipeNettyClientKeyedObjectPool();
        SimpleObjectPool<NettyClient> clientPool = keyedObjectPool
                .getKeyPool(new InetSocketAddress("127.0.0.1", 6379));
        ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1);
        InfoCommand infoCommand = new InfoCommand(clientPool, InfoCommand.INFO_TYPE.REPLICATION, scheduled);
        infoCommand.logResponse(false);
//        infoCommand.logRequest(false);
        infoCommand.execute().get();
    }

    @Test
    public void testDoReceiveResponse() throws Exception {
        AbstractRedisCommand<Object> redisCommand = new AbstractRedisCommand<Object>(
                getXpipeNettyClientKeyedObjectPool().getKeyPool(new InetSocketAddress("127.0.0.1", randomPort())), scheduled) {
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

}
