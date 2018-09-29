package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.CommandTimeoutException;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.simpleserver.Server;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 19, 2017
 */
public class RedisCommandTest extends AbstractRedisTest{

    private int timeoutMilli = 50;

    @Test
    public void testTimeoutNext() throws Exception {

        Server server = startEchoPrefixServer(String.valueOf((char)RedisClientProtocol.PLUS_BYTE));

        SimpleObjectPool<NettyClient> keyPool = getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", server.getPort()));

        int sleepTime = timeoutMilli + 50;
        String str1 = String.format("sleep %d %s\r\n", sleepTime, randomString(10));
        String str2 = randomString(10) + "\r\n";

        try{
            new TestCommand(str1, timeoutMilli, keyPool, scheduled).execute().get();
            Assert.fail();
        }catch (ExecutionException e){
            Assert.assertTrue(e.getCause() instanceof CommandTimeoutException);
        }

        sleep(sleepTime * 2);

        new TestCommand(str2, sleepTime, keyPool, scheduled).execute().get();
    }


    public static class TestCommand extends AbstractRedisCommand<String>{

        private String buff;
        private int timeoutMilli;
        public TestCommand(String buff, int timeoutMilli, SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
            super(clientPool, scheduled);
            this.buff = buff;
            this.timeoutMilli = timeoutMilli;
        }

        @Override
        public ByteBuf getRequest() {
            return Unpooled.wrappedBuffer(buff.getBytes());
        }

        @Override
        protected String format(Object payload) {

            String result = payloadToString(payload);
            if(!buff.trim().equals(result)){
                throw new IllegalStateException("expected:" + buff + ",but:" + result);
            }
            return result;
        }

        @Override
        public int getCommandTimeoutMilli() {
            return timeoutMilli;
        }
    }


}
