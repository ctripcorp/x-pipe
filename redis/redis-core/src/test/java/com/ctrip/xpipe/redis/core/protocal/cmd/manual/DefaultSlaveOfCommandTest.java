package com.ctrip.xpipe.redis.core.protocal.cmd.manual;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.protocal.cmd.DefaultSlaveOfCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.PingCommand;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;

/**
 * @author wenchao.meng
 *         <p>
 *         Feb 24, 2017
 */
public class DefaultSlaveOfCommandTest extends AbstractRedisTest {

    @Test
    public void testMultiServer() throws Exception {

        int begin = 10000;
        int count = 32;

        //make connection active
        for(int i = 0; i < count ;i++){
            int port = begin + i;
            SimpleObjectPool<NettyClient> keyPool = getXpipeNettyClientKeyedObjectPool().getKeyPool(new InetSocketAddress("127.0.0.1", port));
            new PingCommand(keyPool, scheduled).execute().get();
        }

        CountDownLatch latch = new CountDownLatch(count);

        for(int i = 0; i < count ;i++){
            int port = begin + i;
            SimpleObjectPool<NettyClient> keyPool = getXpipeNettyClientKeyedObjectPool().getKeyPool(new InetSocketAddress("127.0.0.1", port));
            long beginTime = System.currentTimeMillis();
            new DefaultSlaveOfCommand(keyPool, "127.0.0.1", 0, scheduled).execute(executors).addListener(new CommandFutureListener<String>() {
                @Override
                public void operationComplete(CommandFuture<String> commandFuture) throws Exception {

                    long endTime = System.currentTimeMillis();
                    long duration = endTime - beginTime;
                    if( duration > 50){
                        logger.warn("[timeout]127.0.0.1:{}, {}", port, duration);
                    }
                    latch.countDown();
                }
            });
        }
        latch.await();
        sleep(1000);
        logger.info("[testMultiServer][end]");
    }


}
