package com.ctrip.xpipe.redis.meta.server.dchange.impl;

import com.ctrip.xpipe.exception.SIMPLE_RETURN_CODE;
import com.ctrip.xpipe.exception.SimpleErrorMessage;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.PingCommand;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.dcchange.impl.AtLeastOneChecker;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * @author wenchao.meng
 *         <p>
 *         Dec 1, 2016
 */
public class AtLeastOneCheckerTest extends AbstractMetaServerTest {

    private int timeoutMilli = 100;

    @Before
    public void beforeAtLeastOneCheckerTest(){
        PingCommand.DEFAULT_PINT_TIME_OUT_MILLI = timeoutMilli;
    }

    @Test
    public void testCheckerTimeout() throws Exception {

        Server server = startServer((String) null);
        List<RedisMeta> redises = new LinkedList<>();
        redises.add(new RedisMeta().setIp("localhost").setPort(server.getPort()));
        SimpleErrorMessage check = new AtLeastOneChecker(redises, getXpipeNettyClientKeyedObjectPool(), scheduled).check();

        Assert.assertEquals(SIMPLE_RETURN_CODE.FAIL, check.getErrorType());
    }

    @Test
    public void testCheckerSuccess() throws Exception {

        int count = 3;
        List<RedisMeta> redises = new LinkedList<>();

        Set<Integer> ports = randomPorts(count);
        int i = 0;
        for (int port : ports) {
            if( i == count -1){
                Server server = startServer(port,toRedisProtocalString("PONG"));
            }
            redises.add(new RedisMeta().setIp("localhost").setPort(port));
            i++;
        }
        SimpleErrorMessage check = new AtLeastOneChecker(redises, getXpipeNettyClientKeyedObjectPool(), scheduled).check();
        Assert.assertEquals(SIMPLE_RETURN_CODE.SUCCESS, check.getErrorType());
    }

    @Test
    public void testCheckerFail() throws Exception {

        int count = 3;
        List<RedisMeta> redises = new LinkedList<>();

        Set<Integer> ports = randomPorts(count);
        for (int port : ports) {
            redises.add(new RedisMeta().setIp("localhost").setPort(port));
        }
        SimpleErrorMessage check = new AtLeastOneChecker(redises, getXpipeNettyClientKeyedObjectPool(), scheduled).check();
        Assert.assertEquals(SIMPLE_RETURN_CODE.FAIL, check.getErrorType());
        logger.info("[testCheckerFail]{}", check.getErrorMessage());
    }

    @Test
    public void testCheckerFailTimeout() throws Exception {

        int port = randomPort();

        List<RedisMeta> redises = new LinkedList<>();
        redises.add(new RedisMeta().setIp("localhost").setPort(port));

        startServer(port, new Callable<String>() {
            @Override
            public String call() throws Exception {
                sleep(timeoutMilli * 2);
                return toRedisProtocalString("PONG");
            }
        });

        SimpleErrorMessage check = new AtLeastOneChecker(redises, getXpipeNettyClientKeyedObjectPool(), scheduled).check();
        Assert.assertEquals(SIMPLE_RETURN_CODE.FAIL, check.getErrorType());
        logger.info("[testCheckerFail]{}", check.getErrorMessage());
    }


}
