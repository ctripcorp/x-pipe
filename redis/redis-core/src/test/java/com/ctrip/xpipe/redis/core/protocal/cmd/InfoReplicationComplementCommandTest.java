package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import org.junit.Test;

import java.net.InetSocketAddress;

/**
 * @author wenchao.meng
 *         <p>
 *         Sep 08, 2017
 */
public class InfoReplicationComplementCommandTest extends AbstractRedisTest{

    private String host = "127.0.0.1";
    private int port = 6379;

    @Test
    public void testSuccess() throws Exception {

        InfoReplicationComplementCommand command = new InfoReplicationComplementCommand(getXpipeNettyClientKeyedObjectPool().getKeyPool(new InetSocketAddress(host, port)), scheduled);
        logger.info("{}", command.execute().get());

    }

    @Test
    public void testFail(){

    }

}
