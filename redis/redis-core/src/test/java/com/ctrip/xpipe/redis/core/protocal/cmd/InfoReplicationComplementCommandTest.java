package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import org.junit.Test;

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

        InfoReplicationComplementCommand command = new InfoReplicationComplementCommand(getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint(host, port)), scheduled);
        logger.info("{}", command.execute().get());

    }

    @Test
    public void testFail(){

    }

}
