package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import org.junit.Test;

/**
 * @author wenchao.meng
 *         <p>
 *         Sep 08, 2017
 */
public class InfoReplicationCommandTest extends AbstractRedisTest{

    private String host = "127.0.0.1";
    private int port = 6000;

    @Test
    public void testMasterInfo() throws Exception {

        InfoReplicationCommand command = new InfoReplicationCommand(getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint(host, port)), scheduled);
        logger.info("{}", command.execute().get());
    }

    @Test
    public void testSplit(){

        String replication = "# Replication\r\n" +
                "role:master\r\n" +
                "connected_slaves:1\r\n" +
                "slave0:ip=127.0.0.1,port=6479,state=online,offset=312553,lag=1\r\n" +
                "master_replid:f1646795ab3b2a0189e5458bc2cbf85413d0a3be\r\n" +
                "master_replid2:0000000000000000000000000000000000000000\r\n" +
                "master_repl_offset:312949\r\n" +
                "second_repl_offset:-1\r\n" +
                "repl_backlog_active:1\r\n" +
                "repl_backlog_size:1048576\r\n" +
                "repl_backlog_first_byte_offset:1\r\n" +
                "repl_backlog_histlen:312949";

        for(String sp : replication.split("\\s+")){
            logger.info("{}", sp);
        }
        String[] ns = "abc".split("n");
        logger.info("{}", ns);
    }

}
