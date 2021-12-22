package com.ctrip.xpipe.redis.core.protocal.cmd.pubsub;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.simpleserver.AbstractIoAction;
import com.ctrip.xpipe.simpleserver.IoAction;
import com.ctrip.xpipe.simpleserver.IoActionFactory;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public class CrdtSubscribeCommandTest extends AbstractTest {

    private Server redis;

    private static final String TEMP_SUB_HEADER = "*3\r\n" +
            "$13\r\n" +
            "crdtsubscribe\r\n" +
            "$%d\r\n" +
            "%s\r\n" +
            ":1\r\n";

    private static final String TEMP_SUB_MESSAGE = "*3\r\n" +
            "$7\r\n" +
            "message\r\n" +
            "$%d\r\n%s\r\n$%d\r\n%s\r\n";

    private static final String testMessage = "test";

    private String channel;

    @Before
    public void setupCrdtSubscribeCommandTest() throws Exception {

        redis = startServer(randomPort(), new IoActionFactory() {
            @Override
            public IoAction createIoAction(Socket socket) {
                return new AbstractIoAction(socket) {

                    private String readLine = null;

                    @Override
                    protected void doWrite(OutputStream ous, Object readResult) throws IOException {
                        try {
                            String[] sp = ((String)readResult).split("\\s+");

                            if (sp.length == 2 && sp[0].toLowerCase().startsWith("crdt.subscribe")) {
                                channel = sp[1];

                                ous.write(String.format(TEMP_SUB_HEADER, channel.length(), channel).getBytes());
                                ous.flush();
                                sleep(10);
                                while (true) {
                                    ous.write(String.format(TEMP_SUB_MESSAGE, channel.length(), channel, testMessage.length(), testMessage).getBytes());
                                    ous.flush();
                                    sleep(10);
                                }
                            } else {
                                ous.write("+OK\r\n".getBytes());
                                ous.flush();
                            }

                        } catch (Exception e) {
                            logger.error("[doWrite] " + e.getMessage());
                        }
                    }

                    @Override
                    protected Object doRead(InputStream ins) throws IOException {
                        readLine = readLine(ins);
                        logger.info("[doRead]{}", readLine == null ? null : readLine.trim());
                        return readLine;
                    }
                };
            }
        });
    }

    @After
    public void afterCrdtSubscribeCommandTest() throws Exception {
        if (null != redis) redis.stop();
    }

    @Test
    public void testCrdtSub() throws Exception {
        SimpleObjectPool<NettyClient> clientPool = getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", redis.getPort()));
        Subscribe crdtSubCmd = new CRDTSubscribeCommand(clientPool, scheduled, 2000, "test-channel");
        AtomicBoolean messageGot = new AtomicBoolean(false);
        AtomicBoolean unsubscribe = new AtomicBoolean(false);
        crdtSubCmd.addChannelListener(new SubscribeListener() {
            @Override
            public void message(String channel, String message) {
                Assert.assertEquals(CrdtSubscribeCommandTest.this.channel, channel);
                Assert.assertEquals(testMessage, message);
                messageGot.set(true);
            }
        });
        crdtSubCmd.execute(executors).addListener(new CommandFutureListener<Object>() {
            public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
                logger.info("[operationComplete]{}", commandFuture.isSuccess());
                if (!commandFuture.isSuccess()) {
                    Assert.fail();
                } else {
                    unsubscribe.set(true);
                }
            }
        });

        waitConditionUntilTimeOut(()->messageGot.get());
        Assert.assertTrue(messageGot.get());
        crdtSubCmd.unSubscribe();
        Assert.assertTrue(unsubscribe.get());
    }

}
