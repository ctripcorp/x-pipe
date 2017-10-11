package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.simpleserver.IoAction;
import com.ctrip.xpipe.simpleserver.IoActionFactory;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * @author wenchao.meng
 *         <p>
 *         Feb 24, 2017
 */
public class DefaultSlaveOfCommandTest extends AbstractRedisTest {

    private Server server;

    @Before
    public void beforeDefaultSlaveOfCommandTest() throws Exception {
        server = startXSlaveNotSupportServer();
    }

    @Test
    public void testSuccess() throws Exception {

        executeSlaveOf("127.0.0.1", server.getPort());
    }

    private Server startXSlaveNotSupportServer() throws Exception {

        return startServer(new IoActionFactory() {

            @Override
            public IoAction createIoAction(Socket socket) {

                return new IoAction() {

                    private String currentLine = null;

                    @Override
                    public Object read() throws IOException {

                        String line = readLine(socket.getInputStream());
                        currentLine = line;
                        return line;
                    }

                    @Override
                    public void write() throws IOException {

                        String result = "+OK\r\n";
                        if (currentLine.indexOf("xslaveof") >= 0) {
                            result = "-unsupport xslaveof command\r\n";
                        }
                        socket.getOutputStream().write(result.getBytes());
                        socket.getOutputStream().flush();
                    }

                    @Override
                    public Socket getSocket() {
                        return null;
                    }
                };
            }
        });
    }

    private void executeSlaveOf(String host, int port) throws Exception {

        executeSlaveOf(host, port, null, 0);
    }

    private void executeSlaveOf(String host, int port, String slaveofHost, int slaveOfPort) throws Exception {

        Command<String> command = new DefaultSlaveOfCommand(
                getXpipeNettyClientKeyedObjectPool().getKeyPool(new InetSocketAddress(host, port)),
                slaveofHost, slaveOfPort,
                scheduled);
        String result = command.execute().get();
        logger.info("{}", result);
    }

    @Test
    public void testMulti() throws Exception {

        for (int i = 0; i < 5; i++) {

            logger.info("round:{}", i);
            executeSlaveOf("127.0.0.1", server.getPort());
        }

    }

}
