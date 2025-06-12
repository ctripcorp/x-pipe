package com.ctrip.xpipe.redis.meta.server.impl;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.core.metaserver.META_SERVER_SERVICE;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServerInfo;
import com.ctrip.xpipe.redis.meta.server.rest.ForwardInfo;
import com.ctrip.xpipe.simpleserver.IoAction;
import com.ctrip.xpipe.simpleserver.IoActionFactory;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.Test;

import java.io.IOException;
import java.net.Socket;

public class RemoteMetaServerTest extends AbstractTest {

    private RemoteMetaServer remoteMetaServer;

    @Test
    public void testMakeMasterReadOnly() throws Exception {
        Server server = startServer(randomPort(), new IoActionFactory() {
            @Override
            public IoAction createIoAction(Socket socket) {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return new IoAction() {
                    @Override
                    public Object read() throws IOException {
                        return null;
                    }

                    @Override
                    public void write(Object readResult) throws IOException {

                    }

                    @Override
                    public Socket getSocket() {
                        return null;
                    }
                };
            }
        });
        remoteMetaServer = new RemoteMetaServer(1, 2, new ClusterServerInfo("127.0.0.1", server.getPort()));
        remoteMetaServer.makeMasterReadOnly("cluster", "shard", false, new ForwardInfo(META_SERVER_SERVICE.MAKE_MASTER_READONLY.getForwardType()));
        Thread.sleep(10000);
    }
}