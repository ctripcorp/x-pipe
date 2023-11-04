package com.ctrip.xpipe.redis.integratedtest.keeper.manul;

import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.integratedtest.keeper.AbstractKeeperIntegratedSingleDc;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.monitor.KeepersMonitorManager;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

/**
 * @author wenchao.meng
 * <p>
 * Sep 29, 2016
 */
public class GivenDataKeeper extends AbstractKeeperIntegratedSingleDc {

    private volatile boolean done = false;

    private String commandFile = "~/tmp/cmd_3512732e-e07a-4b27-912b-26b4561d2450_997506897712";

    @Override
    protected void doBeforeIntegratedTest() throws Exception {
        commandFile = getRealDir(commandFile);
    }

    @Test
    public void startTest() throws IOException {

        waitForAnyKeyToExit();
    }


    @Override
    protected String getXpipeMetaConfigFile() {
        return "one_keeper.xml";
    }

    @Override
    protected RedisKeeperServer createRedisKeeperServer(KeeperMeta keeperMeta, File baseDir, KeeperConfig keeperConfig,
                                                        LeaderElectorManager leaderElectorManager, KeepersMonitorManager keeperMonitorManager) {

        return new DefaultRedisKeeperServer(keeperMeta.parent().getDbId(), keeperMeta, keeperConfig, baseDir, leaderElectorManager,
                keeperMonitorManager, resourceManager) {
            @Override
            public void endWriteRdb() {
                super.endWriteRdb();

                appendCommands(getReplicationStore());

            }
        };
    }

    private void appendCommands(ReplicationStore replicationStore) {

        logger.info("[addendCommands][begin]{}", replicationStore);
        RandomAccessFile file = null;

        ByteBuffer byteBuffer = ByteBuffer.allocate(1024);

        try {
            file = new RandomAccessFile(commandFile, "r");

            while (true) {
                byteBuffer.clear();
                int len = file.getChannel().read(byteBuffer);
                if (len == -1) {
                    logger.info("[]");
                    break;
                }
                byteBuffer.flip();

                replicationStore.appendCommands(Unpooled.wrappedBuffer(byteBuffer));
            }
        } catch (FileNotFoundException e) {
            logger.error("[addendCommands]", e);
        } catch (IOException e) {
            logger.error("[addendCommands]", e);
        } finally {
            if (file != null) {
                try {
                    file.close();
                } catch (IOException e) {
                    logger.error("[addendCommands]", e);
                }
            }
        }

        logger.info("[addendCommands][end]{}", replicationStore);

    }



    @Override
    protected KeeperConfig getKeeperConfig() {
        return new TestKeeperConfig(1 << 30, 5, 1 << 30, 300000);
    }

}
