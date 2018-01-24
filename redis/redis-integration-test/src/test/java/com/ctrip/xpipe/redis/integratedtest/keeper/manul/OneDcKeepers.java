package com.ctrip.xpipe.redis.integratedtest.keeper.manul;

import com.ctrip.xpipe.api.server.PARTIAL_STATE;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.monitor.CatConfig;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.integratedtest.keeper.AbstractKeeperIntegratedSingleDc;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *         <p>
 *         Sep 29, 2016
 */
public class OneDcKeepers extends AbstractKeeperIntegratedSingleDc {

    @Override
    protected void doBeforeAbstractTest() throws Exception {
        System.setProperty(CatConfig.CAT_ENABLED_KEY, "false");
    }

    @Test
    public void startTest() throws IOException {

        try {
            sendMessageToMasterAndTestSlaveRedis();
        } catch (Throwable e) {
            logger.error("[startTest]", e);
        }

        waitForAnyKeyToExit();
    }

    @Test
    public void sendMessageForever() throws IOException {

        executors.execute(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {

                while (!Thread.interrupted()){
                    sendMessageToMaster();
                }
            }
        });

        waitForAnyKeyToExit();
    }

    @Override
    protected void doAfterAbstractTest() throws Exception {
        super.doAfterAbstractTest();
    }

    @Test
    public void killActive() throws Exception {

        RedisMeta redisMaster = getRedisMaster();

        sendMessageToMasterAndTestSlaveRedis();

        System.out.println("press any key to make back keeper active");
        waitForAnyKey();

        KeeperMeta backupKeeper = getKeepersBackup().get(0);
        RedisKeeperServer redisKeeperServer = getRedisKeeperServer(backupKeeper);
        Assert.assertEquals(PARTIAL_STATE.FULL, redisKeeperServer.getRedisMaster().partialState());
        logger.info(remarkableMessage("make keeper active{}"), backupKeeper);
        setKeeperState(backupKeeper, KeeperState.ACTIVE, redisMaster.getIp(), redisMaster.getPort());


        waitForAnyKeyToExit();
    }

    @Test
    public void testKeeperChangeState() throws Exception {

        int round = 1000;

        scheduled.scheduleAtFixedRate(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                sendMessageToMaster(getRedisMaster(), 100);
            }
        }, 0, 500, TimeUnit.SECONDS.MILLISECONDS);

        RedisMeta redisMaster = getRedisMaster();
        KeeperMeta currentActive = activeKeeper;
        KeeperMeta currentBackup = backupKeeper;

        for (int i = 0; i < round; i++){

            logger.info(remarkableMessage("round: {}"), i);
            setKeeperState(currentBackup, KeeperState.ACTIVE, redisMaster.getIp(), redisMaster.getPort());
            setKeeperState(currentActive, KeeperState.BACKUP, currentBackup.getIp(), currentBackup.getPort());

            sleep(1000);

            KeeperMeta tmp = currentActive;
            currentActive = currentBackup;
            currentBackup = tmp;
        }

        waitForAnyKey();

    }

    @Override
    protected KeeperConfig getKeeperConfig() {
        return new TestKeeperConfig(1 << 30, 5, 1 << 30, 300000);
    }

}