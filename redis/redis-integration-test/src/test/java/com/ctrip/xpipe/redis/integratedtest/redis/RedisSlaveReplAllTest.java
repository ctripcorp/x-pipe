package com.ctrip.xpipe.redis.integratedtest.redis;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.ConfigSetCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.integratedtest.AbstractIntegratedTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *         <p>
 *         May 26, 2017
 */
public class RedisSlaveReplAllTest extends AbstractIntegratedTest{

    private List<RedisMeta> redises = new LinkedList<>();
    private int messageCount = 100;

    @Before
    public void beforeRedisSlaveReplAllTest() throws IOException {

        redises.add(new RedisMeta().setIp("127.0.0.1").setPort(6379));
        redises.add(new RedisMeta().setIp("127.0.0.1").setPort(6479));
        redises.add(new RedisMeta().setIp("127.0.0.1").setPort(6579));
        redises.add(new RedisMeta().setIp("127.0.0.1").setPort(6679));

        startRedises(redises);
        slaveWritable(redises);

        sleep(2000);

    }

    private void slaveWritable(List<RedisMeta> redises) {

        redises.forEach((redis) -> {
            slaveReadOnly(redis, false);
        });
    }

    private void slaveReadOnly(RedisMeta redis, boolean readonly) {
        try {
            logger.info("[slaveReadOnly][{}] readonly {}", redis, readonly);
            SimpleObjectPool<NettyClient> keyPool = getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint(redis.getIp(), redis.getPort()));
            new ConfigSetCommand.ConfigSetSlaveReadOnly(readonly, keyPool, scheduled).execute().get();
        } catch (Exception e) {
            logger.error("[slaveReadOnly][{}][{}] fail", redis.desc(), readonly, e);
        }
    }

    @Test
    public void testSlaveOf() throws Exception {

        int slaveWriteIndex = 1;

        makeSlaveReplAll(redises.get(slaveWriteIndex), true);
        sendMesssageToMasterAndTest(messageCount, redises.get(slaveWriteIndex), redises.subList(slaveWriteIndex + 1, redises.size()));

        logger.info("[make slave master]{}", redises.get(slaveWriteIndex));
        createJedis(redises.get(slaveWriteIndex)).slaveofNoOne();

        List<RedisMeta> newRedises = redises.subList(slaveWriteIndex, redises.size());

        Assert.assertEquals(newRedises.size() - 1, getSyncFullCount(newRedises));

        RedisMeta previous = null;
        for(int i=newRedises.size() - 1;i>=0;i--){

            RedisMeta  redis = newRedises.get(i);
            if(previous == null){
                createJedis(redis).slaveofNoOne();
            }else{
                createJedis(redis).slaveof(previous.getIp(), previous.getPort());
            }
            previous = redis;
        }

        Assert.assertEquals(newRedises.size() - 1, getSyncFullCount(newRedises));
    }

    private int getSyncFullCount(List<RedisMeta> newRedises) {

        int count = 0;
        for(RedisMeta redis : newRedises){
            count += getSyncFull(redis);
        }
        return count;
    }

    private int getSyncFull(RedisMeta redis) {

        String stats = createJedis(redis).info("stats");

        for(String split : stats.split("[\r\n]+")){
            String[] sps = split.split(":");
            if(sps.length != 2){
                continue;
            }
            if(sps[0].equalsIgnoreCase("sync_full")){
                return Integer.parseInt(sps[1]);
            }
        }
        throw new IllegalStateException("sync_full section not found!" + stats);
    }


    @Test
    public void testSlaveReplAll() throws Exception {

        sendMesssageToMasterAndTest(messageCount, redises.get(0), redises.subList(1, redises.size()));

        int slaveWriteIndex = 1;

        try{
            logger.info("[testSlaveReplAll][default not replicate]");
            flushAll();
            sendMesssageToMasterAndTest(messageCount, redises.get(slaveWriteIndex), redises.subList(slaveWriteIndex + 1, redises.size()));
            Assert.fail();
        }catch (Throwable th){
            //pass
        }


        logger.info("[testSlaveReplAll][should replicate]");
        flushAll();
        makeSlaveReplAll(redises.get(slaveWriteIndex), true);
        sendMesssageToMasterAndTest(messageCount, redises.get(slaveWriteIndex), redises.subList(slaveWriteIndex + 1, redises.size()));


        logger.info("[testSlaveReplAll][not replicate again]");
        flushAll();
        makeSlaveReplAll(redises.get(slaveWriteIndex), false);
        try{
            sendMesssageToMasterAndTest(messageCount, redises.get(slaveWriteIndex), redises.subList(slaveWriteIndex + 1, redises.size()));
            Assert.fail();
        }catch (Throwable th){
            //pass
        }
    }

    private void flushAll() {

        for(RedisMeta redisMeta : redises){
            createJedis(redisMeta).flushAll();
        }
        sleep(100);
    }

    private void startRedises(List<RedisMeta> redises) throws IOException {

        for( int i=0; i < redises.size() ;i++ ){

            RedisMeta redis = redises.get(i);
            stopServerListeningPort(redis.getPort());

            if(i == 0){
                startRedis(redis);
            }else{
                startRedis(redis, redises.get(i - 1));
            }
        }
    }

    @Test
    public void testSlaveReplAllSlaveOf(){

    }

    @Test
    public void testConcurrentCloseSlaveAndFeedData() throws Exception {
        RedisMeta master = redises.get(0);
        RedisMeta slave = redises.get(1);
        slaveReadOnly(master, true);
        slaveReadOnly(slave, true);

        int cnt = 1;
        while (true) {
            logger.info("[testConcurrentCloseSlaveAndFeedData] round {}", cnt++);
            createJedis(master).slaveofNoOne();
            createJedis(slave).slaveof(master.getIp(), master.getPort());
            waitSlaveOnline(slave.getIp(), slave.getPort());

            CyclicBarrier barrier = new CyclicBarrier(2);
            CountDownLatch latch = new CountDownLatch(2);
            executors.execute(() -> {
                try {
                    barrier.await();
                } catch (Exception e) {
                    // ignore
                }

                try {
                    sendMessageToMaster(master, 100);
                } catch (Exception e) {
                    // ignore
                }
                latch.countDown();
            });
            executors.execute(() -> {
                try {
                    barrier.await();
                } catch (Exception e) {
                    // ignore
                }

                sleep(1);
                createJedis(master).slaveof("127.0.0.1", 0);
                latch.countDown();
            });

            if (!latch.await(2, TimeUnit.SECONDS)) {
                logger.info("[testConcurrentCloseSlaveAndFeedData] wait latch fail");
            }

            sleep(1000);
            long masterOffset = Long.parseLong(infoRedis(master.getIp(), master.getPort(), InfoCommand.INFO_TYPE.REPLICATION, "master_repl_offset"));
            long slaveOffset = Long.parseLong(infoRedis(slave.getIp(), slave.getPort(), InfoCommand.INFO_TYPE.REPLICATION, "master_repl_offset"));
            if (masterOffset != slaveOffset) {
                logger.info("[testConcurrentCloseSlaveAndFeedData][offset not match] {} {}", masterOffset, slaveOffset);
                waitForAnyKey();
            } else {
                logger.info("[testConcurrentCloseSlaveAndFeedData] offset match {}", masterOffset);
            }
        }
    }

    @Override
    protected RedisMeta getRedisMaster() {
        return redises.get(0);
    }

    @Override
    protected List<RedisMeta> getRedisSlaves() {

        return redises.subList(1, redises.size());
    }

    @After
    public void afterRedisSlaveReplAllTest() throws IOException {
    }

    public void makeSlaveReplAll(RedisMeta slave, boolean replAll) throws Exception {

        SimpleObjectPool<NettyClient> keyPool = getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint(slave.getIp(), slave.getPort()));
        ConfigSetCommand.ConfigSetReplAll configSetReplAll = new ConfigSetCommand.ConfigSetReplAll(replAll, keyPool, scheduled);
        configSetReplAll.execute().get();
    }
}
