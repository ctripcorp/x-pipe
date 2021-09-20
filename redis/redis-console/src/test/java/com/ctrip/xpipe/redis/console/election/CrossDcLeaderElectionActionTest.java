package com.ctrip.xpipe.redis.console.election;


import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.dao.ConfigDao;
import com.ctrip.xpipe.redis.console.exception.DalInsertException;
import com.ctrip.xpipe.redis.console.exception.DalUpdateException;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import com.ctrip.xpipe.redis.console.model.ConfigTbl;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.DalNotFoundException;

import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static com.ctrip.xpipe.redis.console.election.CrossDcLeaderElectionAction.KEY_LEASE_CONFIG;

@RunWith(MockitoJUnitRunner.class)
public class CrossDcLeaderElectionActionTest extends AbstractTest {

    private static final String SUB_KEY_CROSS_DC_LEADER = "CROSS_DC_LEADER";

    @Mock
    private MetaCache metaCache;

    private volatile ConfigTbl crossDcLeaderConfigTbl;

    private MockDcLeader fqDc;
    private MockDcLeader oyDc;
    private MockDcLeader rbDc;

    private AtomicInteger configUpdateTimes = new AtomicInteger(0);

    @Before
    public void beforeCrossDcLeaderElectionActionTest() throws Exception {
        fqDc = new MockDcLeader("fq");
        oyDc = new MockDcLeader("oy");
        rbDc = new MockDcLeader("rb");

        Mockito.when(metaCache.getXpipeMeta()).thenReturn(mockXpipeMeta());
        CrossDcLeaderElectionAction.MAX_ELECTION_DELAY_MILLISECOND = 100;
        CrossDcLeaderElectionAction.ELECTION_INTERVAL_SECOND = 1;
    }

    @Test
    public void normalTest() throws Exception {
        // the dc leader with least active cluster is always elected to cross dc leader
        startAllElection();

        // init lease config in the first election, so test from the second election
        waitConditionUntilTimeOut(() -> configUpdateTimes.get() >= 2, CrossDcLeaderElectionAction.ELECTION_INTERVAL_SECOND * 1000 * 2);
        sleep(CrossDcLeaderElectionAction.MAX_ELECTION_DELAY_MILLISECOND + 100);
        assertCrossDcLeader("fq", "fq", "fq");

        waitConditionUntilTimeOut(() -> configUpdateTimes.get() >= 3, CrossDcLeaderElectionAction.ELECTION_INTERVAL_SECOND * 1000);
        sleep(CrossDcLeaderElectionAction.MAX_ELECTION_DELAY_MILLISECOND + 100);
        assertCrossDcLeader("fq", "fq", "fq");

        stopAllElection();
    }

    @Test
    public void onlyBiDirectionClusterTest() throws Exception {
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(mockBiDirectionXpipeMeta());
        normalTest();
    }

    @Test
    public void onlyOneWayClusterTest() throws Exception {
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(mockBiDirectionXpipeMeta());
        normalTest();
    }

    @Test
    public void leaderNetworkBrokenTest() throws Exception {
        startAllElection();
        waitConditionUntilTimeOut(() -> configUpdateTimes.get() >= 2, CrossDcLeaderElectionAction.ELECTION_INTERVAL_SECOND * 1000 * 2);
        sleep(CrossDcLeaderElectionAction.MAX_ELECTION_DELAY_MILLISECOND + 100);
        assertCrossDcLeader("fq", "fq", "fq");

        // fq DC network island
        fqDc.dbWriteAvailable = false;
        fqDc.localConfigTbl = cloneConfig();
        waitConditionUntilTimeOut(() -> configUpdateTimes.get() >= 3, CrossDcLeaderElectionAction.ELECTION_INTERVAL_SECOND * 1000);
        sleep(CrossDcLeaderElectionAction.MAX_ELECTION_DELAY_MILLISECOND + 100);
        assertCrossDcLeader(null, "rb", "rb");

        // fq DC network recover
        fqDc.dbWriteAvailable = true;
        fqDc.localConfigTbl = null;
        waitConditionUntilTimeOut(() -> configUpdateTimes.get() >= 4, CrossDcLeaderElectionAction.ELECTION_INTERVAL_SECOND * 1000);
        sleep(CrossDcLeaderElectionAction.MAX_ELECTION_DELAY_MILLISECOND + 100);
        assertCrossDcLeader("fq", "fq", "fq");

        stopAllElection();
    }

    private void startAllElection() throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(3);
        CountDownLatch latch = new CountDownLatch(3);
        Arrays.asList(fqDc, oyDc, rbDc).forEach(elector -> {
            new Thread(() -> {
                try {
                    barrier.await();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                try {
                    elector.start();
                    latch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
        });

        latch.await();
    }

    private void stopAllElection() throws Exception {
        CountDownLatch latch = new CountDownLatch(3);
        Arrays.asList(fqDc, oyDc, rbDc).forEach(elector -> {
            new Thread(() -> {
                try {
                    elector.stop();
                    latch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
        });

        latch.await();
    }

    private void assertCrossDcLeader(String fqLeaderDc, String oyLeaderDc, String rbLeaderDc) {
        Assert.assertEquals(fqLeaderDc, fqDc.electionResult);
        Assert.assertEquals(oyLeaderDc, oyDc.electionResult);
        Assert.assertEquals(rbLeaderDc, rbDc.electionResult);
    }

    private XpipeMeta mockOneWayXpipeMeta() {
        XpipeMeta xpipeMeta = new XpipeMeta();
        Arrays.asList("fq", "oy", "rb").stream().forEach(dc -> {
            DcMeta dcMeta = new DcMeta();
            dcMeta.setId(dc);
            xpipeMeta.addDc(dcMeta);
        });

        ClusterMeta oyClusterMeta = new ClusterMeta("oyCluster");
        ClusterMeta oyClusterMeta2 = new ClusterMeta("oyCluster2");
        ClusterMeta rbClusterMeta = new ClusterMeta("rbCluster");
        oyClusterMeta.setActiveDc("oy");
        oyClusterMeta2.setActiveDc("oy");
        rbClusterMeta.setActiveDc("rb");
        oyClusterMeta.setType(ClusterType.ONE_WAY.toString());
        oyClusterMeta2.setType(ClusterType.ONE_WAY.toString());
        rbClusterMeta.setType(ClusterType.ONE_WAY.toString());

        xpipeMeta.getDcs().get("oy").addCluster(oyClusterMeta).addCluster(rbClusterMeta).addCluster(oyClusterMeta2);
        xpipeMeta.getDcs().get("rb").addCluster(oyClusterMeta).addCluster(rbClusterMeta).addCluster(oyClusterMeta2);
        return xpipeMeta;
    }

    private XpipeMeta mockBiDirectionXpipeMeta() {
        XpipeMeta xpipeMeta = new XpipeMeta();
        Arrays.asList("fq", "oy", "rb").stream().forEach(dc -> {
            DcMeta dcMeta = new DcMeta();
            dcMeta.setId(dc);
            xpipeMeta.addDc(dcMeta);
        });

        ClusterMeta biClusterMeta = new ClusterMeta("bi-cluster");
        ClusterMeta biClusterMeta2 = new ClusterMeta("bi-cluster2");
        biClusterMeta.setType(ClusterType.BI_DIRECTION.toString());
        biClusterMeta2.setType(ClusterType.BI_DIRECTION.toString());

        xpipeMeta.getDcs().get("fq").addCluster(biClusterMeta);
        xpipeMeta.getDcs().get("oy").addCluster(biClusterMeta).addCluster(biClusterMeta2);
        xpipeMeta.getDcs().get("rb").addCluster(biClusterMeta).addCluster(biClusterMeta2);
        return xpipeMeta;
    }

    private XpipeMeta mockXpipeMeta() {
        XpipeMeta xpipeMeta = new XpipeMeta();
        Arrays.asList("fq", "oy", "rb").stream().forEach(dc -> {
            DcMeta dcMeta = new DcMeta();
            dcMeta.setId(dc);
            xpipeMeta.addDc(dcMeta);
        });

        ClusterMeta oyClusterMeta = new ClusterMeta("oyCluster");
        ClusterMeta oyClusterMeta2 = new ClusterMeta("oyCluster2");
        ClusterMeta rbClusterMeta = new ClusterMeta("rbCluster");
        ClusterMeta biClusterMeta = new ClusterMeta("bi-cluster");
        oyClusterMeta.setActiveDc("oy");
        oyClusterMeta2.setActiveDc("oy");
        rbClusterMeta.setActiveDc("rb");
        oyClusterMeta.setType(ClusterType.ONE_WAY.toString());
        oyClusterMeta2.setType(ClusterType.ONE_WAY.toString());
        rbClusterMeta.setType(ClusterType.ONE_WAY.toString());
        biClusterMeta.setType(ClusterType.BI_DIRECTION.toString());

        xpipeMeta.getDcs().get("fq").addCluster(biClusterMeta);
        xpipeMeta.getDcs().get("oy").addCluster(oyClusterMeta).addCluster(rbClusterMeta).addCluster(oyClusterMeta2).addCluster(biClusterMeta);
        xpipeMeta.getDcs().get("rb").addCluster(oyClusterMeta).addCluster(rbClusterMeta).addCluster(oyClusterMeta2).addCluster(biClusterMeta);
        return xpipeMeta;
    }

    private synchronized void handleWrite(ConfigModel config, Date until, Date dataChangeLastTime) {
        if (null == crossDcLeaderConfigTbl
                || !KEY_LEASE_CONFIG.equals(config.getKey()) || !SUB_KEY_CROSS_DC_LEADER.equals(config.getSubKey())
                || crossDcLeaderConfigTbl.getDataChangeLastTime().compareTo(dataChangeLastTime) != 0) {
            throw new DalUpdateException(String.format("Update failed.No rows updated for %s, %s", config.getKey(), config.getSubKey()));
        }

        logger.info("[handleWrite] {} to {} val {}", crossDcLeaderConfigTbl.getDataChangeLastTime(), dataChangeLastTime, config.getVal());
        this.crossDcLeaderConfigTbl.setValue(config.getVal())
                .setUntil(until)
                .setDataChangeLastTime(new Date());
        configUpdateTimes.incrementAndGet();
    }

    private ConfigTbl handleRead(String key, String subId) throws DalException {
        if (null == crossDcLeaderConfigTbl || !key.equals(KEY_LEASE_CONFIG) || !subId.equals(SUB_KEY_CROSS_DC_LEADER)) {
            throw new DalNotFoundException(String.format("No record has been found for %s, %s", key, subId));
        }

        return cloneConfig();
    }

    private synchronized void handleInsert(ConfigModel config, Date until, String desc) {
        if (null != this.crossDcLeaderConfigTbl) {
            throw new DalInsertException("Insert failed: already exist config " + this.crossDcLeaderConfigTbl.toString());
        }
        if (!KEY_LEASE_CONFIG.equals(config.getKey()) || !SUB_KEY_CROSS_DC_LEADER.equals(config.getSubKey())) {
            throw new DalInsertException(String.format("Insert failed: unexpected config %s, %s", config.getKey(), config.getSubKey()));
        }

        this.crossDcLeaderConfigTbl = new ConfigTbl();
        this.crossDcLeaderConfigTbl.setKey(config.getKey())
                .setSubKey(config.getSubKey())
                .setUntil(until)
                .setValue(config.getVal())
                .setDataChangeLastTime(new Date());
    }

    private ConfigTbl cloneConfig() {
        ConfigTbl configClone = new ConfigTbl();
        configClone.setKey(crossDcLeaderConfigTbl.getKey())
                .setSubKey(crossDcLeaderConfigTbl.getSubKey())
                .setValue(crossDcLeaderConfigTbl.getValue())
                .setDataChangeLastTime(crossDcLeaderConfigTbl.getDataChangeLastTime())
                .setUntil(crossDcLeaderConfigTbl.getUntil());
        return configClone;
    }

    private class MockDcLeader implements Observer {

        private String dcName;

        private boolean dbWriteAvailable;

        private boolean dbReadAvailable;

        private ConfigTbl localConfigTbl;

        private ConfigDao configDao;

        private ConsoleConfig consoleConfig;

        private CrossDcLeaderElectionAction electionAction;

        private String electionResult;

        public MockDcLeader(String dcName) throws Exception {
            this.dcName = dcName;
            this.dbReadAvailable = true;
            this.dbWriteAvailable = true;
            this.localConfigTbl = null;
            this.electionResult = "init";

            configDao = Mockito.mock(ConfigDao.class);
            consoleConfig = Mockito.mock(ConsoleConfig.class);

            Mockito.doAnswer(inv -> {
                if (!dbWriteAvailable) {
                    throw new DalUpdateException("db write is not available for dc " + dcName);
                }

                handleWrite(inv.getArgument(0, ConfigModel.class),
                        inv.getArgument(1, Date.class), inv.getArgument(2, Date.class));
                return null;
            }).when(configDao).updateConfigIdempotent(Mockito.any(), Mockito.any(), Mockito.any());

            Mockito.doAnswer(inv -> {
                if (!dbReadAvailable) {
                    throw new DalException("db read is not available for dc " + dcName);
                }
                if (null != localConfigTbl) return localConfigTbl;

                return handleRead(inv.getArgument(0, String.class), inv.getArgument(1, String.class));
            }).when(configDao).getByKeyAndSubId(KEY_LEASE_CONFIG, SUB_KEY_CROSS_DC_LEADER);

            Mockito.doAnswer(inv -> {
                if (!dbWriteAvailable) {
                    throw new DalInsertException("db write is not available for dc " + dcName);
                }

                handleInsert(inv.getArgument(0, ConfigModel.class),
                        inv.getArgument(1, Date.class), inv.getArgument(2, String.class));
                return null;
            }).when(configDao).insertConfig(Mockito.any(), Mockito.any(), Mockito.anyString());

            Mockito.when(consoleConfig.getCrossDcLeaderLeaseName()).thenReturn(SUB_KEY_CROSS_DC_LEADER);

            electionAction = new CrossDcLeaderElectionAction(configDao, metaCache, consoleConfig);
            electionAction.setExecutors(executors);
            electionAction.dataCenter = dcName;
            electionAction.localIp = "127.0.0.1";
            electionAction.addObserver(this);
        }

        @Override
        public void update(Object args, Observable observable) {
            electionResult = (String) args;
        }

        public void start() throws Exception {
            this.electionAction.start();
        }

        public void stop() throws Exception {
            this.electionAction.stop();
        }

    }

}
