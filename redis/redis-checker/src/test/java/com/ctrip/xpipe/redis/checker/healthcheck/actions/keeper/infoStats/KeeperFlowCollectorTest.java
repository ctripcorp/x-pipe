package com.ctrip.xpipe.redis.checker.healthcheck.actions.keeper.infoStats;

import com.ctrip.xpipe.api.lifecycle.LifecycleState;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.*;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.RedisCheckRule;
import com.ctrip.xpipe.redis.checker.healthcheck.config.HealthCheckConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.AbstractHealthCheckInstance;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

import java.util.List;

/**
 * Created by yu
 * 2023/8/29
 */
public class KeeperFlowCollectorTest extends AbstractCheckerTest {

    private KeeperFlowCollector listener;

    private KeeperHealthCheckInstance instance;

    private KeeperInfoStatsActionContext context;

    @Before
    public void before() throws Exception {
        listener = new KeeperFlowCollector();
        int keeperPort = 6380;
        String keeperIp = "127.0.0.1";
        MockitoAnnotations.initMocks(this);
        instance = newRandomKeeperHealthCheckInstance(keeperIp, keeperPort);
        String info = "# Stats\n" +
                "sync_full:0\n" +
                "sync_partial_ok:0\n" +
                "sync_partial_err:0\n" +
                "total_net_input_bytes:1716640145\n" +
                "instantaneous_input_kbps:1.584961\n" +
                "total_net_output_bytes:97539279\n" +
                "instantaneous_output_kbps:0.030273\n" +
                "peak_input_kbps:60315\n" +
                "peak_output_kbps:2\n" +
                "psync_fail_send:0";

//        InfoResultExtractor extractors = new InfoResultExtractor(info);
        context = new KeeperInfoStatsActionContext(instance, info);
    }

    @Test
    public void testParseResult() {
        listener.onAction(context);
        listener.onAction(context);
        Assert.assertTrue(listener.worksfor(context));
        Assert.assertEquals(1, listener.getHostPort2InputFlow().size());
        listener.stopWatch(new HealthCheckAction() {
            @Override
            public void addListener(HealthCheckActionListener listener) {

            }

            @Override
            public void removeListener(HealthCheckActionListener listener) {

            }

            @Override
            public void addListeners(List list) {

            }

            @Override
            public void addController(HealthCheckActionController controller) {

            }

            @Override
            public void addControllers(List list) {

            }

            @Override
            public void removeController(HealthCheckActionController controller) {

            }

            @Override
            public HealthCheckInstance getActionInstance() {
                return new HealthCheckInstance() {
                    @Override
                    public CheckInfo getCheckInfo() {
                        return new KeeperInstanceInfo() {
                            @Override
                            public ClusterShardHostPort getClusterShardHostport() {
                                return null;
                            }

                            @Override
                            public String getShardId() {
                                return "shard";
                            }

                            @Override
                            public String getDcId() {
                                return "dc";
                            }

                            @Override
                            public boolean isActive() {
                                return false;
                            }

                            @Override
                            public HostPort getHostPort() {
                                return new HostPort("10.10.10.10", 11);
                            }

                            @Override
                            public String getClusterId() {
                                return "cluster";
                            }

                            @Override
                            public ClusterType getClusterType() {
                                return null;
                            }

                            @Override
                            public String getActiveDc() {
                                return null;
                            }

                            @Override
                            public void setActiveDc(String activeDc) {

                            }

                            @Override
                            public List<RedisCheckRule> getRedisCheckRules() {
                                return null;
                            }

                            @Override
                            public void setAzGroupType(String type) {

                            }

                            @Override
                            public String getAzGroupType() {
                                return null;
                            }

                            @Override
                            public void setAsymmetricCluster(boolean isHeteroCluster) {

                            }

                            @Override
                            public boolean isAsymmetricCluster() {
                                return false;
                            }
                        };
                    }

                    @Override
                    public HealthCheckConfig getHealthCheckConfig() {
                        return null;
                    }

                    @Override
                    public void register(HealthCheckAction action) {

                    }

                    @Override
                    public void unregister(HealthCheckAction action) {

                    }

                    @Override
                    public List<HealthCheckAction> getHealthCheckActions() {
                        return null;
                    }

                    @Override
                    public void dispose() throws Exception {

                    }

                    @Override
                    public void initialize() throws Exception {

                    }

                    @Override
                    public LifecycleState getLifecycleState() {
                        return null;
                    }

                    @Override
                    public void start() throws Exception {

                    }

                    @Override
                    public void stop() throws Exception {

                    }

                    @Override
                    public int getOrder() {
                        return 0;
                    }
                };
            }

            @Override
            public void dispose() throws Exception {

            }

            @Override
            public void initialize() throws Exception {

            }

            @Override
            public LifecycleState getLifecycleState() {
                return null;
            }

            @Override
            public void start() throws Exception {

            }

            @Override
            public void stop() throws Exception {

            }

            @Override
            public int getOrder() {
                return 0;
            }
        });
    }

    @Test
    public void testWithNonResult() {
        String info = "# Stats\n" +
                "sync_full:0\n" +
                "sync_partial_ok:0\n" +
                "sync_partial_err:0\n" +
                "psync_fail_send:0";
        context = new KeeperInfoStatsActionContext(instance, info);
        listener.onAction(context);
        Assert.assertTrue(listener.worksfor(context));
        Assert.assertEquals(0, listener.getHostPort2InputFlow().size());
    }

}