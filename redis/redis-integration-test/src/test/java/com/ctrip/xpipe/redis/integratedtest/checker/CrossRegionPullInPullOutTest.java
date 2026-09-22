package com.ctrip.xpipe.redis.integratedtest.checker;

import com.ctrip.xpipe.redis.integratedtest.console.AbstractXPipeDrTest;
import com.ctrip.xpipe.redis.integratedtest.console.cmd.RedisKillCmd;
import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 验证 cross-region（ONE_WAY DR）实例的「拉入 / 拉出」，且多 slave 场景下按 IP 顺序逐个拉入。
 * <pre>
 *   拉入：cross-region slave 与上游 replId 一致 → checker 标记 HEALTHY
 *   拉出：cross-region slave ping 超时            → checker 标记 DOWN
 * </pre>
 *
 * 拓扑参考 {@code AbstractXPipeDrTest#startSimpleXPipeDR()}（xpipe-dr-proxy.sql）：
 *   jq(active) 主 6379 + keeper 7100/7101；oy(backup) 3 个 redis slave(7379/7380/7381) + keeper 8100/8101，
 *   并加 proxy 作为 cross-region 数据通路。
 *
 * 多 slave 的「按 IP 顺序拉入」：keeper 侧 {@code CrossRegionFsyncCoordinator}（默认 maxLoadingSlaves=1）
 * 对 cross-region slave 的 full sync 串行放行（按 ip:port 升序），checker 在各自 full sync 完成后才拉入，
 * 因此 checker 的拉入顺序与 keeper 的全量顺序一致——按 ip:port 升序。
 */
public class CrossRegionPullInPullOutTest extends AbstractXPipeDrTest {

    private static final String OY_CONSOLE = "http://127.0.0.1:8081";

    private static final String SLAVE_IP = "127.0.0.1";

    private static final int KEEPER_PORT = 8100;

    private static final int[] SLAVE_PORTS = {7379, 7380, 7381};

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/xpipe-dr-proxy.sql");
    }

    private String crossRegionHealthUrl(int port) {
        return OY_CONSOLE + "/api/health/cross/region/" + SLAVE_IP + "/" + port;
    }

    @Test
    public void testCrossRegionPullInPullOut() throws Exception {
        // 1. 起 proxy（cross-region 数据通路），并先起额外 2 个 slave，让 metaserver 一并接管
        startProxy("jq", 11080, 11443);
        startProxy("oy", 11081, 11444);
        startRedis(7380);
        startRedis(7381);

        // 2. 起 DR 拓扑：jq(active) + oy(backup)，含 keeper + redis + console + checker
        startSimpleXPipeDR();

        // 3. 初始拉入：3 个 slave 都 HEALTHY
        for (int port : SLAVE_PORTS) {
            waitForServerRespAsExpected(crossRegionHealthUrl(port), String.class, "\"HEALTHY\"", 120000);
        }

        // 4. 拉出：杀掉 3 个 slave → ping 超时 → DOWN
        for (int port : SLAVE_PORTS) {
            new RedisKillCmd(port, executors).execute().get();
        }
        for (int port : SLAVE_PORTS) {
            waitForServerRespAsExpected(crossRegionHealthUrl(port), String.class, "\"DOWN\"", 120000);
        }

        // 5. 模拟增量一直失败：重启 3 个 slave（空 replId，无法增量，只能 full sync）+ 统一 slaveof keeper
        for (int port : SLAVE_PORTS) {
            startRedis(port);
        }
        for (int port : SLAVE_PORTS) {
            waitRedisReady(port);
            slaveofKeeper(port);
        }

        // 6. 采样拉入顺序，断言按 ip:port 升序
        List<Integer> markUpOrder = sampleMarkUpOrder();
        Assert.assertEquals("all slaves should be pulled in", SLAVE_PORTS.length, markUpOrder.size());
        Assert.assertEquals("pull-in should be ascending by ip:port", ascendingPorts(), markUpOrder);
    }

    private void slaveofKeeper(int port) {
        try (Jedis jedis = new Jedis(SLAVE_IP, port)) {
            jedis.slaveof(SLAVE_IP, KEEPER_PORT);
        }
    }

    private void waitRedisReady(int port) throws Exception {
        waitConditionUntilTimeOut(() -> {
            try (Jedis jedis = new Jedis(SLAVE_IP, port)) {
                return "PONG".equals(jedis.ping());
            } catch (Throwable ignore) {
                return false;
            }
        }, 10000, 100);
    }

    private List<Integer> sampleMarkUpOrder() throws Exception {
        List<Integer> order = new ArrayList<>();
        Set<Integer> marked = new HashSet<>();
        long deadline = System.currentTimeMillis() + 120_000;
        while (System.currentTimeMillis() < deadline && marked.size() < SLAVE_PORTS.length) {
            for (int port : SLAVE_PORTS) {
                if (marked.contains(port)) {
                    continue;
                }
                try {
                    String state = restTemplate.getForObject(crossRegionHealthUrl(port), String.class);
                    if ("\"HEALTHY\"".equals(state)) {
                        order.add(port);
                        marked.add(port);
                    }
                } catch (Throwable ignore) {
                }
            }
            Thread.sleep(100);
        }
        return order;
    }

    private List<Integer> ascendingPorts() {
        List<Integer> ports = new ArrayList<>();
        for (int port : SLAVE_PORTS) {
            ports.add(port);
        }
        ports.sort(Integer::compareTo);
        return ports;
    }
}
