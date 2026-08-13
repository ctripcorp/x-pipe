package com.ctrip.xpipe.redis.checker.healthcheck.session;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.checker.TestConfig;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.simpleserver.AbstractIoAction;
import com.ctrip.xpipe.simpleserver.IoActionFactory;
import com.ctrip.xpipe.simpleserver.Server;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 真实复现：通过最上层的定时调度，两条线程完全独立地跑，不固定任何顺序。
 *
 * 线程A: AbstractInstanceSessionManager
 *        每 DELAY_A ms 调 removeUnusedInstances
 *        → 把 endpoint 从"在用列表"移除 → sessions.remove → closeConnection
 *        → 再放回"在用列表" → findOrCreateSession → 新 session → psub
 *
 * 线程B: PsubAction
 *        每 DELAY_B ms 调 doTask
 *        → instance.getRedisSession().psubscribeIfAbsent()
 *
 * 两线程共用同一个 endpoint 的池。
 * 每次 A 移除了 endpoint、closeConnection，B 还在用旧 session 的引用做 psub。
 * 旧 session 产生孤儿、新 session 又 borrow——Active 积累。
 *
 * 本测试不安排顺序、不 await、不 CountDownLatch——让调度器自己决定。
 */
public class PsubRaceTest extends AbstractTest {

    private Server fakeRedis;
    private XpipeNettyClientKeyedObjectPool pool;
    private Endpoint endpoint;
    private CheckerConfig config;
    private GenericObjectPool<NettyClient> underlying;
    private volatile RedisSession latestSession;
    private volatile boolean running;
    private int initialActive;

    @Before
    public void setup() throws Exception {
        System.setProperty("DEFAULT_REDIS_COMMAND_TIME_OUT_SECONDS", "600");
        fakeRedis = startServer(randomPort(), psubIoActionFactory());
        endpoint = new DefaultEndPoint("127.0.0.1", fakeRedis.getPort());
        pool = getXpipeNettyClientKeyedObjectPool();
        config = new TestConfig();
        underlying = (GenericObjectPool<NettyClient>) pool.getObjectPool(endpoint);
        initialActive = underlying.getNumActive();
    }

    @After
    public void teardown() throws Exception {
        if (fakeRedis != null) fakeRedis.stop();
    }

    private IoActionFactory psubIoActionFactory() {
        return socket -> new AbstractIoAction(socket) {
            private boolean responded = false;
            @Override protected Object doRead(InputStream ins) throws IOException {
                byte[] buf = new byte[4096]; int len = ins.read(buf);
                if (len < 0) return null;
                byte[] data = new byte[len];
                System.arraycopy(buf, 0, data, 0, len); return data;
            }
            @Override protected void doWrite(OutputStream ous, Object readResult) throws IOException {
                if (readResult == null || responded) return;
                responded = true;
                ous.write("*3\r\n$9\r\npsubscribe\r\n$5\r\nxpipe*\r\n:1\r\n".getBytes()); ous.flush();
            }
        };
    }

    /**
     * 真实复现：两条完全独立的定时任务线程，通过连接池耦合。
     *
     * 线程A (manager 模拟):
     *   ① 从"在用"列表移除 endpoint → removeUnusedInstances
     *   ② 如果 currentSession 存在, closeConnection + sessions.remove
     *   ③ endpoint 重新加入"在用"列表 → findOrCreateSession → 新 session
     *   ④ 新 session.psubscribeIfAbsent → 从池 borrow
     *
     * 线程B (PsubAction 模拟):
     *   ① 取 latestSession
     *   ② 调 psubscribeIfAbsent → subscribConns 可能已被线程A清空
     *   ③ 发现空了 → 建新 Psubscribe → borrow → 不归还
     *
     * 两线程没有同步、没有 await、没有栅栏。
     * 完全依赖线程调度器自然产生的竞态窗口来制造孤儿。
     * 跟生产环境一样：元信息抖动越频繁、线程调度交错越多，孤儿越多。
     */
    @Test
    public void testRealRaceWithScheduledExecutors() throws Exception {
        final int intervalManager = 30;   // 线程A间隔 (ms)
        final int intervalPsub = 60;      // 线程B间隔 (ms)——更慢，避免每次都赶上 clean
        final int durationMs = 30000;     // 跑 30 秒——让孤儿有机会累积
        running = true;

        // 先创建 sessionA（模拟初始 instance 的 session，固定给线程B用）
        final RedisSession psubSession = new RedisSession(endpoint, scheduled, pool, config);
        psubSession.psubscribeIfAbsent(new NoopCallback(), "xpipe*");
        // 线程A 持有的 session，会被替换（模拟 manager 换 instance）
        latestSession = psubSession;
        System.out.println("[start] initial Active=" + underlying.getNumActive());

        ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(2);

        // === 线程A: 模拟 AbstractInstanceSessionManager（元信息变更） ===
        executor.scheduleAtFixedRate(() -> {
            try {
                // 模拟 removeUnusedInstances:
                // 杀 psubSession（线程B 的 session）——生产里会清理所有关联 session
                psubSession.close();

                // 让线程B 产生孤儿：psubSession.subscribConns 已被清空
                // 等线程B 下一次跑时，它看到的是空 subscribConns → 建新 Psubscribe → orphan

                // 同时模拟 manager 创建新 session（新 instance）
                RedisSession newSession = new RedisSession(endpoint, scheduled, pool, config);
                newSession.psubscribeIfAbsent(new NoopCallback(), "xpipe*");
                latestSession = newSession;
            } catch (Exception e) { }
        }, 0, 50, TimeUnit.MILLISECONDS);

        // === 线程B: 模拟 PsubAction（固定的 session，不跟着换） ===
        executor.scheduleAtFixedRate(() -> {
            try {
                // 线程B 持有一开始绑定的 session 引用，不会因为 manager 操作而改变
                // 即使 manager closed it + 移除了，线程B 还是在用这个老 session
                psubSession.psubscribeIfAbsent(new NoopCallback(), "xpipe*");
            } catch (Exception e) { }
        }, 0, 30, TimeUnit.MILLISECONDS);

        // 让两线程自由竞态跑 durationMs 毫秒
        Thread.sleep(durationMs);
        running = false;
        executor.shutdownNow();
        executor.awaitTermination(2, TimeUnit.SECONDS);

        int finalActive = underlying.getNumActive();
        int activeDelta = finalActive - initialActive;
        String msg = "[result] " + durationMs + "ms 初始Active=" + initialActive
                + " 最终Active=" + finalActive + " 差值=" + activeDelta
                + " Idle=" + underlying.getNumIdle() + " max=" + underlying.getMaxTotal();
        System.err.println(msg);
        java.io.FileWriter fw = new java.io.FileWriter("target/race-final-result.txt");
        fw.write(msg); fw.close();

        // 诊断日志留底，不设置严格断言（timing 敏感）
        System.out.println(msg);

        latestSession.close();
    }

    @Test
    public void testPsubNeverReturnsWithoutRace() throws Exception {
        RedisSession session = new RedisSession(endpoint, scheduled, pool, config);
        session.psubscribeIfAbsent(new NoopCallback(), "xpipe*");
        Assert.assertEquals(1, underlying.getNumActive());
        Assert.assertEquals(0, underlying.getNumIdle());
        session.close();
    }

    private static class NoopCallback implements RedisSession.SubscribeCallback {
        @Override public void message(String channel, String message) { }
        @Override public void fail(Throwable e) { }
    }
}
