package com.ctrip.xpipe.redis.console.proxy.impl;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.console.model.ProxyModel;
import com.ctrip.xpipe.redis.console.proxy.ProxyMonitorCollector;
import com.ctrip.xpipe.redis.console.proxy.ProxyMonitorCollectorManager;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Lists;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultProxyMonitorCollectorTest extends AbstractRedisTest {

        @Test
        public void testManual() throws Exception {
                ProxyModel proxyModel = new ProxyModel().setUri("PROXYTCP://10.2.131.200:80").setDcName("FAT-AWS")
                                .setId(1L).setActive(true).setMonitorActive(true);
                XpipeNettyClientKeyedObjectPool keyedObjectPool = getXpipeNettyClientKeyedObjectPool();
                ProxyMonitorCollector result = new com.ctrip.xpipe.redis.console.proxy.impl.DefaultProxyMonitorCollector(
                                scheduled, keyedObjectPool, proxyModel, () -> 10000) {
                        @Override
                        protected int getStartInterval() {
                                return 0;
                        }
                };
                result.start();
                scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
                        @Override
                        protected void doRun() throws Exception {
                                logger.info("[getProxyInfo] {}", result.getProxyInfo());
                                logger.info("[getPingStatsResults] {}", result.getPingStatsResults());
                                logger.info("[getTunnelSocketStatsResults] {}", result.getSocketStatsResults());
                                logger.info("[getTunnelStatsResults] {}", result.getTunnelStatsResults());
                                logger.info("[getTunnelInfos] {}", result.getTunnelInfos());
                                logger.info("");
                                logger.info("");
                                logger.info("");
                        }
                }, 1000 * 2, 1000, TimeUnit.MILLISECONDS);
                sleep(1000 * 60 * 100);
        }

        @Ignore
        @Test
        public void testIntegrate() throws Exception {
                ProxyModel proxyModel1 = new ProxyModel().setUri("PROXYTCP://10.2.131.201:80").setDcName("NTGXH")
                                .setId(3L)
                                .setActive(true);
                XpipeNettyClientKeyedObjectPool keyedObjectPool = getXpipeNettyClientKeyedObjectPool();
                ProxyMonitorCollector result1 = new com.ctrip.xpipe.redis.console.proxy.impl.DefaultProxyMonitorCollector(
                                scheduled, keyedObjectPool, proxyModel1, () -> 10000);
                result1.start();

                ProxyModel proxyModel2 = new ProxyModel().setUri("PROXYTCP://10.2.131.200:80").setDcName("FAT-AWS")
                                .setId(1L)
                                .setActive(true);
                ProxyMonitorCollector result2 = new com.ctrip.xpipe.redis.console.proxy.impl.DefaultProxyMonitorCollector(
                                scheduled, keyedObjectPool, proxyModel2, () -> 10000);
                result2.start();

                DefaultProxyChainAnalyzer analyzer = new DefaultProxyChainAnalyzer();

                ProxyMonitorCollectorManager manager = mock(ProxyMonitorCollectorManager.class);
                when(manager.getProxyMonitorResults()).thenReturn(Lists.newArrayList(result1, result2));
                analyzer.setProxyMonitorCollectorManager(manager);

                MetaCache metaCache = mock(MetaCache.class);
                when(metaCache.findClusterShard(any(HostPort.class))).thenReturn(null);
                when(metaCache.findClusterShard(new HostPort("10.2.73.170", 6379)))
                                .thenReturn(new Pair<>("cluster_shyin", "shard1"));
                when(metaCache.findClusterShard(new HostPort("10.2.73.170", 6389)))
                                .thenReturn(new Pair<>("cluster_shyin", "shard2"));

                when(metaCache.getActiveDc(anyString())).thenReturn("NTGXH");
                analyzer.setMetaCache(metaCache);

                analyzer.setExecutors(executors);

        }

}