package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author wenchao.meng
 *         <p>
 *         Mar 22, 2018
 */
public class InfoResultExtractorTest extends AbstractRedisTest {


    @Test
    public void testParse() {

        String value = "# Stats\n" +
                "total_connections_received:6\n" +
                "total_commands_processed:2490\n" +
                "instantaneous_ops_per_sec:1\n" +
                "total_net_input_bytes:91177\n" +
                "total_net_output_bytes:48287\n" +
                "instantaneous_input_kbps:0.04\n" +
                "instantaneous_output_kbps:0.00\n" +
                "rejected_connections:0\n" +
                "sync_full: 2\r\n" +
                "sync_partial_ok :0\n" +
                "sync_partial_err : 2\n" +
                "expired_keys:0\n" +
                "evicted_keys:0\n" +
                "keyspace_hits:0\n" +
                "keyspace_misses:0\n" +
                "pubsub_channels:0\n" +
                "pubsub_patterns:0\n" +
                "latest_fork_usec:458\n" +
                "migrate_cached_sockets:0\n" +
                "slave_expires_tracked_keys:0\n" +
                "active_defrag_hits:0\n" +
                "active_defrag_misses:0\n" +
                "active_defrag_key_hits:0\n" +
                "active_defrag_key_misses:0";

        InfoResultExtractor extractor = new InfoResultExtractor(value);

        Assert.assertEquals(null, extractor.extract("sync_full1"));
        Assert.assertEquals("2", extractor.extract("sync_full"));
        Assert.assertEquals(new Integer(2), extractor.extractAsInteger("sync_full"));
        Assert.assertEquals("0", extractor.extract("sync_partial_ok"));
        Assert.assertEquals("2", extractor.extract("sync_partial_err"));

    }

    @Test
    public void testParseSentinel() {
        String info = "# Sentinel\r\n" +
                "sentinel_masters:1\r\n" +
                "sentinel_tilt:0\r\n" +
                "sentinel_running_scripts:0\r\n" +
                "sentinel_scripts_queue_length:0\r\n" +
                "sentinel_simulate_failure_flags:0\r\n" +
                "master0:name=cluster_shyinshard1,status=ok,address=10.2.58.242:6379,slaves=2,sentinels=3\r\n";
        InfoResultExtractor extractor = new InfoResultExtractor(info);

        Assert.assertEquals("name=cluster_shyinshard1,status=ok,address=10.2.58.242:6379,slaves=2,sentinels=3",
                extractor.extract("master0"));


    }

    @Test
    public void testConcurrentInfoResultExtractor() throws InterruptedException {
        String info = "";
        int max = 1000;
        for(int i = 0; i < max; i++) {
            info += i + ":" + i + "\r\n";
        }
        CountDownLatch countDownLatch = new CountDownLatch(100);
        InfoResultExtractor extractor = new InfoResultExtractor(info);
        AtomicInteger success = new AtomicInteger(0);
        int num = 100;
        for(int i = 0; i < num; i++) {
            executors.execute(new Runnable() {
                @Override
                public void run() {
                    String key = String.format("%d", max - 1);
                    if(key.equals(extractor.extract(key))) {
                        success.incrementAndGet();
                    } 
                    countDownLatch.countDown();
                }
            });
        }
        countDownLatch.await();
        Assert.assertEquals(success.get(), num);
        


    }
}
