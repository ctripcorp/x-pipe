package com.ctrip.xpipe.redis.checker.healthcheck.actions.keeper.info;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by yu
 * 2023/8/29
 */
public class RedisUsedMemoryCollectorTest extends AbstractCheckerTest {

    private RedisHealthCheckInstance instance;

    private RedisUsedMemoryCollector listener;

    private RedisInfoActionContext context;

    private static final String INFO_RESPONSE_OF_REDIS = "# Memory\n" +
            "used_memory:550515888\n" +
            "used_memory_human:525.01M\n" +
            "used_memory_rss:544837632\n" +
            "used_memory_rss_human:519.60M\n" +
            "used_memory_peak:550862048\n" +
            "used_memory_peak_human:525.34M\n" +
            "used_memory_peak_perc:99.94%\n" +
            "used_memory_overhead:212334872\n" +
            "used_memory_startup:803592\n" +
            "used_memory_dataset:338181016\n" +
            "used_memory_dataset_perc:61.52%\n" +
            "allocator_allocated:550579680\n" +
            "allocator_active:551002112\n" +
            "allocator_resident:560062464\n" +
            "total_system_memory:8201191424\n" +
            "total_system_memory_human:7.64G\n" +
            "used_memory_lua:45056\n" +
            "used_memory_lua_human:44.00K\n" +
            "used_memory_scripts:816\n" +
            "used_memory_scripts_human:816B\n" +
            "number_of_cached_scripts:2\n" +
            "used_memory_scripts_human:0B\n" +
            "number_of_cached_scripts:0\n" +
            "maxmemory:2415919104";

    private static final String INFO_RESPONSE_OF_ROR = "# Memory\n" +
            "used_memory:109632408\n" +
            "used_memory_human:104.55M\n" +
            "used_memory_rss:92024832\n" +
            "used_memory_rss_human:87.76M\n" +
            "used_memory_peak:109773720\n" +
            "used_memory_peak_human:104.69M\n" +
            "used_memory_peak_perc:99.87%\n" +
            "used_memory_overhead:76041118\n" +
            "used_memory_startup:8819264\n" +
            "used_memory_dataset:33601768\n" +
            "used_memory_dataset_perc:33.33%\n" +
            "allocator_allocated:115857800\n" +
            "allocator_active:118788096\n" +
            "allocator_resident:127303680\n" +
            "total_system_memory:270332620800\n" +
            "total_system_memory_human:251.77G\n" +
            "used_memory_scripts_human:0B\n" +
            "number_of_cached_scripts:0\n" +
            "maxmemory:2415919104\n" +
            "# Swap\n" +
            "swap_used_db_size:2472\n" +
            "swap_max_db_size:32212254720\n" +
            "swap_used_db_percent:0.00%\n" +
            "swap_used_disk_size:96530432\n" +
            "swap_disk_capacity:214748364800\n" +
            "swap_used_disk_percent:0.04%\n" +
            "swap_error_count:1\n" +
            "swap_swapin_attempt_count:2\n" +
            "swap_swapin_not_found_count:1\n" +
            "swap_swapin_no_io_count:0\n" +
            "swap_swapin_memory_hit_perc:0.00%\n" +
            "swap_swapin_keyspace_hit_perc:50.00%\n" +
            "swap_swapin_not_found_coldfilter_cuckoofilter_filt_count:1\n" +
            "swap_swapin_not_found_coldfilter_absentcache_filt_count:0\n" +
            "swap_swapin_not_found_coldfilter_miss:0\n" +
            "swap_swapin_not_found_coldfilter_filt_perc:0.00%" ;


    private static final String INFO_RESPONSE_OF_ROR2 = "# Memory\n" +
            "used_memory:2415919104\n" +
            "maxmemory:2415919104\n" +
            "# Swap\n" +
            "swap_used_db_size:1207959551\n" +
            "# Keyspace\n" +
            "db0:keys=2,evicts=1,metas=0,expires=0,avg_ttl=0";


    private static final String INFO_RESPONSE_OF_ROR3 = "# Memory\n" +
            "used_memory:2415919104\n" +
            "maxmemory:2415919104\n" +
            "# Swap\n" +
            "swap_used_db_size:1207959552\n" +
            "# Keyspace\n" +
            "db0:keys=10,evicts=30,metas=0,expires=0,avg_ttl=0";

    private static final String INFO_RESPONSE_OF_ROR4 = "# Memory\n" +
            "used_memory:2415919104\n" +
            "maxmemory:2415919104\n" +
            "# Swap\n" +
            "swap_used_db_size:1207959552\n" +
            "# Keyspace\n";

    private static final String INFO_RESPONSE_OF_ROR5 = "# Memory\n" +
            "used_memory:2415919104\n" +
            "maxmemory:2415919104\n" +
            "# Swap\n" +
            "swap_used_db_size:1207959552\n" +
            "# Keyspace\n" +
            "db0:evicts=30,metas=0,expires=0,avg_ttl=0";

    private static final String INFO_RESPONSE_OF_ROR6 = "# Memory\n" +
            "used_memory:2415919104\n" +
            "maxmemory:2415919104\n" +
            "# Swap\n" +
            "swap_used_db_size:1207959552\n" +
            "# Keyspace\n" +
            "db0:keys=10,metas=0,expires=0,avg_ttl=0";

    private static final String INFO_RESPONSE_OF_ROR7 = "# Memory\n" +
            "used_memory:2415919104\n" +
            "maxmemory:2415919104\n" +
            "# Swap\n" +
            "swap_used_db_size:1207959552\n" +
            "# Keyspace\n" +
            "db0:keys=0,evicts=30,metas=0,expires=0,avg_ttl=0";


    @Before
    public void before() throws Exception {
        listener = new RedisUsedMemoryCollector();
        instance = newRandomRedisHealthCheckInstance(FoundationService.DEFAULT.getDataCenter(), ClusterType.ONE_WAY, randomPort());
    }

    @Test
    public void testGetUsedMemoryInfoWithRor2() {
        // test dbSize < maxmemory * 0.5
        context = new RedisInfoActionContext(instance, INFO_RESPONSE_OF_ROR2);
        listener.onAction(context);
        Assert.assertTrue(listener.worksfor(context));
        Assert.assertEquals(1, listener.getDcClusterShardUsedMemory().size());
        Assert.assertEquals(2415919104L, (long) listener.getDcClusterShardUsedMemory().get(new DcClusterShard("jq", "cluster", "shard")));

    }

    @Test
    public void testGetUsedMemoryInfoWithRor3() {
        // test dbSize >= maxmemory * 0.5, keyspace normal
        context = new RedisInfoActionContext(instance, INFO_RESPONSE_OF_ROR3);
        listener.onAction(context);
        Assert.assertTrue(listener.worksfor(context));
        Assert.assertEquals(1, listener.getDcClusterShardUsedMemory().size());
        Assert.assertEquals(2415919104L * 4, (long) listener.getDcClusterShardUsedMemory().get(new DcClusterShard("jq", "cluster", "shard")));

    }

    @Test
    public void testGetUsedMemoryInfoWithRedis() {
        context = new RedisInfoActionContext(instance, INFO_RESPONSE_OF_REDIS);
        listener.onAction(context);
        Assert.assertTrue(listener.worksfor(context));
        Assert.assertEquals(1, listener.getDcClusterShardUsedMemory().size());
        Assert.assertEquals(550515888, (long) listener.getDcClusterShardUsedMemory().get(new DcClusterShard("jq", "cluster", "shard")));

    }
}