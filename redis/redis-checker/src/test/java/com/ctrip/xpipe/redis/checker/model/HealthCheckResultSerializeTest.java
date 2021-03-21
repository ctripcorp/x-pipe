package com.ctrip.xpipe.redis.checker.model;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * @author lishanglin
 * date 2021/3/21
 */
public class HealthCheckResultSerializeTest extends AbstractCheckerTest {

    @Test
    public void testSerialize() {
        Map<DcClusterShard, Map<String, Pair<HostPort, Long>>> crossMastersDelay = Collections.singletonMap(new DcClusterShard("dc1", "cluster1", "shard1"),
                Collections.singletonMap("dc2", new Pair<>(new HostPort("127.0.0.1", 6379), 1000L)));
        Map<HostPort, Boolean> redisAlives = Collections.singletonMap(new HostPort("127.0.0.1", 6379), true);
        Map<HostPort, Long> redisDelays = Collections.singletonMap(new HostPort("127.0.0.1", 6379), 1000L);
        Map<String, Set<String>> warningClusterShards = Collections.singletonMap("cluster1", Collections.emptySet());

        HealthCheckResult result = new HealthCheckResult();
        result.encodeCrossMasterDelays(crossMastersDelay);
        result.encodeRedisAlives(redisAlives);
        result.encodeRedisDelays(redisDelays);
        result.setWarningClusterShards(warningClusterShards);

        String codecStr = Codec.DEFAULT.encode(result);
        logger.info("[testSerialize] codec {}", codecStr);

        HealthCheckResult decodedResult = Codec.DEFAULT.decode(codecStr, HealthCheckResult.class);
        logger.info("[testSerialize] decode {}", decodedResult);

        Assert.assertEquals(result, decodedResult);
        Assert.assertEquals(redisAlives, decodedResult.decodeRedisAlives());
        Assert.assertEquals(redisDelays, decodedResult.decodeRedisDelays());
        Assert.assertEquals(crossMastersDelay, decodedResult.decodeCrossMasterDelays());
        Assert.assertEquals(warningClusterShards, decodedResult.getWarningClusterShards());
    }

}
