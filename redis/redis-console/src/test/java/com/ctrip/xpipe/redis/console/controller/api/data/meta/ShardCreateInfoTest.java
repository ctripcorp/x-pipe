package com.ctrip.xpipe.redis.console.controller.api.data.meta;

import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 11, 2017
 */
public class ShardCreateInfoTest extends AbstractConsoleTest {

    @Test
    public void testJson() {

        List<ShardCreateInfo> shards = new LinkedList<>();

        for (int i = 0; i < 3; i++) {

            ShardCreateInfo shardCreateInfo = new ShardCreateInfo();
            shardCreateInfo.setShardName("shard" + i);
            shardCreateInfo.setShardMonitorName("shardMonitor" + i);
            shards.add(shardCreateInfo);
        }

        logger.info("{}", JsonCodec.INSTANCE.encode(shards));
    }
}
