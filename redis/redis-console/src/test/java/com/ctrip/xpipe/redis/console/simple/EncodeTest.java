package com.ctrip.xpipe.redis.console.simple;

import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import org.junit.Test;

/**
 * @author wenchao.meng
 *         <p>
 *         Nov 10, 2017
 */
public class EncodeTest extends AbstractConsoleTest{

    @Test
    public void testEncode(){

        ClusterMeta clusterMeta = new ClusterMeta();
//        clusterMeta.addShard();

        clusterMeta.addShard(new ShardMeta());

        JsonCodec.INSTANCE.encode(clusterMeta);

        logger.info("{}", clusterMeta);


    }

}
