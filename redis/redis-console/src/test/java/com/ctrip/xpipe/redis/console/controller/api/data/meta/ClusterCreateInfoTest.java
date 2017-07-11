package com.ctrip.xpipe.redis.console.controller.api.data.meta;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.google.common.collect.Lists;
import org.junit.Test;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 11, 2017
 */
public class ClusterCreateInfoTest extends AbstractConsoleTest{

    @Test
    public void testCodec(){

        ClusterCreateInfo clusterCreateInfo = new ClusterCreateInfo();
        clusterCreateInfo.setClusterName(randomString(10));
        clusterCreateInfo.setDcs(Lists.newArrayList("jq", "oy"));
        clusterCreateInfo.setDesc(randomString(10));

        logger.info("{}", clusterCreateInfo);


    }
}
