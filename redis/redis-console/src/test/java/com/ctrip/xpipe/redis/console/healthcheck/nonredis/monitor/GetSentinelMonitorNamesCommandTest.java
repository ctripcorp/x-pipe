package com.ctrip.xpipe.redis.console.healthcheck.nonredis.monitor;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.dao.ClusterDao;
import com.ctrip.xpipe.redis.console.dao.ShardDao;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.service.DcClusterShardService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.core.util.SentinelUtil;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class GetSentinelMonitorNamesCommandTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private ClusterDao clusterDao;

    @Autowired
    private ShardDao shardDao;

    @Autowired
    private DcService dcService;

    @Autowired
    private DcClusterShardService dcClusterShardService;

    @Test
    public void testGetMonitorNames() throws Exception {
        Set<String> monitorNames = (new GetSentinelMonitorNamesCommand(dcService, clusterDao, shardDao, executors, scheduled)).execute().get();
        Assert.assertEquals(16, monitorNames.size());

        monitorNames.forEach(monitorName -> {
            SentinelUtil.SentinelInfo sentinelInfo = SentinelUtil.getSentinelInfoFromMonitorName(monitorName);
            DcClusterShardTbl dcClusterShardTbl = dcClusterShardService.find(sentinelInfo.getIdc(), sentinelInfo.getClusterName(), sentinelInfo.getShardName());
            Assert.assertNotNull(dcClusterShardTbl);
        });
    }

    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/apptest.sql");
    }

}
