package com.ctrip.xpipe.redis.ctrip.integratedtest.console.migration.mock;

import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationPublishState;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.service.RedisService;

import java.util.ArrayList;
import java.util.List;

/**
 * @author lishanglin
 * date 2021/3/29
 */
public class MockMigrationPublishState extends MigrationPublishState {

    private static final String mockPublishInfo = "{\"startTime\":\"2021-10-08T11:41:26.223\",\"endTime\":\"2021-10-08T11:41:26.334\",\"publishAddress\":\"http://mock.api\",\"clusterName\":\"test-cluster\",\"primaryDcName\":\"oy\",\"newMasters\":[\"10.0.0.1:6379\",\"10.0.0.1:6479\",\"10.0.0.1:6579\",\"10.0.0.1:6679\",\"10.0.0.1:6779\"],\"success\":true,\"message\":\"设置成功\"}";

    public MockMigrationPublishState(MigrationCluster holder) {
        super(holder);
    }

    @Override
    public void doAction() {
        getHolder().getClusterService().updateActivedcId(getHolder().getCurrentCluster().getId(), getHolder().destDcId());
        getHolder().updatePublishInfo(mockPublishInfo);

        MigrationCluster migrationCluster = getHolder();
        RedisService redisService = migrationCluster.getRedisService();
        List<RedisTbl> prevDcRedises = redisService.findAllRedisesByDcClusterName(migrationCluster.fromDc(), migrationCluster.clusterName());
        List<RedisTbl> newDcRedises = redisService.findAllRedisesByDcClusterName(migrationCluster.destDc(), migrationCluster.clusterName());

        List<RedisTbl> toUpdate = new ArrayList<>();
        toUpdate.addAll(prevDcRedises);
        toUpdate.addAll(newDcRedises);
        migrationCluster.getRedisService().updateBatchMaster(toUpdate);

        updateAndProcess(nextAfterSuccess());
    }


}
