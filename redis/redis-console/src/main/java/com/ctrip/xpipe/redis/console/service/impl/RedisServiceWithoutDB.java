package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.checker.spring.ConsoleDisableDbCondition;
import com.ctrip.xpipe.redis.checker.spring.DisableDbMode;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.ShardModel;
import com.ctrip.xpipe.redis.console.resources.ConsolePortalService;
import com.ctrip.xpipe.redis.console.service.KeeperBasicInfo;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.console.service.exception.ResourceNotFoundException;
import com.ctrip.xpipe.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

import java.util.List;

@Service
@Conditional(ConsoleDisableDbCondition.class)
@DisableDbMode(true)
public class RedisServiceWithoutDB implements RedisService {

    @Autowired
    private ConsolePortalService consolePortalService;

    @Override
    public RedisTbl find(long id) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RedisTbl findByIpPort(String ip, int port) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<RedisTbl> findAllRedisWithSameIP(String ip) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<RedisTbl> findAllRedisByIp(String ip) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<RedisTbl> findAllByDcClusterShard(long dcClusterShardId) {
        return consolePortalService.findAllByDcClusterShard(dcClusterShardId);
    }

    @Override
    public List<RedisTbl> findAllRedisesByDcClusterName(String dcId, String clusterId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<RedisTbl> findAllKeepersByDcClusterName(String dcId, String clusterId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<RedisTbl> findAllByDcClusterShard(String dcId, String clusterId, String shardId) throws ResourceNotFoundException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<RedisTbl> findRedisesByDcClusterShard(String dcId, String clusterId, String shardId) throws ResourceNotFoundException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<RedisTbl> findKeepersByDcClusterShard(String dcId, String clusterId, String shardId) throws ResourceNotFoundException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void insertRedises(String dcId, String clusterId, String shardId, List<Pair<String, Integer>> addrs) throws DalException, ResourceNotFoundException {
        consolePortalService.insertRedises(dcId, clusterId, shardId, addrs);
    }

    @Override
    public void deleteRedises(String dcId, String clusterId, String shardId, List<Pair<String, Integer>> addrs) throws ResourceNotFoundException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int insertKeepers(String dcId, String clusterId, String shardId, List<KeeperBasicInfo> keepers) throws DalException, ResourceNotFoundException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<RedisTbl> deleteKeepers(String dcId, String clusterId, String shardId) throws DalException, ResourceNotFoundException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateByPK(RedisTbl redis) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBatchMaster(List<RedisTbl> redises) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBatchKeeperActive(List<RedisTbl> redises) {
        consolePortalService.updateBatchKeeperActive(redises);
    }

    @Override
    public void updateSourceKeepers(String srcDcName, String clusterName, String shardName, long dstDcId, ShardModel sourceShard) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateRedises(String dcName, String clusterName, String shardName, ShardModel shardModel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<RedisTbl> findAllKeeperContainerCountInfo() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Long> findClusterIdsByKeeperContainer(long keeperContainerId) {
        throw new UnsupportedOperationException();
    }
}
