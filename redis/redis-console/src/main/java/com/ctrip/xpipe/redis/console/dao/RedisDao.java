package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.core.redis.RunidGenerator;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.stereotype.Repository;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ContainerLoader;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author shyin
 *         <p>
 *         Aug 31, 2016
 */
@Repository
public class RedisDao extends AbstractXpipeConsoleDAO {

    private RunidGenerator idGenerator = RunidGenerator.DEFAULT;
    private RedisTblDao redisTblDao;
    private DcClusterShardTblDao dcClusterShardTblDao;

    @PostConstruct
    private void postConstruct() {
        try {
            redisTblDao = ContainerLoader.getDefaultContainer().lookup(RedisTblDao.class);
            dcClusterShardTblDao = ContainerLoader.getDefaultContainer().lookup(DcClusterShardTblDao.class);
        } catch (ComponentLookupException e) {
            throw new ServerException("Cannot construct dao.", e);
        }
    }


    public RedisTbl findByPK(long id){
        return queryHandler.handleQuery(new DalQuery<RedisTbl>() {

            @Override
            public RedisTbl doQuery() throws DalException {
                return redisTblDao.findByPK(id, RedisTblEntity.READSET_FULL);
            }
        });
    }

    public List<RedisTbl> findAllByDcClusterShard(final long dcClusterShardId) {

        return queryHandler.handleQuery(new DalQuery<List<RedisTbl>>() {

            @Override
            public List<RedisTbl> doQuery() throws DalException {
                return redisTblDao.findAllByDcClusterShardId(dcClusterShardId, null, RedisTblEntity.READSET_FULL);
            }
        });
    }

    public List<RedisTbl> findAllByDcClusterShard(final long dcClusterShardId, String redisRole) {

        return queryHandler.handleQuery(new DalQuery<List<RedisTbl>>() {

            @Override
            public List<RedisTbl> doQuery() throws DalException {
                return redisTblDao.findAllByDcClusterShardId(dcClusterShardId, redisRole, RedisTblEntity.READSET_FULL);
            }
        });
    }

    public List<RedisTbl> findAllByDcCluster(long dcId, String clusterName, String redisRole) {
        return queryHandler.handleQuery(new DalQuery<List<RedisTbl>>() {
            @Override
            public List<RedisTbl> doQuery() throws DalException {
                return redisTblDao.findAllByDcCluster(dcId, clusterName, redisRole, RedisTblEntity.READSET_FULL);
            }
        });
    }
    @DalTransaction
    public int[] createRedisesBatch(List<RedisTbl> redises) {
        if (null != redises) {
            Map<Long, String> cache = new HashMap<Long, String>();
            for (RedisTbl redis : redises) {
                checkRedisNotExist(redis);
                if (redis.getRedisRole().equals(XPipeConsoleConstant.ROLE_KEEPER)) {
                    if (null == cache.get(redis.getDcClusterShardId())) {
                        String newKeeperId = getToCreateKeeperId(redis);
                        redis.setRunId(newKeeperId);
                        cache.put(redis.getDcClusterShardId(), newKeeperId);
                    } else {
                        redis.setRunId(cache.get(redis.getDcClusterShardId()));
                    }
                }
            }

            return queryHandler.handleBatchInsert(new DalQuery<int[]>() {
                @Override
                public int[] doQuery() throws DalException {
                    return redisTblDao.insertBatch(redises.toArray(new RedisTbl[redises.size()]));
                }
            });

        }
        return null;
    }

    private void checkRedisNotExist(RedisTbl redisTbl) {
        RedisTbl otherRedis = queryHandler.handleQuery(new DalQuery<RedisTbl>() {
            @Override
            public RedisTbl doQuery() throws DalException {
                return redisTblDao.findWithIpPort(redisTbl.getRedisIp(), redisTbl.getRedisPort(), RedisTblEntity.READSET_IP_AND_PORT);
            }
        });
        if(otherRedis != null) {
            throw new IllegalArgumentException("Redis already exists, " + otherRedis.getRedisIp() + ":" + otherRedis.getRedisPort());
        }
    }

    @DalTransaction
    public RedisTbl createRedisesBatch(RedisTbl redis) throws DalException {
        if (null == redis) throw new BadRequestException("Null redis cannot be created");

        if (redis.getRedisRole().equals(XPipeConsoleConstant.ROLE_KEEPER)) {
            redis.setRunId(getToCreateKeeperId(redis));
        }
        redisTblDao.insertBatch(redis);
        return redisTblDao.findWithBasicConfigurations(redis.getRunId(), redis.getDcClusterShardId(), redis.getRedisIp(), redis.getRedisPort(), RedisTblEntity.READSET_FULL);
    }

    @DalTransaction
    public void deleteRedisesBatch(List<RedisTbl> redises) {
        if (null != redises && !redises.isEmpty()) {
            for (RedisTbl redis : redises) {
                redis.setRunId(generateDeletedName(redis.getRunId()));
            }
            queryHandler.handleBatchDelete(new DalQuery<int[]>() {
                @Override
                public int[] doQuery() throws DalException {
                    return redisTblDao.deleteBatch(redises.toArray(new RedisTbl[redises.size()]), RedisTblEntity.UPDATESET_FULL);
                }
            }, true);
        } else {
            logger.info("[deleteRedisesBatch][null]");
        }
    }

    @DalTransaction
    public void updateBatch(List<RedisTbl> redises) {
        queryHandler.handleBatchUpdate(new DalQuery<int[]>() {
            @Override
            public int[] doQuery() throws DalException {
                return redisTblDao.updateBatch(redises.toArray(new RedisTbl[redises.size()]), RedisTblEntity.UPDATESET_FULL);
            }
        });
    }

    @DalTransaction
    public void updateBatchMaster(List<RedisTbl> redises) {

         queryHandler.handleBatchUpdate(new DalQuery<int[]>() {
            @Override
            public int[] doQuery() throws DalException {
                return redisTblDao.updateBatchMaster(redises.toArray(new RedisTbl[redises.size()]), RedisTblEntity.UPDATESET_FULL);
            }
        });

    }

    @DalTransaction
    public void updateBatchKeeperActive(List<RedisTbl> redises) {

        queryHandler.handleBatchUpdate(new DalQuery<int[]>() {
            @Override
            public int[] doQuery() throws DalException {
                return redisTblDao.updateBatchKeeperActive(redises.toArray(new RedisTbl[redises.size()]), RedisTblEntity.UPDATESET_FULL);
            }
        });
    }

    @DalTransaction
    public void handleUpdate(List<RedisTbl> toCreate, List<RedisTbl> toDelete, List<RedisTbl> left) throws DalException {
        if (null != toCreate && toCreate.size() > 0) {
            logger.info("[handleUpdate][create]{}, {}", toCreate.size(), toCreate);
            createRedisesBatch(toCreate);
        }

        if (null != toDelete && toDelete.size() > 0) {
            logger.info("[handleUpdate][delete]{}, {}", toDelete.size(), toDelete);
            deleteRedisesBatch(toDelete);
        }

        if (null != left && left.size() > 0) {
            logger.info("[handleUpdate][left]{}, {}", left.size(), left);
            updateBatch(left);
        }
    }

    public static List<RedisTbl> findWithRole(List<RedisTbl> redises, String role) {
        List<RedisTbl> results = new LinkedList<>();
        if (null != redises) {
            for (RedisTbl redis : redises) {
                if (redis.getRedisRole().equals(role)) {
                    results.add(redis);
                }
            }
        }
        return results;
    }

    private String getToCreateKeeperId(final RedisTbl redis) {
        if (null == redis) throw new BadRequestException("Cannot obtain keeper-id from null.");
        List<RedisTbl> dcClusterShardRedises = queryHandler.handleQuery(new DalQuery<List<RedisTbl>>() {
            @Override
            public List<RedisTbl> doQuery() throws DalException {
                return redisTblDao.findAllWithHistoryByDcClusterShardId(redis.getDcClusterShardId(), RedisTblEntity.READSET_FULL);
            }
        });

        if (null == dcClusterShardRedises) {
            return generateUniqueKeeperId(redis);
        } else {
            List<RedisTbl> keepers = findWithRole(dcClusterShardRedises, XPipeConsoleConstant.ROLE_KEEPER);
            if (null != keepers && keepers.size() > 0) {
                RedisTbl historyKeeper = keepers.get(0);
                if (historyKeeper.isDeleted()) {
                    int index = historyKeeper.getRunId().indexOf(DELETED_NAME_SPLIT_TAG);
                    if (index > 0) {
                        return historyKeeper.getRunId().substring(index + 1);
                    }
                    return historyKeeper.getRunId();
                } else {
                    return historyKeeper.getRunId();
                }
            } else {
                return generateUniqueKeeperId(redis);
            }
        }
    }

    private String generateUniqueKeeperId(final RedisTbl redis) {
        final String runId = idGenerator.generateRunid();

        // check for unique runId
        DcClusterShardTbl targetDcClusterShard = queryHandler.handleQuery(new DalQuery<DcClusterShardTbl>() {
            @Override
            public DcClusterShardTbl doQuery() throws DalException {
                return dcClusterShardTblDao.findByPK(redis.getDcClusterShardId(), DcClusterShardTblEntity.READSET_FULL);
            }
        });
        if (null == targetDcClusterShard) throw new BadRequestException("Cannot find related dc-cluster-shard");

        List<RedisTbl> redisWithSameRunId = queryHandler.handleQuery(new DalQuery<List<RedisTbl>>() {
            @Override
            public List<RedisTbl> doQuery() throws DalException {
                return redisTblDao.findByRunid(runId, RedisTblEntity.READSET_FULL);
            }
        });

        if (null != redisWithSameRunId && redisWithSameRunId.size() > 0) {
            for (final RedisTbl tmpRedis : redisWithSameRunId) {
                DcClusterShardTbl tmpDcClusterShard = queryHandler.handleQuery(new DalQuery<DcClusterShardTbl>() {
                    @Override
                    public DcClusterShardTbl doQuery() throws DalException {
                        return dcClusterShardTblDao.findByPK(tmpRedis.getDcClusterShardId(), DcClusterShardTblEntity.READSET_FULL);
                    }
                });
                if (null != tmpDcClusterShard
                        && targetDcClusterShard.getShardId() == tmpDcClusterShard.getShardId()) {
                    throw new ServerException("Cannot generate unque keeper id, please retry.");
                }
            }
        }

        return runId;
    }
}
