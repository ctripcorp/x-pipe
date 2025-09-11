package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.ApplierTbl;
import com.ctrip.xpipe.redis.console.model.ApplierTblDao;
import com.ctrip.xpipe.redis.console.model.ApplierTblEntity;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import jakarta.annotation.PostConstruct;
import org.codehaus.plexus.PlexusContainer;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ContainerLoader;

import java.util.List;

@Repository
public class ApplierDao extends AbstractXpipeConsoleDAO {

    private ApplierTblDao applierTblDao;

    @Autowired
    private PlexusContainer container;

    @PostConstruct
    private void postConstruct() {
        try {
            applierTblDao = container.lookup(ApplierTblDao.class);
        } catch (ComponentLookupException e) {
            throw new ServerException("Cannot construct dao.", e);
        }
    }

    public ApplierTbl findByPK(long id) {
        return queryHandler.handleQuery(new DalQuery<ApplierTbl>() {
            @Override
            public ApplierTbl doQuery() throws DalException {
                return applierTblDao.findByPK(id, ApplierTblEntity.READSET_FULL);
            }
        });
    }

    public List<ApplierTbl> findByDcClusterShard(long dcClusterShardId) {
        return queryHandler.handleQuery(new DalQuery<List<ApplierTbl>>() {
            @Override
            public List<ApplierTbl> doQuery() throws DalException {
                return applierTblDao.findAppliersByDcClusterShard(dcClusterShardId, ApplierTblEntity.READSET_FULL);
            }
        });
    }

    public ApplierTbl findByIpPort(String ip, int port) {
        return queryHandler.handleQuery(new DalQuery<ApplierTbl>() {
            @Override
            public ApplierTbl doQuery() throws DalException {
                return applierTblDao.findByIpPort(ip, port, ApplierTblEntity.READSET_FULL);
            }
        });
    }

    public List<ApplierTbl> findByShard(long shardId) {
        return queryHandler.handleQuery(new DalQuery<List<ApplierTbl>>() {
            @Override
            public List<ApplierTbl> doQuery() throws DalException {
                return applierTblDao.findAllByShard(shardId, ApplierTblEntity.READSET_FULL);
            }
        });
    }

    @DalTransaction
    public int[] createApplierBatch(List<ApplierTbl> appliers) {
        if (null != appliers && !appliers.isEmpty()) {
            for (ApplierTbl applier : appliers) {
                checkApplierNotExist(applier);
            }

            return queryHandler.handleBatchInsert(new DalQuery<int[]>() {
                @Override
                public int[] doQuery() throws DalException {
                    return applierTblDao.insertBatch(appliers.toArray(new ApplierTbl[appliers.size()]));
                }
            });
        }

        return null;
    }

    private void checkApplierNotExist(ApplierTbl applierTbl) {
        ApplierTbl exist = findByIpPort(applierTbl.getIp(), applierTbl.getPort());
        if (exist != null) {
            throw new IllegalArgumentException(String.format("applier %s:%d already exist!!",
                    applierTbl.getIp(), applierTbl.getPort()));
        }
    }

    @DalTransaction
    public void updateApplierBatch(List<ApplierTbl> appliers) {
        queryHandler.handleBatchUpdate(new DalQuery<int[]>() {
            @Override
            public int[] doQuery() throws DalException {
                return applierTblDao.updateBatch(appliers.toArray(new ApplierTbl[appliers.size()]),
                        ApplierTblEntity.UPDATESET_FULL);
            }
        });
    }

    @DalTransaction
    public void updateBatchApplierActive(List<ApplierTbl> appliers) {
        queryHandler.handleBatchUpdate(new DalQuery<int[]>() {
            @Override
            public int[] doQuery() throws DalException {
                return applierTblDao.updateBatchApplierActive(appliers.toArray(new ApplierTbl[appliers.size()]),
                        ApplierTblEntity.UPDATESET_FULL);
            }
        });
    }

    @DalTransaction
    public void deleteApplierBatch(List<ApplierTbl> appliers) {
        if (appliers != null && !appliers.isEmpty()) {
            queryHandler.handleBatchDelete(new DalQuery<int[]>() {
                @Override
                public int[] doQuery() throws DalException {
                    return applierTblDao.deleteBatch(appliers.toArray(new ApplierTbl[appliers.size()]),
                            ApplierTblEntity.UPDATESET_FULL);
                }
            }, true);
        } else {
            logger.info("[deleteApplierBatch][null]");
        }
    }

    @DalTransaction
    public void handleUpdate(List<ApplierTbl> toCreate, List<ApplierTbl> toDelete, List<ApplierTbl> toUpdate) {
        if (null != toCreate && toCreate.size() > 0) {
            logger.info("[handleUpdate]create appliers {}, {}", toCreate.size(), toCreate);
            createApplierBatch(toCreate);
        }

        if (null != toDelete && toDelete.size() > 0) {
            logger.info("[handleUpdate]delete appliers {}, {}", toDelete.size(), toDelete);
            deleteApplierBatch(toDelete);
        }

        if (null != toUpdate && toUpdate.size() > 0) {
            logger.info("[handleUpdate]update appliers {}, {}", toUpdate.size(), toUpdate);
            updateApplierBatch(toUpdate);
        }
    }

}
