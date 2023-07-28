package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTblDao;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTblEntity;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.stereotype.Repository;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ContainerLoader;

import javax.annotation.PostConstruct;
import java.util.Date;
import java.util.List;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 02, 2017
 */
@Repository
public class MigrationClusterDao extends AbstractXpipeConsoleDAO{

    private MigrationClusterTblDao migrationClusterTblDao;


    @PostConstruct
    public void postConstruct() throws ComponentLookupException {

        migrationClusterTblDao = ContainerLoader.getDefaultContainer().lookup(MigrationClusterTblDao.class);
    }

    public List<MigrationClusterTbl> find(long size, long offset) {
        return queryHandler.handleQuery(new DalQuery<List<MigrationClusterTbl>>() {
            @Override
            public List<MigrationClusterTbl> doQuery() throws DalException {
                return migrationClusterTblDao.findMigrationClusters(
                        size, offset, MigrationClusterTblEntity.READSET_EVENT_WITH_CLUSTER);
            }
        });
    }

    public List<MigrationClusterTbl> findByCluster(long clusterId, long size, long offset) {
        return queryHandler.handleQuery(new DalQuery<List<MigrationClusterTbl>>() {
            @Override
            public List<MigrationClusterTbl> doQuery() throws DalException {
                return migrationClusterTblDao.findMigrationClustersByCluster(
                        clusterId, size, offset, MigrationClusterTblEntity.READSET_EVENT_WITH_CLUSTER);
            }
        });
    }

    public List<MigrationClusterTbl> findByOperator(String operator, long size, long offset) {
        return queryHandler.handleQuery(new DalQuery<List<MigrationClusterTbl>>() {
            @Override
            public List<MigrationClusterTbl> doQuery() throws DalException {
                return migrationClusterTblDao.findMigrationClustersByOperator(
                        operator, size, offset, MigrationClusterTblEntity.READSET_EVENT_WITH_CLUSTER);
            }
        });
    }

    public List<MigrationClusterTbl> findByStatus(String status, long size, long offset) {
        return queryHandler.handleQuery(new DalQuery<List<MigrationClusterTbl>>() {
            @Override
            public List<MigrationClusterTbl> doQuery() throws DalException {
                return migrationClusterTblDao.findMigrationClustersByStatus(
                        status, size, offset, MigrationClusterTblEntity.READSET_EVENT_WITH_CLUSTER);
            }
        });
    }

    public List<MigrationClusterTbl> findAllEventsWithoutTestCluster(long size, long offset) {
        return queryHandler.handleQuery(new DalQuery<List<MigrationClusterTbl>>() {
            @Override
            public List<MigrationClusterTbl> doQuery() throws DalException {
                return migrationClusterTblDao.findMigrationEventsWithoutTestCluster(
                        size, offset, MigrationClusterTblEntity.READSET_MIG_EVENT_ID);
            }
        });
    }

    public List<MigrationClusterTbl> findByMigEventIds(List<Long> migEventIds) {
        return queryHandler.handleQuery(new DalQuery<List<MigrationClusterTbl>>() {
            @Override
            public List<MigrationClusterTbl> doQuery() throws DalException {
                return migrationClusterTblDao.findMigrationClustersByMigEvents(
                        migEventIds, MigrationClusterTblEntity.READSET_EVENT_WITH_CLUSTER);
            }
        });
    }

    public long countAllByCluster(long clusterId) {
        return queryHandler.handleQuery(new DalQuery<Long>() {
            @Override
            public Long doQuery() throws DalException {
                return migrationClusterTblDao.countAllByCluster(
                        clusterId, MigrationClusterTblEntity.READSET_COUNT).getCount();
            }
        });
    }

    public long countAllByOperator(String operator) {
        return queryHandler.handleQuery(new DalQuery<Long>() {
            @Override
            public Long doQuery() throws DalException {
                return migrationClusterTblDao.countAllByOperator(
                        operator, MigrationClusterTblEntity.READSET_COUNT).getCount();
            }
        });
    }

    public long countAllByStatus(String status) {
        return queryHandler.handleQuery(new DalQuery<Long>() {
            @Override
            public Long doQuery() throws DalException {
                return migrationClusterTblDao.countAllByStatus(
                        status, MigrationClusterTblEntity.READSET_COUNT).getCount();
            }
        });
    }

    public long countAllEventsWithoutTestClusters() {
        return queryHandler.handleQuery(new DalQuery<Long>() {
            @Override
            public Long doQuery() throws DalException {
                return migrationClusterTblDao
                        .countEventsWithoutTestCluster(MigrationClusterTblEntity.READSET_MIG_EVENT_CNT).getMigEventCnt();
            }
        });
    }

    public List<MigrationClusterTbl> findUnfinishedByClusterId(final long clusterId){

        return queryHandler.handleQuery(new DalQuery<List<MigrationClusterTbl>>() {
            @Override
            public List<MigrationClusterTbl> doQuery() throws DalException {
                return migrationClusterTblDao.findUnfinishedByClusterId(clusterId, MigrationClusterTblEntity.READSET_FULL);
            }
        });
    }

    public MigrationClusterTbl getById(long id){

        return queryHandler.handleQuery(new DalQuery<MigrationClusterTbl>() {
            @Override
            public MigrationClusterTbl doQuery() throws DalException {
                return migrationClusterTblDao.findByPK(id, MigrationClusterTblEntity.READSET_FULL);
            }
        });
    }

    public void insert(MigrationClusterTbl migrationCluster){

        queryHandler.handleInsert(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return migrationClusterTblDao.insert(migrationCluster);
            }
        });
    }

    public void updateStartTime(long id, Date date){

        MigrationClusterTbl migrationClusterTbl = new MigrationClusterTbl();
        migrationClusterTbl.setId(id);
        migrationClusterTbl.setStartTime(date);

        queryHandler.handleUpdate(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return migrationClusterTblDao.updateStartTimeById(migrationClusterTbl, MigrationClusterTblEntity.UPDATESET_FULL);
            }
        });
    }


    public void updateStatusAndEndTimeById(long id, MigrationStatus status, Date endTime){

        MigrationClusterTbl migrationClusterTbl = new MigrationClusterTbl();
        migrationClusterTbl.setId(id);
        migrationClusterTbl.setEndTime(endTime);
        migrationClusterTbl.setStatus(status.toString());
        queryHandler.handleUpdate(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return migrationClusterTblDao.updateStatusAndEndTimeById(migrationClusterTbl, MigrationClusterTblEntity.UPDATESET_FULL);
            }
        });
    }

    public void updatePublishInfoById(long id, String publishInfo){

        MigrationClusterTbl migrationClusterTbl = new MigrationClusterTbl();
        migrationClusterTbl.setId(id);
        migrationClusterTbl.setPublishInfo(publishInfo);

        queryHandler.handleUpdate(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return migrationClusterTblDao.updatePublishInfoById(migrationClusterTbl, MigrationClusterTblEntity.UPDATESET_FULL);
            }
        });
    }


    public MigrationClusterTbl findByEventIdAndClusterId(final long eventId, final long clusterId){

        return queryHandler.handleQuery(new DalQuery<MigrationClusterTbl>() {
            @Override
            public MigrationClusterTbl doQuery() throws DalException {
                return migrationClusterTblDao.findByEventIdAndClusterId(eventId, clusterId, MigrationClusterTblEntity.READSET_FULL);
            }
        });
    }

    public List<MigrationClusterTbl> findAllByClusterId(final long clusterId){

        return queryHandler.handleQuery(new DalQuery<List<MigrationClusterTbl>>() {
            @Override
            public List<MigrationClusterTbl> doQuery() throws DalException {
                return migrationClusterTblDao.findAllByClusterId(clusterId, MigrationClusterTblEntity.READSET_FULL);
            }
        });
    }

    protected void updateByPK(final MigrationClusterTbl cluster) {

        queryHandler.handleUpdate(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return migrationClusterTblDao.updateByPK(cluster, MigrationClusterTblEntity.UPDATESET_FULL);
            }
        });
    }

    public MigrationClusterTbl findfindByPK(final long id){
        return queryHandler.handleQuery(new DalQuery<MigrationClusterTbl>() {
            @Override
            public MigrationClusterTbl doQuery() throws DalException {
                return migrationClusterTblDao.findByPK(id, MigrationClusterTblEntity.READSET_FULL);
            }
        });
    }

    public List<MigrationClusterTbl> findLatestMigrationClusters(Date date) {
        return queryHandler.handleQuery(new DalQuery<List<MigrationClusterTbl>>() {
            @Override
            public List<MigrationClusterTbl> doQuery() throws DalException {
                return migrationClusterTblDao.findLatestMigrationClusters(date, MigrationClusterTblEntity.READSET_FULL);
            }
        });
    }

    public List<MigrationClusterTbl> findLatestMigrationClusterWithClusterName(Date date) {
        return queryHandler.handleQuery(new DalQuery<List<MigrationClusterTbl>>() {
            @Override
            public List<MigrationClusterTbl> doQuery() throws DalException {
                return migrationClusterTblDao.findLatestMigrationClustersWithClusterName(date, MigrationClusterTblEntity.READSET_FULL_WITH_CLUSTER_NAME);
            }
        });
    }


}
