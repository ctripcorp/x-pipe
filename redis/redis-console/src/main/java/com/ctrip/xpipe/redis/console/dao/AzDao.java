package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.stereotype.Repository;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ContainerLoader;

import javax.annotation.PostConstruct;
import java.util.List;

/**
 * @author: Song_Yu
 * @date: 2021/11/11
 */
@Repository
public class AzDao extends AbstractXpipeConsoleDAO {
    private AzTblDao azTblDao;

    @PostConstruct
    private void postConstruct() {
        try {
            azTblDao = ContainerLoader.getDefaultContainer().lookup(AzTblDao.class);
        } catch (ComponentLookupException e) {
            throw new ServerException("Cannot construct azTblDao.", e);
        }
    }

    public void deleteAvailableZone(final AzTbl azTbl) {
        AzTbl proto = azTbl;
        proto.setAzName(generateDeletedName(azTbl.getAzName()));

        queryHandler.handleDelete(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return azTblDao.deleteAvailableZone(proto, AzTblEntity.UPDATESET_FULL);
            }
        }, true);
    }

    @DalTransaction
    public AzTbl addAvailablezone(AzTbl proto) {
        queryHandler.handleInsert(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return azTblDao.insert(proto);
            }
        });

        return queryHandler.handleQuery(new DalQuery<AzTbl>() {
            @Override
            public AzTbl doQuery() throws DalException {
                return azTblDao.findAvailableZoneByAz(proto.getAzName(), AzTblEntity.READSET_FULL);
            }
        });
    }

    public void updateAvailableZone(AzTbl proto) {
        queryHandler.handleUpdate(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return azTblDao.updateByPK(proto, AzTblEntity.UPDATESET_FULL);
            }
        });
    }

    public List<AzTbl> findActiveAvailableZonesByDc(long dcId) {
        return queryHandler.handleQuery(new DalQuery<List<AzTbl>>() {
            @Override
            public List<AzTbl> doQuery() throws DalException {
                return azTblDao.findActiveAvailableZoneByDc(dcId, AzTblEntity.READSET_FULL);
            }
        });
    }

    public List<AzTbl> findAvailableZonesByDc(long dcId) {
        return queryHandler.handleQuery(new DalQuery<List<AzTbl>>() {
            @Override
            public List<AzTbl> doQuery() throws DalException {
                return azTblDao.findAvailableZoneByDc(dcId, AzTblEntity.READSET_FULL);
            }
        });
    }

    public List<AzTbl> findAllAvailableZones() {
        return queryHandler.handleQuery(new DalQuery<List<AzTbl>>() {
            @Override
            public List<AzTbl> doQuery() throws DalException {
                return azTblDao.findAllAvailableZone(AzTblEntity.READSET_FULL);
            }
        });
    }

    public AzTbl findAvailableZoneByAz(String azName) {
        return queryHandler.handleQuery(new DalQuery<AzTbl>() {
            @Override
            public AzTbl doQuery() throws DalException {
                return azTblDao.findAvailableZoneByAz(azName, AzTblEntity.READSET_FULL);
            }
        });
    }

}
