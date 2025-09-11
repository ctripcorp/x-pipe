package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.AzTbl;
import com.ctrip.xpipe.redis.console.model.AzTblDao;
import com.ctrip.xpipe.redis.console.model.AzTblEntity;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import jakarta.annotation.PostConstruct;
import org.codehaus.plexus.PlexusContainer;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ContainerLoader;

import java.util.List;

/**
 * @author: Song_Yu
 * @date: 2021/11/11
 */
@Repository
public class AzDao extends AbstractXpipeConsoleDAO {
    private AzTblDao azTblDao;

    @Autowired
    private PlexusContainer container;

    @PostConstruct
    private void postConstruct() {
        try {
            azTblDao = container.lookup(AzTblDao.class);
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

    public List<AzTbl> findAllActiveAvailableZones() {
        return queryHandler.handleQuery(new DalQuery<List<AzTbl>>() {
            @Override
            public List<AzTbl> doQuery() throws DalException {
                return azTblDao.findAllActiveAvailableZone(AzTblEntity.READSET_FULL);
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

    public AzTbl findAvailableZoneById(long azId) {
        return  queryHandler.handleQuery(new DalQuery<AzTbl>() {
            @Override
            public AzTbl doQuery() throws DalException {
                return azTblDao.findByPK(azId, AzTblEntity.READSET_FULL);
            }
        });
    }

}
