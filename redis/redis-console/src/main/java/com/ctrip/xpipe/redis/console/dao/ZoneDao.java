package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.ZoneTbl;
import com.ctrip.xpipe.redis.console.model.ZoneTblDao;
import com.ctrip.xpipe.redis.console.model.ZoneTblEntity;
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
 * @author taotaotu
 * May 23, 2019
 */

@Repository
public class ZoneDao extends AbstractXpipeConsoleDAO{

    private ZoneTblDao zoneTblDao;

    @Autowired
    private PlexusContainer plexusContainer;

    @PostConstruct
    private void postConstruct(){
        try {
            zoneTblDao = plexusContainer.lookup(ZoneTblDao.class);
        } catch (ComponentLookupException e) {
            throw new ServerException("can't construct dao!", e);
        }
    }

    @DalTransaction
    public void insertRecord(ZoneTbl zoneTbl){
        queryHandler.handleInsert(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return zoneTblDao.insert(zoneTbl);
            }
        });
    }

    public ZoneTbl findById(long id){
        return queryHandler.handleQuery(new DalQuery<ZoneTbl>() {
            @Override
            public ZoneTbl doQuery() throws DalException {
                return zoneTblDao.findByPK(id, ZoneTblEntity.READSET_FULL);
            }
        });
    }

    public List<ZoneTbl> findAllZones(){
        return queryHandler.handleQuery(new DalQuery<List<ZoneTbl>>() {
            @Override
            public List<ZoneTbl> doQuery() throws DalException {
                return zoneTblDao.findAllZones(ZoneTblEntity.READSET_FULL);
            }
        });
    }
}
