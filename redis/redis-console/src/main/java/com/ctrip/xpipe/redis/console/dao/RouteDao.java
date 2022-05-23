package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.RouteTbl;
import com.ctrip.xpipe.redis.console.model.RouteTblDao;
import com.ctrip.xpipe.redis.console.model.RouteTblEntity;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.stereotype.Repository;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ContainerLoader;

import javax.annotation.PostConstruct;
import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Jun 19, 2018
 */

@Repository
public class RouteDao extends AbstractXpipeConsoleDAO {

    private RouteTblDao dao;

    @PostConstruct
    private void postConstruct() {
        try {
            dao = ContainerLoader.getDefaultContainer().lookup(RouteTblDao.class);
        } catch (ComponentLookupException e) {
            throw new ServerException("Cannot construct dao.", e);
        }
    }

    public RouteTbl getRouteById(long routeId) {
        return queryHandler.handleQuery(new DalQuery<RouteTbl>() {
            @Override
            public RouteTbl doQuery() throws DalException {
                return dao.findByPK(routeId, RouteTblEntity.READSET_MAIN);
            }
        });
    }

    public List<RouteTbl> getAllActiveRoutesByTag(String tag) {
        return queryHandler.handleQuery(new DalQuery<List<RouteTbl>>() {
            @Override
            public List<RouteTbl> doQuery() throws DalException {
                return dao.findAllActiveByTag(tag, RouteTblEntity.READSET_MAIN);
            }
        });
    }

    public List<RouteTbl> getAllActiveRoutesByTagAndDirection(String tag, long srcDcId, long dstDcId) {
        return queryHandler.handleQuery(new DalQuery<List<RouteTbl>>() {
            @Override
            public List<RouteTbl> doQuery() throws DalException {
                return dao.findAllActiveByTagDirection(tag, srcDcId, dstDcId, RouteTblEntity.READSET_MAIN);
            }
        });
    }

    public List<RouteTbl> getAllActiveRoutesByTagAndSrcDcId(String tag, long srcDcId) {
        return queryHandler.handleQuery(new DalQuery<List<RouteTbl>>() {
            @Override
            public List<RouteTbl> doQuery() throws DalException {
                return dao.findAllActiveByTagAndSrcDc(tag, srcDcId, RouteTblEntity.READSET_MAIN);
            }
        });
    }

    public List<RouteTbl> getAllActiveRoutes() {
        return queryHandler.handleQuery(new DalQuery<List<RouteTbl>>() {
            @Override
            public List<RouteTbl> doQuery() throws DalException {
                return dao.findAllActive(RouteTblEntity.READSET_MAIN);
            }
        });
    }

    public void insert(RouteTbl routeTbl) {
        queryHandler.handleInsert(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return dao.insert(routeTbl);
            }
        });
    }

    public void delete(long id) {
        RouteTbl proto = dao.createLocal().setId(id);
        queryHandler.handleUpdate(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return dao.deleteRoute(proto, RouteTblEntity.UPDATESET_FULL);
            }
        });
    }

    public List<RouteTbl> getAllRoutes() {
        return queryHandler.handleQuery(new DalQuery<List<RouteTbl>>() {
            @Override
            public List<RouteTbl> doQuery() throws DalException {
                return dao.findAll(RouteTblEntity.READSET_MAIN);
            }
        });
    }

    public void update(RouteTbl routeTbl) {
        queryHandler.handleUpdate(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return dao.updateByPK(routeTbl, RouteTblEntity.UPDATESET_main);
            }
        });
    }

}
