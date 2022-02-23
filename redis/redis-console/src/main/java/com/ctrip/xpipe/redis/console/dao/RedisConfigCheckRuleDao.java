package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.RedisConfigCheckRuleTbl;
import com.ctrip.xpipe.redis.console.model.RedisConfigCheckRuleTblDao;
import com.ctrip.xpipe.redis.console.model.RedisConfigCheckRuleTblEntity;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.stereotype.Repository;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ContainerLoader;

import javax.annotation.PostConstruct;
import java.util.List;

@Repository
public class RedisConfigCheckRuleDao extends AbstractXpipeConsoleDAO{

    private RedisConfigCheckRuleTblDao redisConfigCheckRuleTblDao;

    @PostConstruct
    private void postConstruct() {
        try {
            redisConfigCheckRuleTblDao = ContainerLoader.getDefaultContainer().lookup(RedisConfigCheckRuleTblDao.class);
        } catch (ComponentLookupException e) {
            throw new ServerException("Cannot construct azTblDao.", e);
        }
    }

    public void addRedisConfigCHeckRule(RedisConfigCheckRuleTbl proto) {
        queryHandler.handleInsert(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return redisConfigCheckRuleTblDao.insert(proto);
            }
        });

    }

    public void updateRedisConfigCHeckRule(RedisConfigCheckRuleTbl proto) {
        queryHandler.handleUpdate(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return redisConfigCheckRuleTblDao.updateByPK(proto, RedisConfigCheckRuleTblEntity.UPDATESET_FULL);
            }
        });
    }

    public void deleteRedisConfigCheckRule(final RedisConfigCheckRuleTbl redisConfigCheckRuleTbl) {
        RedisConfigCheckRuleTbl proto = redisConfigCheckRuleTbl;
        proto.setParam(generateDeletedName(redisConfigCheckRuleTbl.getParam()));

        queryHandler.handleDelete(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return redisConfigCheckRuleTblDao.deleteRedisConfigCheckRule(proto, RedisConfigCheckRuleTblEntity.UPDATESET_FULL);
            }
        }, true);
    }

    public RedisConfigCheckRuleTbl getRedisConifgCheckRuleById(Long ruleId) {
        return  queryHandler.handleQuery(new DalQuery<RedisConfigCheckRuleTbl>() {
            @Override
            public RedisConfigCheckRuleTbl doQuery() throws DalException {
                return redisConfigCheckRuleTblDao.findRedisConfigCheckRuleById(ruleId, RedisConfigCheckRuleTblEntity.READSET_FULL);
            }
        });
    }

    public RedisConfigCheckRuleTbl getRedisConifgCheckRuleByParam(String param) {
        return queryHandler.handleQuery(new DalQuery<RedisConfigCheckRuleTbl>() {
            @Override
            public RedisConfigCheckRuleTbl doQuery() throws DalException {
                return redisConfigCheckRuleTblDao.findRedisConfigCheckRulesByParam(param, RedisConfigCheckRuleTblEntity.READSET_FULL);
            }
        });
    }

    public List<RedisConfigCheckRuleTbl> getRedisConifgCheckRulesByCheckType(String checkType) {
        return  queryHandler.handleQuery(new DalQuery<List<RedisConfigCheckRuleTbl>>() {
            @Override
            public List<RedisConfigCheckRuleTbl> doQuery() throws DalException {
                return redisConfigCheckRuleTblDao.findRedisConfigCheckRulesByCheckerType(checkType, RedisConfigCheckRuleTblEntity.READSET_FULL);
            }
        });
    }

    public List<RedisConfigCheckRuleTbl> getAllRedisConfigCheckRules() {
        return queryHandler.handleQuery(new DalQuery<List<RedisConfigCheckRuleTbl>>() {
            @Override
            public List<RedisConfigCheckRuleTbl> doQuery() throws DalException {
                return redisConfigCheckRuleTblDao.findAllRedisConfigCheckRules(RedisConfigCheckRuleTblEntity.READSET_FULL);
            }
        });
    }
}
