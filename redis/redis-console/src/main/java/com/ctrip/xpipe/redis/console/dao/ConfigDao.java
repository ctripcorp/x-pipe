package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.exception.DalInsertException;
import com.ctrip.xpipe.redis.console.exception.DalUpdateException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import com.ctrip.xpipe.redis.console.model.ConfigTbl;
import com.ctrip.xpipe.redis.console.model.ConfigTblDao;
import com.ctrip.xpipe.redis.console.model.ConfigTblEntity;
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
 *         Jun 15, 2017
 */
@Repository
public class ConfigDao extends AbstractXpipeConsoleDAO{

    private ConfigTblDao configTblDao;

    @PostConstruct
    private void postConstruct() {
        try {
            configTblDao = ContainerLoader.getDefaultContainer().lookup(ConfigTblDao.class);
        } catch (ComponentLookupException e) {
            throw new ServerException("Cannot construct dao.", e);
        }
    }

    public String getKey(String key) throws DalException {
        return getKey(key, "");
    }

    public String getKey(String key, String subId) throws DalException {
        throw new RuntimeException("[metacache]findByKeyAndSubKey]");
        /*ConfigTbl byKey = configTblDao.findByKeyAndSubKey(key, null == subId ? "" : subId, ConfigTblEntity.READSET_VALUE);
        return byKey.getValue();*/
    }

    protected ConfigTbl findByKey(long id) throws DalException {

        return configTblDao.findByPK(id, ConfigTblEntity.READSET_FULL);
    }

    public synchronized void setKey(String key, String val) throws DalException {
        ConfigModel model = new ConfigModel().setKey(key).setVal(val);
        setConfig(model);
    }

    public synchronized void setConfig(ConfigModel config) throws DalException {

        setConfig(config, null);
    }

    public synchronized void setConfigAndUntil(ConfigModel config, Date until) throws DalException {
        setConfig(config, until);
    }

    public void setConfig(ConfigModel config, Date until) throws DalException {
        logger.info("[setConfig] {}: {}", config, until);
        boolean insert = false;

        try{
            getKey(config.getKey(), config.getSubKey());
        } catch (DalException e){
            logger.info("[setKey][not exist, create]{}", e.getMessage());
            insert = true;
        }

        ConfigTbl configTbl = buildConfigTbl(config, until);
        if(!insert) {
            queryHandler.handleUpdate(new DalQuery<Integer>() {
                @Override
                public Integer doQuery() throws DalException {
                    return configTblDao.updateValAndUntilByKeyAndSubKey(configTbl, ConfigTblEntity.UPDATESET_FULL);
                }
            });
        }else{
            configTbl.setDesc("insert automatically");
            queryHandler.handleInsert(new DalQuery<Integer>() {
                @Override
                public Integer doQuery() throws DalException {
                    return configTblDao.insert(configTbl);
                }
            });
        }
        logger.info("[setConfig] config update successfully, as {}", config.toString());
    }

    public ConfigTbl getByKey(String key) throws DalException {
        return getByKeyAndSubId(key, "");
    }

    public List<ConfigTbl> getAllByKey(String key)  throws DalException {
        return configTblDao.findByKey(key, ConfigTblEntity.READSET_FULL);
    }

    public ConfigTbl getByKeyAndSubId(String key, String subId) throws DalException {
        throw new RuntimeException("[metacache]findByKeyAndSubKey]");
        // return configTblDao.findByKeyAndSubKey(key, subId, ConfigTblEntity.READSET_FULL);
    }

    public void insertConfig(ConfigModel config, Date until, String desc) throws DalInsertException {
        ConfigTbl configTbl = buildConfigTbl(config, until);
        configTbl.setDesc(desc);

        queryHandler.handleInsert(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return configTblDao.insert(configTbl);
            }
        });
    }

    public void updateConfigIdempotent(ConfigModel config, Date until, Date dataChangeLastTime) throws DalUpdateException {
        ConfigTbl configTbl = buildConfigTbl(config, until);
        configTbl.setDataChangeLastTime(dataChangeLastTime);

        queryHandler.handleUpdate(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return configTblDao.updateByKeyAndSubKeyAndChangeTime(configTbl, ConfigTblEntity.UPDATESET_VALUE_AND_UNTIL);
            }
        });
    }

    public List<ConfigTbl> findAllByKeyAndValueAndUntilAfter(String key, String value, Date until) {
        return queryHandler.handleQuery(new DalQuery<List<ConfigTbl>>() {
            @Override
            public List<ConfigTbl> doQuery() throws DalException {
                return configTblDao.findAllByKeyAndValueAndUntilAfter(key, value, until, ConfigTblEntity.READSET_FULL);
            }
        });
    }

    private ConfigTbl buildConfigTbl(ConfigModel config, Date until) {
        ConfigTbl configTbl = new ConfigTbl();
        configTbl.setKey(config.getKey());
        configTbl.setValue(config.getVal());
        configTbl.setSubKey(config.getSubKey());
        if (null == configTbl.getSubKey()) {
            configTbl.setSubKey("");
        }
        if(config.getUpdateIP() != null) {
            configTbl.setLatestUpdateIp(config.getUpdateIP());
        }
        if(config.getUpdateUser() != null) {
            configTbl.setLatestUpdateUser(config.getUpdateUser());
        }

        if(until != null) {
            configTbl.setUntil(until);
        }

        return configTbl;
    }

}
