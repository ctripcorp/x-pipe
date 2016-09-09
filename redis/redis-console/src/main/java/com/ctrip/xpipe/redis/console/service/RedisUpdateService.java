package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.constant.XpipeConsoleConstant;
import com.ctrip.xpipe.redis.console.dao.RedisDao;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.RedisTblDao;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;
import java.util.LinkedList;
import java.util.List;

/**
 * @Author shyin
 *
 * Aug 20, 2016
 */
@Service
public class RedisUpdateService extends AbstractConsoleService<RedisTblDao> {
    @Autowired
    private RedisDao redisDao;

    public void updateWithMasterDcOriginalMasterChangedKeeper(List<RedisTbl> toCreate, List<RedisTbl> toDelete, List<RedisTbl> left) {
        RedisTbl originalMaster = RedisService.findMaster(left);
        if(null == originalMaster) throw new BadRequestException("No master redis found.");

        List<RedisTbl> toCreateRedises = RedisService.findWithRole(toCreate, XpipeConsoleConstant.ROLE_REDIS);
        if(null != toCreateRedises) {
            for(RedisTbl redis : toCreateRedises) {
                redis.setRedisMaster(originalMaster.getId());
            }
        }

        List<RedisTbl> target = new LinkedList<>(toCreate);
        target.addAll(left);
        List<RedisTbl> keepers = RedisService.findWithRole(target, XpipeConsoleConstant.ROLE_KEEPER);
        for(RedisTbl keeper : keepers) {
            keeper.setRedisMaster(RedisService.MASTER_REQUIRED);
            keeper.setKeeperActive(false);
        }

        handleUpdate(toCreate, toDelete, left);
    }

    public void updateWithMasterDcOriginalMasterUnchangedKeeper(List<RedisTbl> toCreate, List<RedisTbl> toDelete, List<RedisTbl> left) {
        RedisTbl originalMaster = RedisService.findMaster(left);
        if(null == originalMaster) throw new BadRequestException("No master redis found.");

        List<RedisTbl> toCreateRedises = RedisService.findWithRole(toCreate, XpipeConsoleConstant.ROLE_REDIS);
        if(null != toCreateRedises) {
            for(RedisTbl redis : toCreateRedises) {
                redis.setRedisMaster(originalMaster.getId());
            }
        }

        handleUpdate(toCreateRedises, toDelete, left);
    }

    @DalTransaction
    public void updateWithMasterDcNewMasterChangedKeeper(List<RedisTbl> toCreate, List<RedisTbl> toDelete, List<RedisTbl> left) {
        final RedisTbl toCreateMaster = RedisService.findMaster(toCreate);
        toCreate.remove(toCreateMaster);
        RedisTbl master = queryHandler.handleQuery(new DalQuery<RedisTbl>() {
            @Override
            public RedisTbl doQuery() throws DalException {
                return redisDao.createRedisesBatch(toCreateMaster);
            }
        });
        if(null == master) throw new ServerException("Create new master redis failed.");

        List<RedisTbl> target = new LinkedList<>(toCreate);
        target.addAll(left);
        List<RedisTbl> redises = RedisService.findWithRole(target, XpipeConsoleConstant.ROLE_REDIS);
        if(null != redises) {
            for(RedisTbl redis : redises) {
                redis.setRedisMaster(master.getId());
            }
        }

        List<RedisTbl> keepers = RedisService.findWithRole(target, XpipeConsoleConstant.ROLE_KEEPER);
        if(null != keepers) {
            for(RedisTbl keeper : keepers) {
                keeper.setRedisMaster(RedisService.MASTER_REQUIRED);
                keeper.setKeeperActive(false);
            }
        }

        handleUpdate(toCreate, toDelete, left);
    }

    @DalTransaction
    public void updateWithMasterDcNewMasterUnchangedKeeper(List<RedisTbl> toCreate, List<RedisTbl> toDelete, List<RedisTbl> left) {
        final RedisTbl toCreateMaster = RedisService.findMaster(toCreate);
        toCreate.remove(toCreateMaster);
        RedisTbl master = queryHandler.handleQuery(new DalQuery<RedisTbl>() {
            @Override
            public RedisTbl doQuery() throws DalException {
                return redisDao.createRedisesBatch(toCreateMaster);
            }
        });
        if(null == master) throw new ServerException("Create new master redis failed.");

        List<RedisTbl> target = new LinkedList<>(toCreate);
        target.addAll(left);
        List<RedisTbl> redises = RedisService.findWithRole(target, XpipeConsoleConstant.ROLE_REDIS);
        if(null != redises) {
            for(RedisTbl redis : redises) {
                redis.setRedisMaster(master.getId());
            }
        }
        RedisTbl activeKeeper = RedisService.findActiveKeeper(target);
        if(null != activeKeeper) {
            activeKeeper.setRedisMaster(master.getId());
        }

        handleUpdate(toCreate, toDelete, left);
    }

    public void updateWithBackupDcChangedKeeper(List<RedisTbl> toCreate, List<RedisTbl> toDelete, List<RedisTbl> left) {
        List<RedisTbl> target = new LinkedList<>(toCreate);
        target.addAll(left);

        List<RedisTbl> redises = RedisService.findWithRole(toCreate, XpipeConsoleConstant.ROLE_REDIS);
        if(null != redises) {
        	for(RedisTbl redis : redises) {
        		redis.setRedisMaster(RedisService.MASTER_REQUIRED);
            }
        }
        
        List<RedisTbl> keepers = RedisService.findWithRole(target, XpipeConsoleConstant.ROLE_KEEPER);
        for(RedisTbl keeper : keepers) {
            keeper.setRedisMaster(RedisService.MASTER_REQUIRED);
            keeper.setKeeperActive(false);
        }

        handleUpdate(toCreate, toDelete, left);
    }

    public void updateWithBackupDcUnchangedKeeper(List<RedisTbl> toCreate, List<RedisTbl> toDelete, List<RedisTbl> left) {
        RedisTbl originalActiveKeeper = RedisService.findActiveKeeper(left);

        if(null != originalActiveKeeper) {
            if(null != toCreate) {
                for(RedisTbl redis : toCreate) {
                    redis.setRedisMaster(originalActiveKeeper.getId());
                }
            }
        }

        handleUpdate(toCreate, toDelete, left);
    }

    private void handleUpdate(final List<RedisTbl> toCreate, final List<RedisTbl> toDelete, final List<RedisTbl> left) {
    	queryHandler.handleQuery(new DalQuery<Integer>() {
			@Override
			public Integer doQuery() throws DalException {
				redisDao.handleUpdate(toCreate, toDelete, left);
				return 0;
			}
    	});
    }
}
