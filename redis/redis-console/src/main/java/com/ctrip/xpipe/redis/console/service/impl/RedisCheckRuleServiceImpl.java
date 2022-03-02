package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.DcClusterCreateInfo;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.RedisCheckRuleCreateInfo;
import com.ctrip.xpipe.redis.console.dao.RedisCheckRuleDao;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.model.RedisCheckRuleTbl;
import com.ctrip.xpipe.redis.console.model.RedisCheckRuleTblDao;
import com.ctrip.xpipe.redis.console.service.AbstractConsoleService;
import com.ctrip.xpipe.redis.console.service.RedisCheckRuleService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;

@Service
public class RedisCheckRuleServiceImpl extends AbstractConsoleService<RedisCheckRuleTblDao> implements RedisCheckRuleService {

    @Autowired
    private RedisCheckRuleDao redisCheckRuleDao;

    @Autowired
    private DcClusterServiceImpl dcClusterService;

    @Autowired
    private MetaCache metaCache;

    @Override
    public void addRedisCheckRule(RedisCheckRuleCreateInfo redisCheckRuleCreateInfo) {
        RedisCheckRuleTbl proto = dao.createLocal();

        if(redisCheckRuleIsExist(redisCheckRuleCreateInfo)){
            throw new IllegalArgumentException("Redis config check rule : " + redisCheckRuleCreateInfo.getCheckType() + redisCheckRuleCreateInfo.getParam() + " already exists");
        }

        if(redisCheckRuleCreateInfo.getDescription() == null)
            proto.setDescription("");
        else
            proto.setDescription(redisCheckRuleCreateInfo.getDescription());

        proto.setCheckType(redisCheckRuleCreateInfo.getCheckType()).setParam(redisCheckRuleCreateInfo.getParam());

        redisCheckRuleDao.addRedisCheckRule(proto);
    }

    @Override
    public void updateRedisCheckRule(RedisCheckRuleCreateInfo redisCheckRuleCreateInfo) {
        RedisCheckRuleTbl redisCheckRuleTbl = getRedisCheckRuleById(redisCheckRuleCreateInfo.getId());

        if(null == redisCheckRuleTbl)
            throw new IllegalArgumentException(String.format("Redis config check rule %s %s not found", redisCheckRuleCreateInfo.getCheckType(), redisCheckRuleCreateInfo.getParam()));

        if(redisCheckRuleCreateInfo.getCheckType() != null)
            redisCheckRuleTbl.setCheckType(redisCheckRuleCreateInfo.getCheckType());
        if(redisCheckRuleCreateInfo.getParam() != null)
            redisCheckRuleTbl.setParam(redisCheckRuleCreateInfo.getParam());
        if(redisCheckRuleCreateInfo.getDescription() != null)
            redisCheckRuleTbl.setDescription(redisCheckRuleCreateInfo.getDescription());

        redisCheckRuleDao.updateRedisCheckRule(redisCheckRuleTbl);
    }

    @Override
    @DalTransaction
    public void deleteRedisCheckRuleById(Long ruleId) {
        RedisCheckRuleTbl redisCheckRuleTbl = getRedisCheckRuleById(ruleId);
        if(null == redisCheckRuleTbl)
            throw new BadRequestException(String.format("Redis config check rule %d not found",ruleId));

        deleteRedisCheckRule(redisCheckRuleTbl);
    }

    @Override
    @DalTransaction
    public void deleteRedisCheckRuleByParam(String param) {
        RedisCheckRuleTbl redisCheckRuleTbl = getRedisCheckRuleByParam(param);
        if(null == redisCheckRuleTbl)
            throw new BadRequestException(String.format("Redis config check rule %s not found",param));

        deleteRedisCheckRule(redisCheckRuleTbl);
    }

    private void deleteRedisCheckRule(RedisCheckRuleTbl proto) {
        redisCheckRuleDao.deleteRedisCheckRule(proto);

        Long id = proto.getId();
        for(DcMeta dcMeta : metaCache.getXpipeMeta().getDcs().values()) {
            for(ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
                String oldRedisCheckRule = clusterMeta.getActiveRedisCheckRules();
                if(oldRedisCheckRule.contains(id.toString())) {
                    dcClusterService.updateDcCluster(new DcClusterCreateInfo().setDcName(dcMeta.getId())
                            .setClusterName(clusterMeta.getId()).setRedisCheckRule(removeOneRuleId(oldRedisCheckRule, id.toString())));
                }
            }
        }
    }

    private String removeOneRuleId(String oldRedisCheckRule, String deletedId) {
        if(oldRedisCheckRule.equals(deletedId))
            return "";

        List<String> oldRedisCheckRules = Arrays.asList(oldRedisCheckRule.split(","));
        StringBuilder stringBuilder = new StringBuilder();
        oldRedisCheckRules.forEach(redisCheckRule -> {
            if(!deletedId.equals(redisCheckRule)) {
                stringBuilder.append(redisCheckRule).append(",");
            }
        });

        stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        return stringBuilder.toString();
    }


    @Override
    public RedisCheckRuleTbl getRedisCheckRuleById(Long ruleId) {
        return redisCheckRuleDao.getRedisCheckRuleById(ruleId);
    }

    @Override
    public RedisCheckRuleTbl getRedisCheckRuleByParam(String param) {
        return redisCheckRuleDao.getRedisCheckRuleByParam(param);
    }

    @Override
    public List<RedisCheckRuleTbl> getRedisCheckRulesByCheckType(String checkType) {
        if(!"info".equals(checkType) && !"config".equals(checkType)){
            throw new IllegalArgumentException("checkType must be config or info");
        }

        return redisCheckRuleDao.getRedisCheckRulesByCheckType(checkType);
    }

    @Override
    public List<RedisCheckRuleTbl> getAllRedisCheckRules() {
        return redisCheckRuleDao.getAllRedisCheckRules();
    }

    @Override
    public List<RedisCheckRuleCreateInfo> getRedisCheckRuleInfosByCheckType(String checkType) {
        return Lists.newArrayList(Lists.transform(getRedisCheckRulesByCheckType(checkType), new Function<RedisCheckRuleTbl, RedisCheckRuleCreateInfo>() {
            @Override
            public RedisCheckRuleCreateInfo apply(RedisCheckRuleTbl redisCheckRuleTbl) {
                RedisCheckRuleCreateInfo redisCheckRuleCreateInfo =
                        new RedisCheckRuleCreateInfo().setId(redisCheckRuleTbl.getId())
                        .setCheckType(redisCheckRuleTbl.getCheckType()).setParam(redisCheckRuleTbl.getParam())
                        .setDescription(redisCheckRuleTbl.getDescription());

                return redisCheckRuleCreateInfo;
            }
        }));
    }

    @Override
    public List<RedisCheckRuleCreateInfo> getAllRedisCheckRuleInfos() {
        return Lists.newArrayList(Lists.transform(getAllRedisCheckRules(), new Function<RedisCheckRuleTbl, RedisCheckRuleCreateInfo>() {
            @Override
            public RedisCheckRuleCreateInfo apply(RedisCheckRuleTbl redisCheckRuleTbl) {
                RedisCheckRuleCreateInfo redisCheckRuleCreateInfo =
                        new RedisCheckRuleCreateInfo().setId(redisCheckRuleTbl.getId())
                                .setCheckType(redisCheckRuleTbl.getCheckType()).setParam(redisCheckRuleTbl.getParam())
                                .setDescription(redisCheckRuleTbl.getDescription());

                return  redisCheckRuleCreateInfo;
            }
        }));
    }

    boolean redisCheckRuleIsExist(RedisCheckRuleCreateInfo redisCheckRuleCreateInfo){
        RedisCheckRuleTbl exist = getRedisCheckRuleByParam(redisCheckRuleCreateInfo.getParam());
        return exist != null;
    }

}
