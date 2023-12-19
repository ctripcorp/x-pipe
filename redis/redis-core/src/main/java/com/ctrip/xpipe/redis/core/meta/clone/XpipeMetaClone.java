package com.ctrip.xpipe.redis.core.meta.clone;

import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.RedisCheckRuleMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.InnerMetaClone;

/**
 * @author lishanglin
 * date 2023/12/13
 */
public class XpipeMetaClone implements InnerMetaClone<XpipeMeta> {

    @Override
    public XpipeMeta clone(XpipeMeta o) {
        XpipeMeta clone = new XpipeMeta();
        clone.mergeAttributes(o);

        if (null != o.getDcs()) {
            for (DcMeta dcMeta: o.getDcs().values()) {
                DcMeta cloneDc = MetaCloneFacade.INSTANCE.clone(dcMeta);
                clone.addDc(cloneDc);
            }
        }

        if (null != o.getRedisCheckRules()) {
            for (RedisCheckRuleMeta redisCheckRuleMeta: o.getRedisCheckRules().values()) {
                RedisCheckRuleMeta cloneRedisCheckRule = MetaCloneFacade.INSTANCE.clone(redisCheckRuleMeta);
                clone.addRedisCheckRule(cloneRedisCheckRule);
            }
        }

        return clone;
    }
}
