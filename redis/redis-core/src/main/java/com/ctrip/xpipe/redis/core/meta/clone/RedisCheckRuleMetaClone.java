package com.ctrip.xpipe.redis.core.meta.clone;

import com.ctrip.xpipe.redis.core.entity.RedisCheckRuleMeta;
import com.ctrip.xpipe.redis.core.meta.InnerMetaClone;

/**
 * @author lishanglin
 * date 2023/12/13
 */
public class RedisCheckRuleMetaClone implements InnerMetaClone<RedisCheckRuleMeta> {

    @Override
    public RedisCheckRuleMeta clone(RedisCheckRuleMeta o) {
        RedisCheckRuleMeta clone = new RedisCheckRuleMeta();
        clone.setId(o.getId());
        clone.mergeAttributes(o);
        return clone;
    }
}
