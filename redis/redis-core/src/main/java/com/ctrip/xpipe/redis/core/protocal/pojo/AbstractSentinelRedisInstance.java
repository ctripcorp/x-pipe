package com.ctrip.xpipe.redis.core.protocal.pojo;


import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;


public abstract class AbstractSentinelRedisInstance extends AbstractInstance implements SentinelRedisInstance {

    protected Map<String, String> info;

    private List<SentinelFlag> sentinelFlags;

    public AbstractSentinelRedisInstance(Map<String, String> infos) {
        info = infos;
        hostPort = new HostPort(info.get("ip"), Integer.parseInt(info.get("port")));
        extractFlags();
    }

    @Override
    public String name() {
        return info.get("name");
    }


    @Override
    public Set<SentinelFlag> flags() {
        return Sets.newHashSet(sentinelFlags);
    }


    protected void extractFlags() {
        String flags = info.get("flags");
        String[] allFlags = StringUtil.splitRemoveEmpty("\\s*,\\s*", flags);
        sentinelFlags = Lists.newArrayListWithExpectedSize(allFlags.length);
        for (String flag : allFlags) {
            sentinelFlags.add(SentinelFlag.valueOf(flag));
        }
    }
}
