package com.ctrip.xpipe.redis.console.keeper;

import com.ctrip.xpipe.utils.StringUtil;

public enum KeeperContainerOverloadCause {
    PEER_DATA_OVERLOAD,
    INPUT_FLOW_OVERLOAD,
    BOTH,
    KEEPER_PAIR_PEER_DATA_OVERLOAD,
    KEEPER_PAIR_INPUT_FLOW_OVERLOAD,
    KEEPER_PAIR_BOTH,
    RESOURCE_LACK,
    PAIR_RESOURCE_LACK,;

    public static KeeperContainerOverloadCause findByValue(String value) {
        if(StringUtil.isEmpty(value)) return null;
        String upperValue = value.toUpperCase();
        for (KeeperContainerOverloadCause keeperContainerOverloadCause : KeeperContainerOverloadCause.values()) {
            if (keeperContainerOverloadCause.name().equals(upperValue)) {
                return keeperContainerOverloadCause;
            }
        }
        throw new IllegalArgumentException("no DcGroupType for value " + value);
    }
}
