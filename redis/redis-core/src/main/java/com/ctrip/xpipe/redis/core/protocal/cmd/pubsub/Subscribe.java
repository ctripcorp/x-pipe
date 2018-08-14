package com.ctrip.xpipe.redis.core.protocal.cmd.pubsub;

import com.ctrip.xpipe.utils.StringUtil;

/**
 * @author chen.zhu
 * <p>
 * Apr 04, 2018
 */
public interface Subscribe {

    String PSUBSCRIBE = "psubscribe";

    String SUBSCRIBE = "subscribe";

    void addChannelListener(SubscribeListener listener);

    void removeChannelListener(SubscribeListener listener);

    enum SUBSCRIBE_STATE {
        WAITING_RESPONSE,
        SUBSCRIBING,
        UNSUBSCRIBE
    }

    enum MESSAGE_TYPE {
        MESSAGE {
            @Override
            public boolean isFromSubType(String subType) {
                return StringUtil.trimEquals(SUBSCRIBE, subType, false);
            }
        }, PMESSAGE {
            @Override
            public boolean isFromSubType(String subType) {
                return StringUtil.trimEquals(PSUBSCRIBE, subType, false);
            }
        };

        public abstract boolean isFromSubType(String subType);

        public String desc() {
            return name().toLowerCase();
        }

        public boolean matches(String type) {
            return StringUtil.trimEquals(type, desc(), true);
        }


    }
}
