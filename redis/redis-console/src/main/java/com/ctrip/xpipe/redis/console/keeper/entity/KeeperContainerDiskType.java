package com.ctrip.xpipe.redis.console.keeper.entity;

import com.ctrip.xpipe.utils.StringUtil;

public enum KeeperContainerDiskType {

    DEFAULT("DEFAULT");
    KeeperContainerDiskType(String desc){
        this.desc = desc;
    }

    private final String desc;

    public final String interval = "-";

    public String getDesc() {
        return desc;
    }

    public enum Standard {

        PEER_DATA("peerData"),
        INPUT_FLOW("inputFlow");

        Standard(String desc){
            this.desc = desc;
        }
        private final String desc;

        public String getDesc() {
            return desc;
        }
    }

}

