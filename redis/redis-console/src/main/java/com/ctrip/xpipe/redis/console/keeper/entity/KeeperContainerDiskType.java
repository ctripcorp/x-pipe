package com.ctrip.xpipe.redis.console.keeper.entity;

public enum KeeperContainerDiskType {

    DEFAULT("DEFAULT"),
    AWS_1T("AWS_1T"),
    ALI_2T("ALI_2T"),
    SHA_6T("SHA_6T");
    KeeperContainerDiskType(String desc){
        this.desc = desc;
    }

    private final String desc;

    private final String interval = "-";

    public String getDesc() {
        return desc;
    }

    public String getPeerData() {
        return desc + interval + Standard.PEER_DATA.getDesc();
    }

    public String getInputFlow() {
        return desc + interval + Standard.INPUT_FLOW.getDesc();
    }

    private enum Standard {

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

