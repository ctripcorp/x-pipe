package com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster;

public enum ClusterHealthState {
    NORMAL {
        @Override
        public int getLevel() {
            return -1;
        }
    }, LEAST_ONE_DOWN {
        @Override
        public int getLevel() {
            return 0;
        }
    }, QUARTER_DOWN {
        @Override
        public int getLevel() {
            return 1;
        }
    }, HALF_DOWN {
        @Override
        public int getLevel() {
            return 2;
        }
    }, THREE_QUARTER_DOWN {
        @Override
        public int getLevel() {
            return 3;
        }
    },FULL_DOWN {
        @Override
        public int getLevel() {
            return 4;
        }
    };

    public abstract int getLevel();


    /*Assume one cluster has 100 shards at most, the multiple operation would not cross over Integer.MAX_VALUE*/
    public static ClusterHealthState getState(int totalShardNums, int warningShardNums) {
        if(warningShardNums == 0) {
            return NORMAL;
        }
        if(warningShardNums >= totalShardNums) {
            return FULL_DOWN;
        } else if(warningShardNums * 100>= (totalShardNums * 3 * 100) / 4) {
            return THREE_QUARTER_DOWN;
        } else if(warningShardNums * 100>= (totalShardNums * 100) / 2) {
            return HALF_DOWN;
        } else if(warningShardNums * 100>= (totalShardNums * 100) / 4) {
            return QUARTER_DOWN;
        } else if(warningShardNums > 0) {
            return LEAST_ONE_DOWN;
        }
        return NORMAL;
    }

}
