package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 07, 2017
 */
public enum HEALTH_STATE {

    UNKNOWN(false, true) {
        @Override
        protected HEALTH_STATE afterPingSuccess() {
            return INSTANCEUP;
        }

        @Override
        protected HEALTH_STATE afterPingHalfFail() {
            return UNKNOWN;
        }

        @Override
        protected HEALTH_STATE afterPingFail() {
            return DOWN;
        }

        @Override
        protected HEALTH_STATE afterDelaySuccess() {
            return UNKNOWN;
        }

        @Override
        protected HEALTH_STATE afterDelayFail() {
            return UNKNOWN;
        }

        @Override
        protected HEALTH_STATE afterDelayHalfFail() {
            return UNKNOWN;
        }

        @Override
        public boolean shouldNotifyMarkup() {
            return false;
        }

        @Override
        public boolean shouldNotifyMarkDown() {
            return false;
        }
    },
    INSTANCEUP(true, true) {
        @Override
        protected HEALTH_STATE afterPingSuccess() {
            return INSTANCEUP;
        }

        @Override
        protected HEALTH_STATE afterPingHalfFail() {
            return UNHEALTHY;
        }

        @Override
        protected HEALTH_STATE afterPingFail() {
            return DOWN;
        }

        @Override
        protected HEALTH_STATE afterDelaySuccess() {
            return HEALTHY;
        }

        @Override
        protected HEALTH_STATE afterDelayFail() {
            return SICK;
        }

        @Override
        protected HEALTH_STATE afterDelayHalfFail() {
            return UNHEALTHY;
        }

        @Override
        public boolean shouldNotifyMarkup() {
            return false;
        }

        @Override
        public boolean shouldNotifyMarkDown() {
            return false;
        }
    },
    HEALTHY(false, true){
        @Override
        protected HEALTH_STATE afterPingSuccess() {
            return HEALTHY;
        }

        @Override
        protected HEALTH_STATE afterPingHalfFail() {
            return UNHEALTHY;
        }

        @Override
        protected HEALTH_STATE afterPingFail() {
            return DOWN;
        }

        @Override
        protected HEALTH_STATE afterDelaySuccess() {
            return HEALTHY;
        }

        @Override
        protected HEALTH_STATE afterDelayFail() {
            return SICK;
        }

        @Override
        protected HEALTH_STATE afterDelayHalfFail() {
            return UNHEALTHY;
        }

        @Override
        public boolean shouldNotifyMarkup() {
            return true;
        }

        @Override
        public boolean shouldNotifyMarkDown() {
            return false;
        }
    },
    UNHEALTHY(false, true) {
        @Override
        protected HEALTH_STATE afterPingSuccess() {
            return INSTANCEUP;
        }

        @Override
        protected HEALTH_STATE afterPingHalfFail() {
            return UNHEALTHY;
        }

        @Override
        protected HEALTH_STATE afterPingFail() {
            return DOWN;
        }

        @Override
        protected HEALTH_STATE afterDelaySuccess() {
            return UNHEALTHY;
        }

        @Override
        protected HEALTH_STATE afterDelayFail() {
            return SICK;
        }

        @Override
        protected HEALTH_STATE afterDelayHalfFail() {
            return UNHEALTHY;
        }

        @Override
        public boolean shouldNotifyMarkup() {
            return false;
        }

        @Override
        public boolean shouldNotifyMarkDown() {
            return false;
        }
    },
    SICK(true, false) {
        @Override
        protected HEALTH_STATE afterPingSuccess() {
            return SICK;
        }

        @Override
        protected HEALTH_STATE afterPingHalfFail() {
            return SICK;
        }

        @Override
        protected HEALTH_STATE afterPingFail() {
            return DOWN;
        }

        @Override
        protected HEALTH_STATE afterDelaySuccess() {
            return HEALTHY;
        }

        @Override
        protected HEALTH_STATE afterDelayFail() {
            return SICK;
        }

        @Override
        protected HEALTH_STATE afterDelayHalfFail() {
            return SICK;
        }

        @Override
        public boolean shouldNotifyMarkup() {
            return false;
        }

        @Override
        public boolean shouldNotifyMarkDown() {
            return true;
        }
    },
    DOWN(true, false) {
        @Override
        protected HEALTH_STATE afterPingSuccess() {
            return INSTANCEUP;
        }

        @Override
        protected HEALTH_STATE afterPingHalfFail() {
            return DOWN;
        }

        @Override
        protected HEALTH_STATE afterPingFail() {
            return DOWN;
        }

        @Override
        protected HEALTH_STATE afterDelaySuccess() {
            return DOWN;
        }

        @Override
        protected HEALTH_STATE afterDelayFail() {
            return DOWN;
        }

        @Override
        protected HEALTH_STATE afterDelayHalfFail() {
            return DOWN;
        }

        @Override
        public boolean shouldNotifyMarkup() {
            return false;
        }

        @Override
        public boolean shouldNotifyMarkDown() {
            return true;
        }
    };

    private boolean toUpNotify;
    private boolean toDownNotify;

    HEALTH_STATE(boolean toUpNotify, boolean toDownNotify){
        this.toUpNotify = toUpNotify;
        this.toDownNotify = toDownNotify;
    }

    public boolean isToUpNotify() {
        return toUpNotify;
    }

    public boolean isToDownNotify() {
        return toDownNotify;
    }

    protected abstract HEALTH_STATE afterPingSuccess();

    protected abstract HEALTH_STATE afterPingHalfFail();

    protected abstract HEALTH_STATE afterPingFail();

    protected abstract HEALTH_STATE afterDelaySuccess();

    protected abstract HEALTH_STATE afterDelayFail();

    protected abstract HEALTH_STATE afterDelayHalfFail();

    public abstract boolean shouldNotifyMarkup();

    public abstract boolean shouldNotifyMarkDown();
}
