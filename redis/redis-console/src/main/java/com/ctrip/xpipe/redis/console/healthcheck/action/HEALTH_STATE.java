package com.ctrip.xpipe.redis.console.healthcheck.action;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 07, 2017
 */
public enum HEALTH_STATE {

    UNKNOWN(false, false) {
        @Override
        protected HEALTH_STATE afterPingSuccess() {
            return INSTANCEOK;
        }

        @Override
        protected HEALTH_STATE afterPingFail() {
            return UNKNOWN;
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
        protected boolean markUp() {
            return false;
        }

        @Override
        protected boolean markDown() {
            return false;
        }
    },
    INSTANCEOK(true, true) {
        @Override
        protected HEALTH_STATE afterPingSuccess() {
            return INSTANCEOK;
        }

        @Override
        protected HEALTH_STATE afterPingFail() {
            return DOWN;
        }

        @Override
        protected HEALTH_STATE afterDelaySuccess() {
            return UP;
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
        protected boolean markUp() {
            return false;
        }

        @Override
        protected boolean markDown() {
            return false;
        }
    },
    UP(false, true){
        @Override
        protected HEALTH_STATE afterPingSuccess() {
            return UP;
        }

        @Override
        protected HEALTH_STATE afterPingFail() {
            return DOWN;
        }

        @Override
        protected HEALTH_STATE afterDelaySuccess() {
            return UP;
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
        protected boolean markUp() {
            return true;
        }

        @Override
        protected boolean markDown() {
            return false;
        }
    },
    UNHEALTHY(false, true) {
        @Override
        protected HEALTH_STATE afterPingSuccess() {
            return UNHEALTHY;
        }

        @Override
        protected HEALTH_STATE afterPingFail() {
            return DOWN;
        }

        @Override
        protected HEALTH_STATE afterDelaySuccess() {
            return UP;
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
        protected boolean markUp() {
            return false;
        }

        @Override
        protected boolean markDown() {
            return false;
        }
    },
    SICK(true, false) {
        @Override
        protected HEALTH_STATE afterPingSuccess() {
            return SICK;
        }

        @Override
        protected HEALTH_STATE afterPingFail() {
            return DOWN;
        }

        @Override
        protected HEALTH_STATE afterDelaySuccess() {
            return UP;
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
        protected boolean markUp() {
            return false;
        }

        @Override
        protected boolean markDown() {
            return true;
        }
    },
    DOWN(true, false) {
        @Override
        protected HEALTH_STATE afterPingSuccess() {
            return INSTANCEOK;
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
        protected boolean markUp() {
            return false;
        }

        @Override
        protected boolean markDown() {
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

    protected abstract HEALTH_STATE afterPingFail();

    protected abstract HEALTH_STATE afterDelaySuccess();

    protected abstract HEALTH_STATE afterDelayFail();

    protected abstract HEALTH_STATE afterDelayHalfFail();

    protected abstract boolean markUp();

    protected abstract boolean markDown();
}
