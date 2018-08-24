package com.ctrip.xpipe.redis.console.healthcheck.ping;

/**
 * @author chen.zhu
 * <p>
 * Aug 24, 2018
 */
public enum PingStatus {

    FailOnce{
        @Override
        protected PingStatus afterSuccess() {
            return Healthy;
        }

        @Override
        protected PingStatus afterFail() {
            return FailTwice;
        }

        @Override
        protected boolean isHealthy() {
            return true;
        }
    }, FailTwice {
        @Override
        protected PingStatus afterSuccess() {
            return SuccessOnce;
        }

        @Override
        protected PingStatus afterFail() {
            return UnHealthy;
        }

        @Override
        protected boolean isHealthy() {
            return true;
        }
    }, UnHealthy {
        @Override
        protected PingStatus afterSuccess() {
            return SuccessOnce;
        }

        @Override
        protected PingStatus afterFail() {
            return UnHealthy;
        }

        @Override
        protected boolean isHealthy() {
            return false;
        }
    }, SuccessOnce {
        @Override
        protected PingStatus afterSuccess() {
            return Healthy;
        }

        @Override
        protected PingStatus afterFail() {
            return FailOnce;
        }

        @Override
        protected boolean isHealthy() {
            return false;
        }
    }, Healthy {
        @Override
        protected PingStatus afterSuccess() {
            return Healthy;
        }

        @Override
        protected PingStatus afterFail() {
            return FailOnce;
        }

        @Override
        protected boolean isHealthy() {
            return true;
        }
    }, Unknown {
        @Override
        protected PingStatus afterSuccess() {
            return Healthy;
        }

        @Override
        protected PingStatus afterFail() {
            return FailOnce;
        }

        @Override
        protected boolean isHealthy() {
            return true;
        }
    };

    protected abstract PingStatus afterSuccess();

    protected abstract PingStatus afterFail();

    protected abstract boolean isHealthy();
}
