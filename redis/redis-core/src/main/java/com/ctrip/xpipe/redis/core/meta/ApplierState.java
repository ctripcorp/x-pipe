package com.ctrip.xpipe.redis.core.meta;

/**
 * @author ayq
 * <p>
 * 2022/4/6 21:57
 */
public enum ApplierState {
    UNKNOWN {
        @Override
        public boolean isActive() {
            return false;
        }

        @Override
        public boolean isBackup() {
            return false;
        }
    },

    BACKUP {
        @Override
        public boolean isActive() {
            return false;
        }

        @Override
        public boolean isBackup() {
            return true;
        }
    },

    ACTIVE {
        @Override
        public boolean isActive() {
            return true;
        }

        @Override
        public boolean isBackup() {
            return false;
        }
    };

    public abstract boolean isActive();
    public abstract boolean isBackup();
}
