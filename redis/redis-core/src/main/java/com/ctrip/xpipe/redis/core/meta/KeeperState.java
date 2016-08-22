package com.ctrip.xpipe.redis.core.meta;

/**
 * @author wenchao.meng
 *
 * Jul 7, 2016
 */
public enum KeeperState {
	
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
	},

	
	PRE_ACTIVE {//when restart, state load from local store
		@Override
		public boolean isActive() {
			return false;
		}

		@Override
		public boolean isBackup() {
			return false;
		}
	},

	PRE_BACKUP {//when restart, state load from local store
		@Override
		public boolean isActive() {
			return false;
		}

		@Override
		public boolean isBackup() {
			return false;
		}
	};


	public abstract boolean isActive();
	public abstract boolean isBackup();
}
