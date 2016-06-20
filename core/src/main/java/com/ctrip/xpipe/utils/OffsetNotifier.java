package com.ctrip.xpipe.utils;

/**
 * @author marsqing
 *
 *         May 20, 2016 2:06:20 PM
 */
public class OffsetNotifier {
	private static final class Sync extends AbstractQueuedSynchronizer {
		private static final long serialVersionUID = 4982264981922014374L;

		Sync(long offset) {
			setState(offset);
		}

		@Override
		protected int tryAcquireShared(long startOffset) {
			return (getState() >= startOffset) ? 1 : -1;
		}

		@Override
		protected boolean tryReleaseShared(long newOffset) {
			setState(newOffset);
			return true;
		}
	}

	private final Sync sync;

	public OffsetNotifier(long offset) {
		this.sync = new Sync(offset);
	}

	public void await(long startOffset) throws InterruptedException {
		sync.acquireSharedInterruptibly(startOffset);
	}
	
	public boolean await(long startOffset, long miliSeconds) throws InterruptedException{
		return sync.tryAcquireSharedNanos(startOffset, miliSeconds * (1000*1000));
		
	}

	public void offsetIncreased(long newOffset) {
		sync.releaseShared(newOffset);
	}
}
