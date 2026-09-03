package com.ctrip.xpipe.redis.keeper.prepare;

/**
 * {@code latest.store.dir} 与当前只读 Store {@code baseDir} 不一致时，由 Watcher 在 watch 线程同步回调。
 * 持有方先断已挂 slave，再返回；Watcher 随后 {@code releaseCurrentStore()}，新店由请求侧 {@code getCurrent()} 打开（D8）。
 */
@FunctionalInterface
public interface PrepareStoreChangeListener {

	PrepareStoreChangeListener NOOP = reason -> {
	};

	void onStoreChanged(String reason);
}
