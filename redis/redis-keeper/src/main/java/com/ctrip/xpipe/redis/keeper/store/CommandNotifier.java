package com.ctrip.xpipe.redis.keeper.store;

import java.io.IOException;

import com.ctrip.xpipe.redis.core.store.CommandsListener;

public interface CommandNotifier {

	void start(CommandStore store, long offset, CommandsListener listener) throws IOException;

	void close() throws IOException;

}
